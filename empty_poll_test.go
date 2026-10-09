package flo

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeServer answers every request OK with the body answer returns for its
// op code; answer may sleep to stand in for a parked read.
func fakeServer(t *testing.T, answer func(op OpCode) []byte) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				for {
					header := make([]byte, HeaderSize)
					if _, err := io.ReadFull(conn, header); err != nil {
						return
					}
					payload := make([]byte, binary.LittleEndian.Uint32(header[4:8]))
					if _, err := io.ReadFull(conn, payload); err != nil {
						return
					}
					data := answer(OpCode(binary.LittleEndian.Uint16(header[20:22])))
					resp := make([]byte, HeaderSize+len(data))
					binary.LittleEndian.PutUint32(resp[0:4], Magic)
					binary.LittleEndian.PutUint32(resp[4:8], uint32(len(data)))
					copy(resp[8:16], header[8:16])
					resp[20] = Version
					copy(resp[HeaderSize:], data)
					binary.LittleEndian.PutUint32(resp[16:20], computeCRC32(resp[:HeaderSize], data))
					if _, err := conn.Write(resp); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return ln.Addr().String()
}

// oneRecord is a GroupRead body carrying a single record.
var oneRecord = []byte{
	1, 0, 0, 0, // count
	1, 0, 0, 0, 0, 0, 0, 0, // sequence
	1, 0, 0, 0, 0, 0, 0, 0, // timestamp_ms
	0,          // tier
	0, 0, 0, 0, // partition
	0,               // key_present
	1, 0, 0, 0, 'x', // payload
	0, 0, 0, 0, // header count
}

// countingEmpty answers every request empty at once, as the server does to a
// blocking read it has no room to park, and counts the op polls.
func countingEmpty(op OpCode, polls *atomic.Int64) func(OpCode) []byte {
	return func(got OpCode) []byte {
		if got == op {
			polls.Add(1)
		}
		return nil
	}
}

func newTestActionWorker(t *testing.T, addr string) *ActionWorker {
	t.Helper()
	w, err := NewClient(addr).NewActionWorker(ActionWorkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	w.MustRegisterAction("a", func(*ActionContext) (interface{}, error) { return nil, nil })
	return w
}

func newTestStreamWorker(t *testing.T, addr string, blockMS uint32, handler StreamRecordHandler) *StreamWorker {
	t.Helper()
	sw, err := NewClient(addr).NewStreamWorker(StreamWorkerOptions{
		Stream:                      "s",
		BlockMS:                     blockMS,
		RedeliverPendingOnReconnect: new(bool),
	}, handler)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sw.Close() })
	return sw
}

const emptyPollWindow = 500 * time.Millisecond

// Back-to-back early empties pause 50, 100, 200 ms...: a handful of polls fit
// in the window, where a worker that never pauses makes thousands.
const maxEmptyPolls = 20

func TestActionWorkerBacksOffWhenServerFull(t *testing.T) {
	var polls atomic.Int64
	w := newTestActionWorker(t, fakeServer(t, countingEmpty(OpActionAwait, &polls)))
	ctx, cancel := context.WithTimeout(context.Background(), emptyPollWindow)
	defer cancel()
	w.Start(ctx)
	if n := polls.Load(); n > maxEmptyPolls {
		t.Errorf("ActionWorker polled %d times in %v against empty answers", n, emptyPollWindow)
	}
}

func TestStreamWorkerBacksOffWhenServerFull(t *testing.T) {
	var polls atomic.Int64
	sw := newTestStreamWorker(t, fakeServer(t, countingEmpty(OpStreamGroupRead, &polls)), 0,
		func(*StreamContext) error { return nil })
	ctx, cancel := context.WithTimeout(context.Background(), emptyPollWindow)
	defer cancel()
	sw.Start(ctx)
	if n := polls.Load(); n > maxEmptyPolls {
		t.Errorf("StreamWorker polled %d times in %v against empty answers", n, emptyPollWindow)
	}
}

// A parked group read is woken empty by an append; the worker reads again
// promptly and gets the record.
func TestStreamWorkerRereadsPromptlyAfterWake(t *testing.T) {
	const block = 3 * time.Second
	const wakes = 2
	var reads atomic.Int32
	var lastWake atomic.Int64
	addr := fakeServer(t, func(op OpCode) []byte {
		if op != OpStreamGroupRead {
			return nil
		}
		switch n := reads.Add(1); {
		case n <= wakes:
			time.Sleep(10 * time.Millisecond) // parked, then woken by an append
			lastWake.Store(time.Now().UnixNano())
			return nil
		case n == wakes+1:
			return oneRecord
		default:
			time.Sleep(block)
			return nil
		}
	})
	handled := make(chan time.Time, 1)
	sw := newTestStreamWorker(t, addr, uint32(block/time.Millisecond), func(*StreamContext) error {
		handled <- time.Now()
		return nil
	})
	go sw.Start(context.Background())
	select {
	case at := <-handled:
		if d := at.Sub(time.Unix(0, lastWake.Load())); d > 90*time.Millisecond {
			t.Fatalf("record handled %v after the last wake", d)
		}
	case <-time.After(block):
		t.Fatal("record never handled")
	}
}

// A consumer that loses the group re-read to another one is woken empty on
// every append, each long after a round trip. It never pauses.
func TestStreamWorkerDoesNotPauseAfterLateWakes(t *testing.T) {
	const block = 3 * time.Second
	const wakes = 4
	var mu sync.Mutex
	var reads int
	var answered time.Time
	var maxGap time.Duration
	done := make(chan struct{})
	addr := fakeServer(t, func(op OpCode) []byte {
		if op != OpStreamGroupRead {
			return nil
		}
		mu.Lock()
		reads++
		n := reads
		if n > 1 {
			maxGap = max(maxGap, time.Since(answered))
		}
		mu.Unlock()
		if n > wakes {
			close(done)
			time.Sleep(block)
			return nil
		}
		time.Sleep(300 * time.Millisecond) // woken by an append another consumer won
		mu.Lock()
		answered = time.Now()
		mu.Unlock()
		return nil
	})
	sw := newTestStreamWorker(t, addr, uint32(block/time.Millisecond), func(*StreamContext) error { return nil })
	go sw.Start(context.Background())
	select {
	case <-done:
	case <-time.After(2 * block):
		t.Fatal("worker stopped polling")
	}
	mu.Lock()
	defer mu.Unlock()
	if maxGap > 30*time.Millisecond {
		t.Errorf("worker waited %v before re-polling after a late wake", maxGap)
	}
}

// Stopping a worker ends a backoff pause at once.
func TestWorkerStopEndsBackoffPause(t *testing.T) {
	var polls atomic.Int64
	addr := fakeServer(t, countingEmpty(OpActionAwait, &polls))
	w := newTestActionWorker(t, addr)
	stopped := make(chan time.Time)
	go func() {
		w.Start(context.Background())
		stopped <- time.Now()
	}()
	// Early empties pause 50, 100, 200 ms and then 400 ms, from about 350 ms.
	time.Sleep(450 * time.Millisecond)
	stopAt := time.Now()
	w.Stop()
	select {
	case at := <-stopped:
		if d := at.Sub(stopAt); d > 100*time.Millisecond {
			t.Errorf("Start returned %v after Stop", d)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after Stop")
	}
}
