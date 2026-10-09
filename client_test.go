package flo

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// fakeReply builds a response frame for requestID carrying a KV Get body
// (version u64 followed by the value).
func fakeReply(requestID uint64, value string) []byte {
	data := make([]byte, 8+len(value))
	copy(data[8:], value)
	frame := make([]byte, HeaderSize+len(data))
	binary.LittleEndian.PutUint32(frame[0:4], Magic)
	binary.LittleEndian.PutUint32(frame[4:8], uint32(len(data)))
	binary.LittleEndian.PutUint64(frame[8:16], requestID)
	frame[20] = Version
	frame[21] = byte(StatusOK)
	copy(frame[HeaderSize:], data)
	binary.LittleEndian.PutUint32(frame[16:20], computeCRC32(frame[:HeaderSize], data))
	return frame
}

// readRequestID reads one request frame and returns its request id.
func readRequestID(conn net.Conn) (uint64, error) {
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(conn, header); err != nil {
		return 0, err
	}
	payload := make([]byte, binary.LittleEndian.Uint32(header[4:8]))
	if _, err := io.ReadFull(conn, payload); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(header[8:16]), nil
}

// A reply that arrives after the client gave up on its request must never be
// returned to the next call on the same client.
func TestLateReplyNotReadByNextCall(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	timedOut := make(chan struct{})
	go func() {
		for first := true; ; first = false {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn, first bool) {
				defer conn.Close()
				if first {
					id, err := readRequestID(conn)
					if err != nil {
						return
					}
					// Answer only after the client has timed out.
					<-timedOut
					conn.Write(fakeReply(id, "stale"))
				}
				for {
					id, err := readRequestID(conn)
					if err != nil {
						return
					}
					conn.Write(fakeReply(id, "fresh"))
				}
			}(conn, first)
		}
	}()

	c := NewClient(ln.Addr().String(), WithTimeout(200*time.Millisecond))
	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := c.KV.Get("k", nil); !IsConnectionError(err) {
		t.Fatalf("first Get: got %v, want a connection error", err)
	}
	close(timedOut)
	time.Sleep(50 * time.Millisecond) // let the late reply reach the socket

	res, err := c.KV.Get("k", nil)
	if err == nil && string(res.Value) == "stale" {
		t.Fatal("second Get returned the first call's late reply")
	}
	if !errors.Is(err, ErrNotConnected) {
		t.Fatalf("second Get: got %v, %v; want ErrNotConnected", res, err)
	}

	if err := c.Reconnect(); err != nil {
		t.Fatal(err)
	}
	res, err = c.KV.Get("k", nil)
	if err != nil || string(res.Value) != "fresh" {
		t.Fatalf("Get after reconnect: got %v, %v; want fresh", res, err)
	}
}

// A connection dropped right after a reconnect must still be reconnected:
// the client drops its connection on any I/O error, so the 2 s throttle in
// handleReconnect cannot assume the last reconnect left it up.
func TestStreamWorkerReconnectsDroppedConnWithinThrottle(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { conn.Close() })
			go func() {
				for {
					id, err := readRequestID(conn)
					if err != nil {
						return
					}
					conn.Write(fakeReply(id, ""))
				}
			}()
		}
	}()

	c := NewClient(ln.Addr().String(), WithTimeout(time.Second))
	sw, err := c.NewStreamWorker(StreamWorkerOptions{Stream: "s"}, func(*StreamContext) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer sw.Close()

	sw.ctx = context.Background()
	sw.lastReconnect = time.Now()
	sw.client.Close()
	if err := sw.handleReconnect(); err != nil {
		t.Fatal(err)
	}
	if !sw.client.IsConnected() {
		t.Error("handleReconnect skipped a dropped connection because a reconnect was recent")
	}
}
