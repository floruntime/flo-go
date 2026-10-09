package flo

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// emptyServer answers every request OK with an empty body at once, as the
// server does to a blocking read it has no room to park. It counts the
// requests carrying op.
func emptyServer(t *testing.T, op OpCode, polls *atomic.Int64) string {
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
					if OpCode(binary.LittleEndian.Uint16(header[20:22])) == op {
						polls.Add(1)
					}
					resp := make([]byte, HeaderSize)
					binary.LittleEndian.PutUint32(resp[0:4], Magic)
					copy(resp[8:16], header[8:16])
					resp[20] = Version
					binary.LittleEndian.PutUint32(resp[16:20], computeCRC32(resp, nil))
					if _, err := conn.Write(resp); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return ln.Addr().String()
}

const emptyPollWindow = 500 * time.Millisecond

// At most a handful of polls fit in the window once each early empty answer
// is followed by a pause; without one a worker re-polls thousands of times.
const maxEmptyPolls = 20

func TestActionWorkerPausesAfterEarlyEmptyPoll(t *testing.T) {
	var polls atomic.Int64
	c := NewClient(emptyServer(t, OpActionAwait, &polls))
	w, err := c.NewActionWorker(ActionWorkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	w.MustRegisterAction("a", func(*ActionContext) (interface{}, error) { return nil, nil })
	ctx, cancel := context.WithTimeout(context.Background(), emptyPollWindow)
	defer cancel()
	w.Start(ctx)
	if n := polls.Load(); n > maxEmptyPolls {
		t.Errorf("ActionWorker polled %d times in %v against empty answers", n, emptyPollWindow)
	}
}

func TestStreamWorkerPausesAfterEarlyEmptyPoll(t *testing.T) {
	var polls atomic.Int64
	c := NewClient(emptyServer(t, OpStreamGroupRead, &polls))
	sw, err := c.NewStreamWorker(StreamWorkerOptions{Stream: "s"}, func(*StreamContext) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer sw.Close()
	ctx, cancel := context.WithTimeout(context.Background(), emptyPollWindow)
	defer cancel()
	sw.Start(ctx)
	if n := polls.Load(); n > maxEmptyPolls {
		t.Errorf("StreamWorker polled %d times in %v against empty answers", n, emptyPollWindow)
	}
}
