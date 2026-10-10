package flo

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
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
	binary.LittleEndian.PutUint64(frame[24:32], TableHash)
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

// A connection dropped right after a reconnect must still be reconnected.
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
	if _, err := sw.client.KV.Get("k", nil); err != nil {
		t.Errorf("request after handleReconnect: %v", err)
	}
}

// IsConnected is read by worker goroutines while another goroutine's request
// drops the connection; under -race this must not report a data race.
func TestIsConnectedWhileRequestDropsConnection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		t.Cleanup(func() { conn.Close() })
		readRequestID(conn) // never answered
	}()

	c := NewClient(ln.Addr().String(), WithTimeout(50*time.Millisecond))
	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			c.IsConnected()
			time.Sleep(100 * time.Microsecond)
		}
	}()
	c.KV.Get("k", nil)
	<-done
	if c.IsConnected() {
		t.Error("client still connected after a timed-out request")
	}
}

// Stop and Close call Interrupt, often while the interrupted request is
// dropping the connection; under -race this must not report a data race.
func TestInterruptWhileRequestDropsConnection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		t.Cleanup(func() { conn.Close() })
		readRequestID(conn) // never answered
	}()

	c := NewClient(ln.Addr().String(), WithTimeout(time.Second))
	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			c.Interrupt()
			time.Sleep(100 * time.Microsecond)
		}
	}()
	if _, err := c.KV.Get("k", nil); err == nil {
		t.Error("interrupted request succeeded")
	}
	<-done
}

// serveAll accepts connections and answers each request with reply(id).
func serveAll(t *testing.T, reply func(id uint64) []byte) net.Listener {
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
			t.Cleanup(func() { conn.Close() })
			go func() {
				for {
					id, err := readRequestID(conn)
					if err != nil {
						return
					}
					if _, err := conn.Write(reply(id)); err != nil {
						return
					}
				}
			}()
		}
	}()
	return ln
}

// A malformed or truncated reply leaves the stream position unknown, so the
// connection is dropped like on any other I/O error.
func TestBadReplyDropsConnection(t *testing.T) {
	cases := map[string]func(id uint64) []byte{
		"crc mismatch": func(id uint64) []byte {
			f := fakeReply(id, "v")
			f[len(f)-1] ^= 0xff
			return f
		},
		"bad magic": func(id uint64) []byte {
			f := fakeReply(id, "v")
			f[0] ^= 0xff
			return f
		},
		"eof mid-frame": func(id uint64) []byte {
			f := fakeReply(id, "value")
			return f[:len(f)-2]
		},
		"wrong request id": func(id uint64) []byte {
			return fakeReply(id+1, "v")
		},
	}
	for name, reply := range cases {
		t.Run(name, func(t *testing.T) {
			var ln net.Listener
			if name == "eof mid-frame" {
				ln = eofServer(t, reply)
			} else {
				ln = serveAll(t, reply)
			}
			c := NewClient(ln.Addr().String(), WithTimeout(time.Second))
			if err := c.Connect(); err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if _, err := c.KV.Get("k", nil); err == nil {
				t.Fatal("first Get succeeded on a bad reply")
			}
			if c.IsConnected() {
				t.Error("still connected after a bad reply")
			}
			if _, err := c.KV.Get("k", nil); !errors.Is(err, ErrNotConnected) {
				t.Errorf("second Get: got %v, want ErrNotConnected", err)
			}
		})
	}
}

// A request the server can't parse is refused with id 0; the caller gets the
// server's error, not a request-id mismatch.
func TestRefusalWithIDZeroKeepsServerError(t *testing.T) {
	refusal := func(uint64) []byte {
		f := fakeReply(0, "")
		msg := []byte("Invalid request")
		f = append(f[:HeaderSize], msg...)
		binary.LittleEndian.PutUint32(f[4:8], uint32(len(msg)))
		f[21] = byte(StatusBadRequest)
		binary.LittleEndian.PutUint32(f[16:20], computeCRC32(f[:HeaderSize], msg))
		return f
	}
	ln := serveAll(t, refusal)
	c := NewClient(ln.Addr().String(), WithTimeout(time.Second))
	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.KV.Get("k", nil); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("got %v, want ErrBadRequest", err)
	}
	if c.IsConnected() {
		t.Error("still connected after a refusal")
	}
}

// eofServer answers one request with reply(id) and then closes the connection.
func eofServer(t *testing.T, reply func(id uint64) []byte) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if id, err := readRequestID(conn); err == nil {
			conn.Write(reply(id))
		}
	}()
	return ln
}

// Concurrent reconnects must not leak sockets or tear down each other's
// connection: once they all return, exactly one connection is open.
func TestConcurrentReconnectsLeaveOneConnection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	var open atomic.Int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			open.Add(1)
			go func() {
				defer open.Add(-1)
				defer conn.Close()
				io.Copy(io.Discard, conn)
			}()
		}
	}()

	c := NewClient(ln.Addr().String(), WithTimeout(time.Second))
	defer c.Close()
	for round := 0; round < 5; round++ {
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := c.Reconnect(); err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		deadline := time.Now().Add(2 * time.Second)
		for open.Load() != 1 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if n := open.Load(); n != 1 {
			t.Fatalf("round %d: %d server-side connections open, want 1", round, n)
		}
		if !c.IsConnected() {
			t.Fatalf("round %d: client not connected", round)
		}
	}
}

// Interrupt marks the client disconnected, and the next request fails
// instead of using the closed connection.
func TestInterruptClearsConnected(t *testing.T) {
	ln := serveAll(t, func(id uint64) []byte { return fakeReply(id, "") })
	c := NewClient(ln.Addr().String(), WithTimeout(time.Second))
	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.Interrupt()
	if c.IsConnected() {
		t.Error("still connected after Interrupt")
	}
	if _, err := c.KV.Get("k", nil); err == nil {
		t.Error("Get succeeded on an interrupted connection")
	}
	if err := c.Reconnect(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.KV.Get("k", nil); err != nil {
		t.Errorf("Get after reconnect: %v", err)
	}
}
