package flo

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"
)

func TestCheckBlockMS(t *testing.T) {
	if err := checkBlockMS(nil); err != nil {
		t.Errorf("checkBlockMS(nil) = %v, want nil", err)
	}
	for _, v := range []uint32{0, MaxBlockMS} {
		if err := checkBlockMS(&v); err != nil {
			t.Errorf("checkBlockMS(%d) = %v, want nil", v, err)
		}
	}
	over := MaxBlockMS + 1
	if err := checkBlockMS(&over); !errors.Is(err, ErrBlockTooLong) {
		t.Errorf("checkBlockMS(%d) = %v, want ErrBlockTooLong", over, err)
	}
}

func TestWorkerBlockMS(t *testing.T) {
	for in, want := range map[uint32]uint32{0: 30000, 1000: 1000, MaxBlockMS: MaxBlockMS} {
		if got, err := workerBlockMS(in); err != nil || got != want {
			t.Errorf("workerBlockMS(%d) = %d, %v; want %d, nil", in, got, err, want)
		}
	}
	if _, err := workerBlockMS(MaxBlockMS + 1); !errors.Is(err, ErrBlockTooLong) {
		t.Errorf("workerBlockMS(%d): got %v, want ErrBlockTooLong", MaxBlockMS+1, err)
	}
}

// An over-long wait is refused before any round trip, so an unconnected
// client reports ErrBlockTooLong rather than ErrNotConnected.
func TestBlockMSRefusedBeforeRoundTrip(t *testing.T) {
	c := NewClient("localhost:1")
	over := MaxBlockMS + 1
	calls := map[string]func() error{
		"KV.Get": func() error {
			_, err := c.KV.Get("k", &GetOptions{BlockMS: &over})
			return err
		},
		"Queue.Dequeue": func() error {
			_, err := c.Queue.Dequeue("q", 1, &DequeueOptions{BlockMS: &over})
			return err
		},
		"Stream.Read": func() error {
			_, err := c.Stream.Read("s", &StreamReadOptions{BlockMS: &over})
			return err
		},
		"Stream.GroupRead": func() error {
			_, err := c.Stream.GroupRead("s", "g", "c", &StreamGroupReadOptions{BlockMS: &over})
			return err
		},
		"Worker.Await": func() error {
			_, err := c.workerClient("w").Await([]string{"t"}, &WorkerAwaitOptions{BlockMS: &over})
			return err
		},
	}
	for name, call := range calls {
		if err := call(); !errors.Is(err, ErrBlockTooLong) {
			t.Errorf("%s: got %v, want ErrBlockTooLong", name, err)
		}
	}
}

func TestWorkersRefuseOverLongBlockMS(t *testing.T) {
	c := NewClient("localhost:1")
	if _, err := c.NewActionWorker(ActionWorkerOptions{BlockMS: MaxBlockMS + 1}); !errors.Is(err, ErrBlockTooLong) {
		t.Errorf("NewActionWorker: got %v, want ErrBlockTooLong", err)
	}
	handler := func(*StreamContext) error { return nil }
	if _, err := c.NewStreamWorker(StreamWorkerOptions{Stream: "s", BlockMS: MaxBlockMS + 1}, handler); !errors.Is(err, ErrBlockTooLong) {
		t.Errorf("NewStreamWorker: got %v, want ErrBlockTooLong", err)
	}
}

// A worker never polls with BlockMS 0: that would spin against the server.
func TestWorkersDefaultZeroBlockMS(t *testing.T) {
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
		}
	}()
	c := NewClient(ln.Addr().String())
	handler := func(*StreamContext) error { return nil }
	for in, want := range map[uint32]uint32{0: 30000, 1000: 1000} {
		aw, err := c.NewActionWorker(ActionWorkerOptions{BlockMS: in})
		if err != nil {
			t.Fatal(err)
		}
		if aw.config.BlockMS != want {
			t.Errorf("ActionWorker BlockMS %d: got %d, want %d", in, aw.config.BlockMS, want)
		}
		aw.Close()
		sw, err := c.NewStreamWorker(StreamWorkerOptions{Stream: "s", BlockMS: in}, handler)
		if err != nil {
			t.Fatal(err)
		}
		if sw.config.BlockMS != want {
			t.Errorf("StreamWorker BlockMS %d: got %d, want %d", in, sw.config.BlockMS, want)
		}
		sw.Close()
	}
}

// Await sends the server's 30000 default when BlockMS is unset, so the client
// deadline (timeout + BlockMS) covers the wait instead of failing at 5 s.
func TestAwaitSendsDefaultBlockMS(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	got := make(chan []byte, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 4096)
		n, _ := conn.Read(buf)
		got <- buf[:n]
	}()
	c := NewClient(ln.Addr().String(), WithTimeout(time.Second))
	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.workerClient("w").Await([]string{"t"}, nil) // the fake server never answers
	req := <-got
	i := bytes.Index(req, []byte{byte(OptBlockMS), 4})
	if i < 0 || extractBlockMS(req[i:]) != 30000 {
		t.Errorf("Await(nil) request carries no block_ms 30000: % x", req)
	}
}
