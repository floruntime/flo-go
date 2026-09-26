package flo

import (
	"errors"
	"testing"
)

func TestCheckBlockMS(t *testing.T) {
	zero, max, over := uint32(0), MaxBlockMS, MaxBlockMS+1
	for _, v := range []*uint32{nil, &zero, &max} {
		if err := checkBlockMS(v); err != nil {
			t.Errorf("checkBlockMS(%v) = %v, want nil", v, err)
		}
	}
	if err := checkBlockMS(&over); !errors.Is(err, ErrBlockTooLong) {
		t.Errorf("checkBlockMS(%d) = %v, want ErrBlockTooLong", over, err)
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
