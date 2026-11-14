package flo

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"
)

// A full queue drains as messages are acked: it is overloaded, so the
// client sends the enqueue again, and once its retries run out the caller
// can tell it from other overloads.
func TestQueueFullIsRetriedAsOverloaded(t *testing.T) {
	var s scripted
	addr := s.serve(t, func(n int, id uint64) []byte {
		if n < 2 {
			return statusFrame(id, StatusOverloaded, refusalBody(ReasonQueueFull, RanNo, "overloaded: queue jobs holds 10 messages"))
		}
		return statusFrame(id, StatusOK, binary.LittleEndian.AppendUint64(nil, 42))
	})
	c := connectTo(t, addr, fastRetry)
	seq, err := c.Queue.Enqueue("jobs", []byte("x"), nil)
	if err != nil || seq != 42 {
		t.Fatalf("enqueue = %d, %v; want 42 after two queue_full refusals", seq, err)
	}
	if n := len(s.seen()); n != 3 {
		t.Errorf("sent %d times, want 3", n)
	}

	var always scripted
	addr = always.serve(t, func(_ int, id uint64) []byte {
		return statusFrame(id, StatusOverloaded, refusalBody(ReasonQueueFull, RanNo, "full"))
	})
	c = connectTo(t, addr, fastRetry)
	_, err = c.Queue.Enqueue("jobs", []byte("x"), nil)
	if !errors.Is(err, ErrQueueFull) || !IsQueueFull(err) || !IsOverloaded(err) || IsGroupHeldFull(err) || IsLimit(err) {
		t.Fatalf("got %v, want ErrQueueFull, also overloaded, not group_held_full or limit", err)
	}
	if n := len(always.seen()); n != fastRetry.MaxAttempts {
		t.Errorf("sent %d times, want %d", n, fastRetry.MaxAttempts)
	}
	if IsQueueFull(checkStatus(StatusOverloaded, refusalBody(ReasonUnclassified, RanNo, "busy"), false)) {
		t.Error("a plain overloaded matched ErrQueueFull")
	}
}

// A group whose consumers hold too many records stays full until they ack
// or nack: a read returns it at once, unretried.
func TestGroupHeldFullIsReturnedNotRetried(t *testing.T) {
	var s scripted
	addr := s.serve(t, func(_ int, id uint64) []byte {
		return statusFrame(id, StatusOverloaded, refusalBody(ReasonGroupHeldFull, RanNo, "overloaded: group g of s holds 10 records"))
	})
	c := connectTo(t, addr, fastRetry)
	_, err := c.Stream.GroupRead("s", "g", "c", nil)
	if !errors.Is(err, ErrGroupHeldFull) || !IsGroupHeldFull(err) || !IsOverloaded(err) || IsQueueFull(err) {
		t.Fatalf("got %v, want ErrGroupHeldFull", err)
	}
	var se *ServerError
	if !errors.As(err, &se) || !strings.Contains(se.Message, "holds 10 records") {
		t.Errorf("got %#v, want the server's message", err)
	}
	if n := len(s.seen()); n != 1 {
		t.Errorf("sent %d times, want once", n)
	}
}

// A nack's error ends its body, up to 1024 bytes; past that the client
// refuses it unsent, as it does an abandon given a delay, dead or error.
func TestGroupNackCarriesItsErrorInTheBody(t *testing.T) {
	var value, options []byte
	sent := 0
	c := tableServer(t, func(_ OpCode, p []byte) (StatusCode, []byte) {
		sent++
		_, value, options = requestParts(t, p)
		return StatusOK, groupReportBody1
	})
	lease := StreamGroupLease{Consumer: "c", Term: 1, Index: 2}
	items := []StreamGroupItem{{ID: StreamID{TimestampMS: 5, Sequence: 6}, Deliveries: 1}}
	why := strings.Repeat("e", MaxGroupNackError)
	if _, err := c.Stream.GroupNack("s", "g", lease, items, &StreamGroupNackOptions{Error: why}); err != nil {
		t.Fatal(err)
	}
	tail := append([]byte{0x00, 0x04}, why...)
	if !bytes.HasSuffix(value, tail) || len(options) != 0 {
		t.Fatalf("value ends % x (%d bytes), options % x; want the 1024-byte error last and no options", value[len(value)-4:], len(value), options)
	}
	for name, opts := range map[string]*StreamGroupNackOptions{
		"1025-byte error":  {Error: why + "e"},
		"abandon, delayed": {Abandon: true, DelayMS: 1},
		"abandon, dead":    {Abandon: true, Dead: true},
		"abandon, error":   {Abandon: true, Error: "x"},
	} {
		if _, err := c.Stream.GroupNack("s", "g", lease, items, opts); err == nil {
			t.Errorf("%s: sent, want refused", name)
		}
	}
	if sent != 1 {
		t.Errorf("%d nacks sent, want only the first", sent)
	}
	if got := truncateError(strings.Repeat("é", MaxGroupNackError)); len(got) != MaxGroupNackError || !strings.HasSuffix(got, "é") {
		t.Errorf("truncated to %d bytes, want %d whole characters' worth", len(got), MaxGroupNackError)
	}
}

// A report to a group that isn't there is refused whole, not_found.
func TestAReportToAMissingGroupIsNotFound(t *testing.T) {
	c := tableServer(t, func(OpCode, []byte) (StatusCode, []byte) {
		return StatusNotFound, refusalBody(ReasonNotFound, RanNo, "not found: consumer group not found")
	})
	lease := StreamGroupLease{Consumer: "c", Term: 1, Index: 2}
	items := []StreamGroupItem{{ID: StreamID{TimestampMS: 5, Sequence: 6}, Deliveries: 1}}
	_, ackErr := c.Stream.GroupAck("s", "gone", lease, items, nil)
	_, nackErr := c.Stream.GroupNack("s", "gone", lease, items, &StreamGroupNackOptions{Abandon: true})
	_, touchErr := c.Stream.GroupTouch("s", "gone", lease, items, nil)
	for name, err := range map[string]error{"ack": ackErr, "nack": nackErr, "touch": touchErr} {
		if !IsNotFound(err) {
			t.Errorf("%s: got %v, want ErrNotFound", name, err)
		}
	}
}

// A stream worker whose group is held full keeps polling with a backoff
// and warns once, as at a limit.
func TestStreamWorkerBacksOffAHeldFullGroupWarningOnce(t *testing.T) {
	s, addr := newFrameServer(t, func(op OpCode, n int) reply {
		switch {
		case op == OpStreamGroupRead && n < 4:
			return reply{status: StatusOverloaded, body: refusalBody(ReasonGroupHeldFull, RanNo, "overloaded: group full")}
		case op == OpWorkerHeartbeat:
			return activeBeat()
		case op == OpStreamGroupRead:
			time.Sleep(10 * time.Millisecond)
		}
		return reply{}
	})
	sw, _, _ := streamDrainWorker(t, addr, 0)
	logger := sw.logger.(*captureLogger)
	done := startWorker(sw.Start)
	waitFor(t, "polls past the full group", func() bool { return len(s.got(OpStreamGroupRead)) > 4 })
	select {
	case err := <-done:
		t.Fatalf("Start returned %v on a full group", err)
	default:
	}
	sw.Stop()
	waitStart(t, done)
	warned := 0
	for _, line := range logger.all() {
		if strings.Contains(line, "(group_held_full)") {
			warned++
		}
	}
	if warned != 1 {
		t.Errorf("warned %d times, want once; log %q", warned, logger.all())
	}
}
