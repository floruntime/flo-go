package flo

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestDequeueSendsEveryFieldInTheBody(t *testing.T) {
	lease, block := uint64(60000), uint32(2000)
	p := capturePayload(t, func(c *Client) {
		c.Queue.Dequeue("jobs", 5, &DequeueOptions{Consumer: "c1", LeaseMS: &lease, BlockMS: &block, Ack: true})
	})
	_, key, body := tableBody(t, p)
	want := cat([]byte{1, 2, 0, 'c', '1', 1, 5, 0, 0, 0, 1}, u64le(60000), []byte{1}, u64le(2000), []byte{1, 1})
	if key != "jobs" || !bytes.Equal(body, want) {
		t.Fatalf("key %q body % x, want jobs and % x", key, body, want)
	}
	p = capturePayload(t, func(c *Client) { c.Queue.Dequeue("jobs", 1, nil) })
	_, _, body = tableBody(t, p)
	// No consumer, one message, the server's lease, no wait, no ack.
	if want := cat([]byte{1, 0, 0, 1, 1, 0, 0, 0, 0, 1}, u64le(0), []byte{1, 0}); !bytes.Equal(body, want) {
		t.Fatalf("defaults % x, want % x", body, want)
	}
	if !isTableOp(OpQueueDequeue) || !isTableOp(OpQueueComplete) || !isTableOp(OpQueueFail) || !isTableOp(OpQueueTouch) {
		t.Fatal("dequeue, complete, fail and touch are table ops")
	}
}

func TestDequeueRefusesWhatTheServerWould(t *testing.T) {
	q := NewClient("127.0.0.1:1").Queue
	short, long := uint64(MinLeaseMS-1), uint64(MaxLeaseMS+1)
	for name, call := range map[string]func() error{
		"count 0":     func() error { _, err := q.Dequeue("q", 0, nil); return err },
		"count 101":   func() error { _, err := q.Dequeue("q", 101, nil); return err },
		"short lease": func() error { _, err := q.Dequeue("q", 1, &DequeueOptions{LeaseMS: &short}); return err },
		"long lease":  func() error { _, err := q.Dequeue("q", 1, &DequeueOptions{LeaseMS: &long}); return err },
		"no items":    func() error { _, err := q.Complete("q", QueueLease{}, nil, nil); return err },
		"long error": func() error {
			_, err := q.Fail("q", QueueLease{}, []QueueItem{{1, 1}}, &QueueFailOptions{Error: strings.Repeat("e", 1025)})
			return err
		},
		"long delay": func() error { _, err := q.Enqueue("q", nil, &EnqueueOptions{DelayMS: MaxQueueDelayMS + 1}); return err },
	} {
		if call() == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestDequeueAnswerIsDecodedWhole(t *testing.T) {
	w := bodyWriter{}
	w.u8(2)
	for _, m := range []Message{{Seq: 7, Priority: 3, Deliveries: 1, EnqueuedAtMS: 1700, Payload: []byte("a")}, {Seq: 8, Deliveries: 2, Payload: []byte("bc")}} {
		w.u64(m.Seq)
		w.u32(m.Priority)
		w.u32(m.Deliveries)
		w.u64(uint64(m.EnqueuedAtMS))
		w.bytes32(m.Payload)
	}
	w.u64(4)
	w.u64(90)
	w.u64(31700)
	got, err := decodeDequeueAnswer(w.buf)
	if err != nil {
		t.Fatal(err)
	}
	want := &DequeueResult{
		Messages:    []Message{{Seq: 7, Priority: 3, Deliveries: 1, EnqueuedAtMS: 1700, Payload: []byte("a")}, {Seq: 8, Deliveries: 2, Payload: []byte("bc")}},
		Lease:       QueueLease{Term: 4, Index: 90},
		ExpiresAtMS: 31700,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if items := got.Items(); !reflect.DeepEqual(items, []QueueItem{{7, 1}, {8, 2}}) {
		t.Fatalf("items %+v", items)
	}
	for n := range w.buf {
		if _, err := decodeDequeueAnswer(w.buf[:n]); !errors.Is(err, ErrIncompleteResponse) {
			t.Fatalf("cut at %d: %v", n, err)
		}
	}
}

// A report names the dequeue's consumer and token, and each message by seq
// and deliveries; a fail adds its delay, dead flag and error.
func TestReportsNameTheLeaseAndEachMessage(t *testing.T) {
	lease := QueueLease{Consumer: "c1", Term: 4, Index: 90}
	items := []QueueItem{{7, 1}, {8, 2}}
	head := cat([]byte{1, 2, 0, 'c', '1'}, u64le(4), u64le(90), []byte{2}, u64le(7), []byte{1, 0, 0, 0}, u64le(8), []byte{2, 0, 0, 0})
	for name, tc := range map[string]struct {
		call func(c *Client)
		tail []byte
	}{
		"complete": {func(c *Client) { c.Queue.Complete("jobs", lease, items, nil) }, nil},
		"touch":    {func(c *Client) { c.Queue.Touch("jobs", lease, items, nil) }, nil},
		"fail": {func(c *Client) {
			c.Queue.Fail("jobs", lease, items, &QueueFailOptions{DelayMS: 500, Dead: true, Error: "x"})
		},
			cat([]byte{1}, u64le(500), []byte{1, 1, 1, 1, 0, 'x'})},
	} {
		_, key, body := tableBody(t, capturePayload(t, tc.call))
		if want := cat(head, tc.tail); key != "jobs" || !bytes.Equal(body, want) {
			t.Errorf("%s: key %q body % x, want jobs and % x", name, key, body, want)
		}
	}
}

// Per-message refusals come in an ok answer, each with its code.
func TestAReportsRefusalsArriveInAnOkAnswer(t *testing.T) {
	w := bodyWriter{}
	w.u32(1)
	w.u8(2)
	w.u64(7)
	w.u8(uint8(QueueGone))
	w.bytes8([]byte("not found: no such message in this queue"))
	w.u64(8)
	w.u8(uint8(QueueOtherHolder))
	w.bytes8([]byte("stale report: another consumer holds this delivery"))
	c := tableServer(t, func(op OpCode, _ []byte) (StatusCode, []byte) {
		if op == OpQueueTouch {
			return StatusOK, cat(u64le(5000), w.buf)
		}
		return StatusOK, w.buf
	})
	lease := QueueLease{Consumer: "c1", Term: 4, Index: 90}
	got, err := c.Queue.Complete("jobs", lease, []QueueItem{{6, 1}, {7, 1}, {8, 1}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Applied != 1 || len(got.Refused) != 2 || got.Refused[0].Why != QueueGone || got.Refused[1].Why != QueueOtherHolder ||
		got.Refused[1].Reason != "stale report: another consumer holds this delivery" {
		t.Fatalf("report %+v", got)
	}
	touched, err := c.Queue.Touch("jobs", lease, []QueueItem{{6, 1}}, nil)
	if err != nil || touched.ExpiresAtMS != 5000 || touched.Applied != 1 {
		t.Fatalf("touch %+v, %v", touched, err)
	}
	bad := append([]byte{}, w.buf...)
	bad[4+1+8] = 6 // an unknown why
	if _, err := decodeQueueReport(bad, false); !errors.Is(err, ErrIncompleteResponse) {
		t.Fatalf("unknown why: %v", err)
	}
	for n := range w.buf {
		if _, err := decodeQueueReport(w.buf[:n], false); !errors.Is(err, ErrIncompleteResponse) {
			t.Fatalf("cut at %d: %v", n, err)
		}
	}
	if QueueRefusalWhy(9).String() != "why(9)" || QueueExpired.String() != "expired" {
		t.Fatal("why names")
	}
}

// A full queue refuses an enqueue overloaded, ran no: the client tries it
// as often as its policy allows, then returns the refusal with the
// server's message.
func TestAFullQueuesRefusalIsRetriedThenReturned(t *testing.T) {
	msg := "overloaded: queue 'jobs' is full: it holds 10 messages (at most 10) and 400 held bytes (at most 1000), ready, leased and dead; consume or purge it"
	s, addr := newFrameServer(t, func(op OpCode, _ int) reply {
		return reply{status: StatusOverloaded, body: refusalBody(ReasonUnclassified, RanNo, msg)}
	})
	c := connectTo(t, addr, fastRetry)
	_, err := c.Queue.Enqueue("jobs", []byte("x"), nil)
	var se *ServerError
	if !IsOverloaded(err) || !errors.As(err, &se) || se.Ran != RanNo || se.Message != msg {
		t.Fatalf("enqueue: %v, want the full-queue refusal", err)
	}
	if n := len(s.got(OpQueueEnqueue)); n != fastRetry.MaxAttempts {
		t.Fatalf("sent %d times, want %d", n, fastRetry.MaxAttempts)
	}
}

func TestEnqueueSendsItsDelay(t *testing.T) {
	_, opts := splitValueAndOptions(capturePayload(t, func(c *Client) {
		c.Queue.Enqueue("jobs", []byte("x"), &EnqueueOptions{Priority: 2, DelayMS: 1500})
	}))
	if want := cat([]byte{byte(OptPriority), 1, 2, byte(OptDelayMS), 8}, u64le(1500)); !bytes.Equal(opts, want) {
		t.Fatalf("options % x, want % x", opts, want)
	}
}

func TestPurgeReadsTheCountRemoved(t *testing.T) {
	c := tableServer(t, func(OpCode, []byte) (StatusCode, []byte) { return StatusOK, []byte{3, 0, 0, 0} })
	if n, err := c.Queue.Purge("jobs", nil); err != nil || n != 3 {
		t.Fatalf("purge: %d, %v", n, err)
	}
}
