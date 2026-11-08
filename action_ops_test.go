package flo

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestAwaitAnswerDecodesEveryTaskField(t *testing.T) {
	want := &Task{
		Delivery:           Delivery{RunID: "act-1", Action: "send", Deliveries: 3, Term: 4, Index: 90},
		Namespace:          "billing",
		CreatedAtMS:        1700,
		Attempt:            2,
		LeaseMS:            5000,
		CallerRunID:        "wf-1",
		CallerWorkflowName: "orders",
		Input:              []byte("ping"),
	}
	b := awaitAnswer(want)
	got, err := decodeAwaitAnswer(b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for n := range b {
		if _, err := decodeAwaitAnswer(b[:n]); !errors.Is(err, ErrIncompleteResponse) {
			t.Fatalf("cut at %d: %v", n, err)
		}
	}
	if _, err := decodeAwaitAnswer(append(b, 0)); !errors.Is(err, ErrIncompleteResponse) {
		t.Fatalf("trailing byte: %v", err)
	}
	if task, err := decodeAwaitAnswer(noTask); task != nil || err != nil {
		t.Fatalf("no task: %+v, %v", task, err)
	}
	if _, err := decodeAwaitAnswer(nil); !errors.Is(err, ErrIncompleteResponse) {
		t.Fatalf("empty answer: %v", err)
	}
}

func TestAwaitSendsActionsBlockAndRequestKey(t *testing.T) {
	block := uint32(5000)
	p := capturePayload(t, func(c *Client) {
		c.workerClient("w1").Await([]string{"echo", "ab"}, &WorkerAwaitOptions{BlockMS: &block, RequestKey: "rk-1"})
	})
	_, key, body := tableBody(t, p)
	want := cat([]byte{2, 4, 0}, []byte("echo"), []byte{2, 0}, []byte("ab"), []byte{1}, u64le(5000), []byte{1, 4}, []byte("rk-1"))
	if key != "w1" || !bytes.Equal(body, want) {
		t.Fatalf("key %q body % x, want w1 and % x", key, body, want)
	}
}

func TestAwaitRefusesWhatTheServerWould(t *testing.T) {
	wc := (&Client{}).workerClient("w")
	for name, opts := range map[string]struct {
		actions []string
		key     string
	}{
		"no actions":       {nil, ""},
		"65 actions":       {make([]string, 65), ""},
		"empty action":     {[]string{""}, ""},
		"long action":      {[]string{strings.Repeat("a", 257)}, ""},
		"long request key": {[]string{"a"}, strings.Repeat("k", 65)},
	} {
		if _, err := wc.Await(opts.actions, &WorkerAwaitOptions{RequestKey: opts.key}); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// A report is keyed by its run and names the delivery in its body.
func TestReportsNameTheDeliveryKeyedByRun(t *testing.T) {
	d := testTask(0).Delivery
	delay := uint64(250)
	cases := []struct {
		name string
		call func(wc *WorkerClient)
		tail []byte
	}{
		{"complete", func(wc *WorkerClient) { wc.Complete(d, []byte("pong"), &WorkerCompleteOptions{Outcome: "ok"}) },
			cat([]byte{1, 2, 0}, []byte("ok"), []byte{1, 4, 0}, []byte("pong"))},
		{"complete, success", func(wc *WorkerClient) { wc.Complete(d, nil, nil) },
			[]byte{1, 0, 0, 1, 0, 0}},
		{"fail", func(wc *WorkerClient) {
			wc.Fail(d, "boom", &WorkerFailOptions{Retry: true, DelayMS: delay})
		}, cat([]byte{1, 1, 1, 0, 1}, u64le(250), []byte{1, 4, 0}, []byte("boom"))},
		{"fail, abandoned", func(wc *WorkerClient) { wc.Fail(d, "", &WorkerFailOptions{Abandoned: true}) },
			cat([]byte{1, 0, 1, 1, 1}, u64le(0), []byte{1, 0, 0})},
		{"touch", func(wc *WorkerClient) { wc.Touch(d, nil) }, nil},
	}
	for _, tc := range cases {
		p := capturePayload(t, func(c *Client) { tc.call(c.workerClient("w1")) })
		_, key, body := tableBody(t, p)
		if want := cat(deliveryFields("w1"), tc.tail); key != "t1" || !bytes.Equal(body, want) {
			t.Errorf("%s: key %q body % x, want t1 and % x", tc.name, key, body, want)
		}
	}
}

func TestTouchReturnsTheExpiry(t *testing.T) {
	c := tableServer(t, func(op OpCode, _ []byte) (StatusCode, []byte) {
		return StatusOK, u64le(1_700_000_000_123)
	})
	at, err := c.workerClient("w1").Touch(testTask(0).Delivery, nil)
	if err != nil || at != 1_700_000_000_123 {
		t.Fatalf("touch: %d, %v", at, err)
	}
}

// A stale report is its own error: still a conflict and a ServerError, but
// told apart from a CAS or other conflict.
func TestAStaleReportIsAStaleReportError(t *testing.T) {
	msg := "stale report: the run was completed with another result"
	c := tableServer(t, func(op OpCode, _ []byte) (StatusCode, []byte) {
		return StatusConflict, refusalBody(ReasonStaleReport, RanNo, msg)
	})
	err := c.workerClient("w1").Complete(testTask(0).Delivery, []byte("other"), nil)
	var stale *StaleReportError
	if !IsStaleReport(err) || !errors.As(err, &stale) || stale.Message != msg {
		t.Fatalf("got %v, want a stale report saying %q", err, msg)
	}
	var se *ServerError
	if !IsConflict(err) || !errors.As(err, &se) || se.Reason != ReasonStaleReport {
		t.Fatalf("got %v, want it to stay a conflict ServerError", err)
	}
	if IsStaleReport(newServerError(StatusConflict, refusalBody(ReasonCASMismatch, RanNo, "cas"))) {
		t.Fatal("a CAS conflict reads as a stale report")
	}
}

// A new leader not ready yet refuses unavailable / leader_unknown, ran no:
// the SDK sends it again rather than surfacing it.
func TestLeaderUnknownIsRetried(t *testing.T) {
	notReady := reply{status: StatusUnavailable, body: refusalBody(ReasonLeaderUnknown, RanNo, "unavailable: this node's leadership isn't settled yet — retry")}
	for _, op := range []OpCode{OpActionComplete, OpActionAwait, OpKVGet} {
		s, addr := newFrameServer(t, func(got OpCode, n int) reply {
			if got == op && n == 0 {
				return notReady
			}
			if got == OpKVGet {
				return reply{body: cat(u64le(1), []byte{1, 0, 0, 0, 'v', 0})}
			}
			return reply{}
		})
		c := connectTo(t, addr, fastRetry)
		var err error
		switch op {
		case OpActionComplete:
			err = c.workerClient("w1").Complete(testTask(0).Delivery, nil, nil)
		case OpActionAwait:
			zero := uint32(0)
			_, err = c.workerClient("w1").Await([]string{"a"}, &WorkerAwaitOptions{BlockMS: &zero})
		case OpKVGet:
			_, err = c.KV.Get("k", nil)
		}
		if err != nil {
			t.Fatalf("op 0x%x: %v, want the retry to succeed", op, err)
		}
		if n := len(s.got(op)); n != 2 {
			t.Fatalf("op 0x%x sent %d times, want 2", op, n)
		}
	}
}

func TestTaskIDRoundTrips(t *testing.T) {
	d := Delivery{RunID: "act-1", Deliveries: 2, Term: 3, Index: 40, Action: "send:mail"}
	if got := d.TaskID(); got != "act-1:2:3:40:send:mail" {
		t.Fatalf("TaskID = %q", got)
	}
	back, err := ParseTaskID(d.TaskID())
	if err != nil || back != d {
		t.Fatalf("ParseTaskID = %+v, %v", back, err)
	}
	for _, cut := range []string{"", "act-1", "act-1:2:3:40", "act-1:2:3:40:", ":2:3:40:a", "act-1:x:3:40:a"} {
		if _, err := ParseTaskID(cut); err == nil {
			t.Errorf("ParseTaskID(%q) parsed", cut)
		}
	}
}

// A poll keeps its request key until one is answered: a poll whose answer
// was lost gets the delivery the first one got.
func TestAWorkerResendsAFailedPollWithItsRequestKey(t *testing.T) {
	s, addr := newFrameServer(t, func(op OpCode, n int) reply {
		if op != OpActionAwait {
			return reply{}
		}
		switch n {
		case 0:
			return reply{status: StatusUnavailable, body: refusalBody(ReasonLeaderUnknown, RanUnknown, "may have been given")}
		case 1, 2:
			return reply{}
		}
		time.Sleep(20 * time.Millisecond)
		return reply{}
	})
	w := newTestActionWorker(t, addr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Start(ctx)
	waitFor(t, "four polls", func() bool { return len(s.got(OpActionAwait)) >= 4 })
	keys := make([]string, 0, 3)
	for _, f := range s.got(OpActionAwait)[:3] {
		_, _, body := tableBody(t, f[HeaderSize:])
		keys = append(keys, string(body[len(body)-32:]))
	}
	if keys[0] != keys[1] || keys[1] == keys[2] {
		t.Fatalf("request keys %q: want the refused poll's key resent once, then a new one", keys)
	}
}

func TestActionInfoDecodesItsCounts(t *testing.T) {
	w := bodyWriter{}
	w.bytes16([]byte("payments"))
	w.u32(2)
	w.u64(5000)
	w.u32(3)
	w.u64(1700)
	w.u64(12)
	w.u8(1)
	w.u8(0)
	for i := 0; i < 9; i++ {
		if i == 4 {
			w.u8(0)
			continue
		}
		w.u8(1)
		w.u64(uint64(i + 1))
	}
	got, err := decodeActionInfoAnswer(w.buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Owner != "payments" || got.Version != 2 || got.TimeoutMS != 5000 || got.MaxAttempts != 3 || got.CreatedAtMS != 1700 ||
		got.Ready != 12 || !got.ReadyCapped || got.TooManyRuns || *got.Running != 1 || *got.TimedOut != 4 || got.LatencyCount != nil || *got.LatencyMaxMS != 9 {
		t.Fatalf("decoded %+v", got)
	}
	for n := range w.buf {
		if _, err := decodeActionInfoAnswer(w.buf[:n]); !errors.Is(err, ErrIncompleteResponse) {
			t.Fatalf("cut at %d: %v", n, err)
		}
	}
	if !isTableOp(OpActionInfo) {
		t.Fatal("action.info is a table op")
	}
}

// The read deadline covers an await's wait, which travels in its body.
func TestAnAwaitsDeadlineCoversItsBlock(t *testing.T) {
	body, err := encodeAwaitBody([]string{"a", "bb"}, 45000, "rk")
	if err != nil {
		t.Fatal(err)
	}
	if got := waitMS(OpActionAwait, body, nil); got != 45000 {
		t.Fatalf("waitMS = %d, want 45000", got)
	}
}
