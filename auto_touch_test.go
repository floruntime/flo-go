package flo

import (
	"bytes"
	"context"
	"encoding/hex"
	"slices"
	"strings"
	"testing"
	"time"
)

func sleeper(d time.Duration) ActionHandler {
	return func(*ActionContext) (interface{}, error) {
		time.Sleep(d)
		return []byte("done"), nil
	}
}

// touchWorker is resultTestWorker with AutoTouch, whose handler for action
// "a" is h.
func touchWorker(t *testing.T, addr string, h ActionHandler) *ActionWorker {
	t.Helper()
	// Retrying off, so each touch the server sees is one the worker sent.
	w, err := NewClient(addr, WithTimeout(time.Second), WithRetry(RetryPolicy{MaxAttempts: 1})).
		NewActionWorker(ActionWorkerOptions{ActionTimeout: time.Minute, Logger: &captureLogger{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	w.handlers["a"] = h
	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.config.AutoTouch = true
	return w
}

func TestRegisterActionSendsTheOptionsItWasGiven(t *testing.T) {
	s, addr := newFrameServer(t, func(OpCode, int) reply { return reply{} })
	w, _ := resultTestWorker(t, addr, time.Second, time.Minute, succeed)
	timeout, retries := uint32(600000), uint32(5)
	opts := &ActionRegisterOptions{Namespace: "elsewhere", TimeoutMS: &timeout, MaxRetries: &retries, Owner: "payments"}
	if err := w.RegisterAction("b", succeed, opts); err != nil {
		t.Fatal(err)
	}
	// [type][timeout_ms][max_retries][owner_len][owner], as the server parses it.
	want, _ := hex.DecodeString("00" + "c0270900" + "05000000" + "0800")
	want = append(want, "payments"...)
	frames := s.got(OpActionRegister)
	if len(frames) != 1 || !bytes.Contains(frames[0], want) {
		t.Fatalf("register frames %x, want one holding %x", frames, want)
	}
	if bytes.Contains(frames[0], []byte("elsewhere")) {
		t.Error("registered in the options' namespace, not the worker's")
	}
}

// Without a timeout the register sends 0, which the server reads as its
// default lease.
func TestRegisterActionWithoutATimeoutSendsZero(t *testing.T) {
	s, addr := newFrameServer(t, func(OpCode, int) reply { return reply{} })
	w, _ := resultTestWorker(t, addr, time.Second, time.Minute, succeed)
	if err := w.RegisterAction("b", succeed, nil); err != nil {
		t.Fatal(err)
	}
	// Then the empty options field.
	want, _ := hex.DecodeString("00" + "00000000" + "03000000" + "0000" + "0000")
	frames := s.got(OpActionRegister)
	if len(frames) != 1 || !bytes.HasSuffix(frames[0], want) {
		t.Fatalf("register frames %x, want one ending %x", frames, want)
	}
}

func TestAutoTouchRenewsTheLeaseWhileTheHandlerRuns(t *testing.T) {
	s, addr := newFrameServer(t, func(OpCode, int) reply { return reply{} })
	// A 600 ms lease touches every 100 ms: a 450 ms handler gets about four.
	w := touchWorker(t, addr, sleeper(450*time.Millisecond))
	runTaskLeased(w, 600)
	touches := s.got(OpActionTouch)
	if len(touches) < 2 || len(touches) > 5 {
		t.Fatalf("%d touches, want 2 to 5", len(touches))
	}
	// Each names the delivery, keyed by its run, and asks for no length.
	for _, f := range touches {
		_, key, body := tableBody(t, f[HeaderSize:])
		if want := deliveryFields(w.config.WorkerID); key != "t1" || !bytes.Equal(body, want) {
			t.Errorf("touch key %q body %x, want t1 and %x", key, body, want)
		}
	}
	checkSends(t, s, 1, 0)
	time.Sleep(250 * time.Millisecond)
	if n := len(s.got(OpActionTouch)); n != len(touches) {
		t.Errorf("%d touches after the complete", n-len(touches))
	}
}

func TestNoAutoTouchWhenOffOrTheActionNeverTimesOut(t *testing.T) {
	s, addr := newFrameServer(t, func(OpCode, int) reply { return reply{} })
	off := touchWorker(t, addr, sleeper(450*time.Millisecond))
	off.config.AutoTouch = false
	runTaskLeased(off, 600)
	runTaskLeased(touchWorker(t, addr, sleeper(300*time.Millisecond)), 0)
	if n := len(s.got(OpActionTouch)); n != 0 {
		t.Errorf("%d touches, want none", n)
	}
}

func TestAutoTouchStopsOnlyOnARefusalThatMeansTheRunMovedOn(t *testing.T) {
	for _, c := range []struct {
		status StatusCode
		atMost int
	}{{StatusConflict, 1}, {StatusNotFound, 1}, {StatusBadRequest, 1}, {StatusOverloaded, 100}} {
		t.Run(c.status.String(), func(t *testing.T) {
			s, addr := newFrameServer(t, func(op OpCode, _ int) reply {
				if op == OpActionTouch {
					return reply{status: c.status, body: refusalBody(ReasonUnclassified, RanNo, "no")}
				}
				return reply{}
			})
			runTaskLeased(touchWorker(t, addr, sleeper(450*time.Millisecond)), 600)
			n := len(s.got(OpActionTouch))
			if c.atMost == 1 && n != 1 {
				t.Errorf("%d touches after a %v, want 1", n, c.status)
			}
			if c.atMost > 1 && n < 2 {
				t.Errorf("%d touches after a %v, want touching to go on", n, c.status)
			}
		})
	}
}

func TestATouchInFlightWhenTheHandlerEndsIsLetFinish(t *testing.T) {
	// The touch at 100 ms is answered at 250 ms; the handler ends at 150 ms.
	s, addr := newFrameServer(t, func(op OpCode, _ int) reply {
		if op == OpActionTouch {
			time.Sleep(150 * time.Millisecond)
		}
		return reply{}
	})
	runTaskLeased(touchWorker(t, addr, sleeper(150*time.Millisecond)), 600)
	if n := len(s.got(OpActionTouch)); n != 1 {
		t.Errorf("%d touches, want 1", n)
	}
	checkSends(t, s, 1, 0)
	if n := s.accepted.Load(); n != 2 {
		t.Errorf("%d connections, want 2 (poll and result), none redialed", n)
	}
}

func TestTouchIntervalIsASixthOfTheLease(t *testing.T) {
	for _, c := range []struct {
		leaseMS  uint64
		interval time.Duration
	}{
		{600, 100 * time.Millisecond},
		{300000, 50 * time.Second},
		{60, minTouchInterval},
	} {
		if got := touchInterval(c.leaseMS); got != c.interval {
			t.Errorf("touchInterval(%d) = %v, want %v", c.leaseMS, got, c.interval)
		}
	}
}

// With the complete slow to answer, a touch still going would land after
// it: none does, because touching stops before the outcome is sent.
func TestTouchingStopsBeforeTheOutcomeIsSent(t *testing.T) {
	s, addr := newFrameServer(t, func(op OpCode, _ int) reply {
		if op == OpActionComplete {
			time.Sleep(300 * time.Millisecond)
		}
		return reply{}
	})
	runTaskLeased(touchWorker(t, addr, sleeper(250*time.Millisecond)), 600)
	time.Sleep(200 * time.Millisecond)
	order := s.arrivals()
	completeAt := slices.Index(order, OpActionComplete)
	if completeAt < 0 {
		t.Fatal("no complete")
	}
	if n := len(s.got(OpActionTouch)); n == 0 {
		t.Fatal("no touch went out while the handler ran")
	}
	if after := slices.Index(order[completeAt:], OpActionTouch); after >= 0 {
		t.Errorf("a touch arrived after the complete: %v", order)
	}
}

func TestRegisterActionRefusesAnOwnerTooLongForTheWire(t *testing.T) {
	s, addr := newFrameServer(t, func(OpCode, int) reply { return reply{} })
	w, _ := resultTestWorker(t, addr, time.Second, time.Minute, succeed)
	owner := strings.Repeat("o", 64*1024)
	if err := w.RegisterAction("b", succeed, &ActionRegisterOptions{Owner: owner}); err == nil {
		t.Fatal("a 64 KiB owner was accepted")
	}
	if n := len(s.got(OpActionRegister)); n != 0 {
		t.Errorf("%d register requests sent", n)
	}
}
