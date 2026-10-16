package flo

import (
	"bytes"
	"context"
	"encoding/binary"
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

// touchWorker is resultTestWorker with AutoTouch and action "a" registered
// with a timeout of timeoutMS.
func touchWorker(t *testing.T, addr string, timeoutMS uint32, h ActionHandler) *ActionWorker {
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
	w.timeouts["a"] = timeoutMS
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
	if w.timeouts["b"] != 600000 {
		t.Errorf("remembered timeout %d", w.timeouts["b"])
	}
}

func TestAutoTouchRenewsTheLeaseWhileTheHandlerRuns(t *testing.T) {
	s, addr := newFrameServer(t, func(OpCode, int) reply { return reply{} })
	// A 300 ms timeout touches every 100 ms: a 450 ms handler gets about four.
	runTask(touchWorker(t, addr, 300, sleeper(450*time.Millisecond)))
	touches := s.got(OpActionTouch)
	if len(touches) < 2 || len(touches) > 5 {
		t.Fatalf("%d touches, want 2 to 5", len(touches))
	}
	// Each asks for this worker's timeout: "t1" then u32 300.
	want := binary.LittleEndian.AppendUint32([]byte("t1"), 300)
	for _, f := range touches {
		if !bytes.Contains(f, want) {
			t.Errorf("touch %x doesn't ask for 300 ms", f)
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
	off := touchWorker(t, addr, 300, sleeper(450*time.Millisecond))
	off.config.AutoTouch = false
	runTask(off)
	runTask(touchWorker(t, addr, 0, sleeper(300*time.Millisecond)))
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
			runTask(touchWorker(t, addr, 300, sleeper(450*time.Millisecond)))
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
	runTask(touchWorker(t, addr, 300, sleeper(150*time.Millisecond)))
	if n := len(s.got(OpActionTouch)); n != 1 {
		t.Errorf("%d touches, want 1", n)
	}
	checkSends(t, s, 1, 0)
	if n := s.accepted.Load(); n != 2 {
		t.Errorf("%d connections, want 2 (poll and result), none redialed", n)
	}
}

func TestTouchPaceIsAThirdOfTheLeaseItAsksFor(t *testing.T) {
	for _, c := range []struct {
		timeoutMS, extendMS uint32
		interval            time.Duration
	}{
		{300, 300, 100 * time.Millisecond},
		{30000, 30000, 10 * time.Second},
		{60, 60, minTouchInterval},
		// Past an hour the lease is capped, and so is the pace.
		{4 * 3_600_000, 3_600_000, 20 * time.Minute},
	} {
		interval, extend := touchPace(c.timeoutMS)
		if interval != c.interval || extend != c.extendMS {
			t.Errorf("touchPace(%d) = %v, %d; want %v, %d", c.timeoutMS, interval, extend, c.interval, c.extendMS)
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
	runTask(touchWorker(t, addr, 300, sleeper(250*time.Millisecond)))
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
