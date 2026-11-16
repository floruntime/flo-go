package flo

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// Close waits for the run, so a handler still finishing when Close is
// called gets its complete sent before the connections close.
func TestActionWorkerCloseSendsTheCompleteOfARunInFlight(t *testing.T) {
	s, addr := newFrameServer(t, pollOnce(OpActionAwait, oneTask(), nil))
	running := make(chan struct{})
	w := drainTestWorker(t, addr, func(ctx *ActionContext) (interface{}, error) {
		close(running)
		<-ctx.Context().Done()
		// Finishing up after the stop, then succeeding anyway.
		time.Sleep(100 * time.Millisecond)
		return []byte("ok"), nil
	})
	done := startWorker(w.Start)
	<-running
	w.Close()
	select {
	case <-done:
	default:
		t.Error("Close returned before Start")
	}
	checkSends(t, s, 1, 0)
}

// Close waits only so long for a handler that ignores its context.
func TestActionWorkerCloseWaitIsBounded(t *testing.T) {
	old := closeRunWait
	closeRunWait = 100 * time.Millisecond
	t.Cleanup(func() { closeRunWait = old })
	_, addr := newFrameServer(t, pollOnce(OpActionAwait, oneTask(), nil))
	running := make(chan struct{})
	release := make(chan struct{})
	w := drainTestWorker(t, addr, func(*ActionContext) (interface{}, error) {
		close(running)
		<-release
		return nil, nil
	})
	done := startWorker(w.Start)
	<-running
	start := time.Now()
	w.Close()
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("Close took %v with a handler that won't return", d)
	}
	// Let the handler and the run end, so neither outlives the test.
	close(release)
	waitStart(t, done)
}

// Close waits for the run, so the hand-back of a record the stop cut short
// is sent before the ack connection closes.
func TestStreamWorkerCloseSendsTheHandBackOfARecordInFlight(t *testing.T) {
	s, addr := newFrameServer(t, pollOnce(OpStreamGroupRead, readOf(1, time.Now().UnixMilli()+30000), nil))
	running := make(chan struct{})
	sw, err := NewClient(addr, WithTimeout(time.Second)).NewStreamWorker(StreamWorkerOptions{Stream: "s", Logger: &captureLogger{}},
		func(sctx *StreamContext) error {
			close(running)
			<-sctx.Ctx().Done()
			time.Sleep(100 * time.Millisecond)
			return sctx.Ctx().Err()
		})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sw.Close() })
	done := startWorker(sw.Start)
	<-running
	sw.Close()
	select {
	case <-done:
	default:
		t.Error("Close returned before Start")
	}
	nacks := s.got(OpStreamGroupNack)
	if len(nacks) != 1 || !abandons(t, nacks[0]) {
		t.Fatalf("%d nacks; want the one abandon", len(nacks))
	}
}

// A handler that panics once the stop cancelled it was cut short, as one
// returning an error then is: its record is abandoned, not nacked.
func TestStreamWorkerAbandonsAHandlerThatPanicsAfterTheStop(t *testing.T) {
	s, addr := newFrameServer(t, pollOnce(OpStreamGroupRead, readOf(1, time.Now().UnixMilli()+30000), nil))
	running := make(chan struct{})
	sw, err := NewClient(addr, WithTimeout(time.Second)).NewStreamWorker(StreamWorkerOptions{Stream: "s", Logger: &captureLogger{}},
		func(sctx *StreamContext) error {
			close(running)
			<-sctx.Ctx().Done()
			panic("the connection the stop closed")
		})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sw.Close() })
	done := startWorker(sw.Start)
	<-running
	sw.Stop()
	waitStart(t, done)
	nacks := s.got(OpStreamGroupNack)
	if len(nacks) != 1 || !abandons(t, nacks[0]) {
		t.Fatalf("%d nacks; want the one abandon", len(nacks))
	}
	if atomic.LoadUint64(&sw.messagesFailed) != 0 {
		t.Errorf("the panic after the stop counted as a failure")
	}
}

// A panic while the worker runs is a failure: a plain nack saying so.
func TestStreamWorkerNacksAPanicWhenNotStopping(t *testing.T) {
	s, addr := newFrameServer(t, pollOnce(OpStreamGroupRead, readOf(1, time.Now().UnixMilli()+30000), nil))
	sw, err := NewClient(addr, WithTimeout(time.Second)).NewStreamWorker(StreamWorkerOptions{Stream: "s", Logger: &captureLogger{}},
		func(*StreamContext) error { panic("bug") })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sw.Close() })
	done := startWorker(sw.Start)
	waitFor(t, "the nack", func() bool { return len(s.got(OpStreamGroupNack)) > 0 })
	sw.Stop()
	waitStart(t, done)
	nacks := s.got(OpStreamGroupNack)
	if len(nacks) != 1 || abandons(t, nacks[0]) {
		t.Fatalf("%d nacks; want one plain nack", len(nacks))
	}
	if _, value, _ := requestParts(t, nacks[0][HeaderSize:]); !bytes.HasSuffix(value, []byte("handler panicked: bug")) {
		t.Errorf("nack value % x, want the panic", value)
	}
}

// A group read answered not found stops the worker, and Start returns the
// error: reading on would look like an empty stream.
func TestStreamWorkerStartFailsOnAGroupReadNotFound(t *testing.T) {
	var reads atomic.Int32
	_, addr := newFrameServer(t, func(op OpCode, n int) reply {
		if op == OpStreamGroupRead {
			reads.Add(1)
			return reply{status: StatusNotFound, body: refusalBody(ReasonNotFound, RanNo, "not found: no such stream")}
		}
		return reply{}
	})
	sw, err := NewClient(addr, WithTimeout(time.Second)).NewStreamWorker(StreamWorkerOptions{Stream: "s", Logger: &captureLogger{}},
		func(*StreamContext) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sw.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sw.Start(ctx); !IsNotFound(err) {
		t.Fatalf("Start = %v, want the not-found error", err)
	}
	if n := reads.Load(); n != 1 {
		t.Errorf("%d reads; want the one", n)
	}
}

// abandonsRun reports whether an action fail frame is the stop's hand-back:
// retry, abandoned, no delay, "worker stopped".
func abandonsRun(frame []byte) bool {
	want := append(cat([]byte{1, 1, 1, 1, 1}, u64le(0), []byte{1, 14, 0}), "worker stopped"...)
	return bytes.HasSuffix(frame, want)
}

// After Stop, an action handler's panic or non-context error abandons its
// run, as the stream worker abandons its record.
func TestActionWorkerAbandonsAHandlerThatFailsAfterTheStop(t *testing.T) {
	for _, name := range []string{"panic", "error"} {
		t.Run(name, func(t *testing.T) {
			s, addr := newFrameServer(t, pollOnce(OpActionAwait, oneTask(), nil))
			running := make(chan struct{})
			w := drainTestWorker(t, addr, func(ctx *ActionContext) (interface{}, error) {
				close(running)
				<-ctx.Context().Done()
				if name == "error" {
					return nil, errSentinel
				}
				panic("the connection the stop closed")
			})
			done := startWorker(w.Start)
			<-running
			w.Stop()
			waitStart(t, done)
			fails := s.got(OpActionFail)
			if len(fails) != 1 || !abandonsRun(fails[0]) {
				t.Fatalf("%d fails; want the one abandon", len(fails))
			}
		})
	}
}

// Before Stop, a panic keeps its rule: a fail with retry naming it.
func TestActionWorkerFailsAPanicWhenNotStopping(t *testing.T) {
	s, addr := newFrameServer(t, pollOnce(OpActionAwait, oneTask(), nil))
	w := drainTestWorker(t, addr, func(*ActionContext) (interface{}, error) { panic("bug") })
	done := startWorker(w.Start)
	waitFor(t, "the fail", func() bool { return len(s.got(OpActionFail)) > 0 })
	w.Stop()
	waitStart(t, done)
	fails := s.got(OpActionFail)
	if len(fails) != 1 || abandonsRun(fails[0]) || !bytes.HasSuffix(fails[0], []byte("action panicked: bug")) {
		t.Fatalf("%d fails; want one plain fail naming the panic", len(fails))
	}
}

var errSentinel = errors.New("gave up")
