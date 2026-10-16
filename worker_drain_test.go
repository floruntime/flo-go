package flo

import (
	"context"
	"encoding/binary"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

// fastHeartbeats shortens the heartbeat, drain notice and result reconnect
// waits for a test.
func fastHeartbeats(t *testing.T) {
	hb, notice, result := heartbeatInterval, drainNoticeInterval, resultReconnectWait
	heartbeatInterval, drainNoticeInterval, resultReconnectWait = 30*time.Millisecond, 50*time.Millisecond, 200*time.Millisecond
	t.Cleanup(func() { heartbeatInterval, drainNoticeInterval, resultReconnectWait = hb, notice, result })
}

// oneTask is an Await answer assigning task "t1" of action "a".
func oneTask() []byte {
	b := binary.LittleEndian.AppendUint16(nil, 2)
	b = append(b, "t1"...)
	b = binary.LittleEndian.AppendUint16(b, 1)
	b = append(b, 'a')
	b = binary.LittleEndian.AppendUint64(b, 0) // created_at
	b = binary.LittleEndian.AppendUint32(b, 1) // attempt
	return append(b, 0)                        // no caller, empty payload
}

// pollOnce answers the first poll with work and later ones empty after a
// short wait, standing in for a parked read.
func pollOnce(pollOp OpCode, work []byte, decide func(op OpCode, n int) reply) func(OpCode, int) reply {
	return func(op OpCode, n int) reply {
		if op == pollOp {
			if n == 0 {
				return reply{body: work}
			}
			time.Sleep(20 * time.Millisecond)
			return reply{}
		}
		if decide != nil {
			return decide(op, n)
		}
		return reply{}
	}
}

func drainingHeartbeat(op OpCode, n int) reply {
	if op == OpWorkerHeartbeat {
		return reply{body: []byte{byte(WorkerStatusDraining)}}
	}
	return reply{}
}

// startWorker runs start in the background and returns its result channel.
func startWorker(start func(context.Context) error) <-chan error {
	done := make(chan error, 1)
	go func() { done <- start(context.Background()) }()
	return done
}

func waitStart(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return")
		return nil
	}
}

// slowHandler returns an action handler that signals running, then works for
// d unless its context ends first.
func slowHandler(running chan<- struct{}, d time.Duration) ActionHandler {
	return func(ctx *ActionContext) (interface{}, error) {
		close(running)
		select {
		case <-time.After(d):
			return []byte("ok"), nil
		case <-ctx.Context().Done():
			return nil, ctx.Context().Err()
		}
	}
}

func drainTestWorker(t *testing.T, addr string, h ActionHandler) *ActionWorker {
	t.Helper()
	w, err := NewClient(addr, WithTimeout(time.Second)).NewActionWorker(ActionWorkerOptions{Logger: &captureLogger{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	w.MustRegisterAction("a", h, nil)
	return w
}

func TestADrainingHeartbeatLetsTheRunningActionComplete(t *testing.T) {
	fastHeartbeats(t)
	s, addr := newFrameServer(t, pollOnce(OpActionAwait, oneTask(), drainingHeartbeat))
	running := make(chan struct{})
	w := drainTestWorker(t, addr, slowHandler(running, 400*time.Millisecond))
	done := startWorker(w.Start)
	<-running
	if err := waitStart(t, done); err != nil {
		t.Errorf("Start = %v after a drain, want nil", err)
	}
	checkSends(t, s, 1, 0)
	if n := len(s.got(OpWorkerDrain)); n != 0 {
		t.Errorf("sent %d drain notices for a drain the server announced, want 0", n)
	}
	// Heartbeats keep the worker alive while its action finishes.
	if n := len(s.got(OpWorkerHeartbeat)); n < 4 {
		t.Errorf("saw %d heartbeats over a 400ms drain, want them to go on", n)
	}
}

func TestDrainLetsTheRunningActionCompleteAndTellsTheServer(t *testing.T) {
	fastHeartbeats(t)
	s, addr := newFrameServer(t, pollOnce(OpActionAwait, oneTask(), nil))
	running := make(chan struct{})
	w := drainTestWorker(t, addr, slowHandler(running, 200*time.Millisecond))
	done := startWorker(w.Start)
	<-running
	w.Drain()
	if err := waitStart(t, done); err != nil {
		t.Errorf("Start = %v after a drain, want nil", err)
	}
	checkSends(t, s, 1, 0)
	if n := len(s.got(OpWorkerDrain)); n != 1 {
		t.Errorf("sent %d drain notices, want 1", n)
	}
}

func TestADrainNoticeIsRetriedWhileTheConnectionIsDown(t *testing.T) {
	fastHeartbeats(t)
	s, addr := newFrameServer(t, pollOnce(OpActionAwait, oneTask(), func(op OpCode, n int) reply {
		return reply{hangUp: op == OpWorkerDrain && n < 2}
	}))
	running := make(chan struct{})
	w := drainTestWorker(t, addr, slowHandler(running, 400*time.Millisecond))
	done := startWorker(w.Start)
	<-running
	w.Drain()
	if err := waitStart(t, done); err != nil {
		t.Errorf("Start = %v after a drain, want nil", err)
	}
	checkSends(t, s, 1, 0)
	if n := len(s.got(OpWorkerDrain)); n != 3 {
		t.Errorf("sent %d drain notices, want 3 (two lost, one landed)", n)
	}
}

func TestAnIdleWorkerTriesTheDrainNoticeOnce(t *testing.T) {
	fastHeartbeats(t)
	s, addr := newFrameServer(t, func(op OpCode, n int) reply {
		if op == OpActionAwait {
			time.Sleep(20 * time.Millisecond)
		}
		return reply{hangUp: op == OpWorkerDrain}
	})
	w := drainTestWorker(t, addr, succeed)
	done := startWorker(w.Start)
	for len(s.got(OpActionAwait)) == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	w.Drain()
	if err := waitStart(t, done); err != nil {
		t.Errorf("Start = %v after a drain, want nil", err)
	}
	// One try, and one more after the reconnect it makes.
	if n := len(s.got(OpWorkerDrain)); n > 2 {
		t.Errorf("sent %d drain notices from an idle worker, want at most 2", n)
	}
}

func TestStopDuringADrainStillCancelsTheRunningAction(t *testing.T) {
	fastHeartbeats(t)
	s, addr := newFrameServer(t, pollOnce(OpActionAwait, oneTask(), nil))
	running := make(chan struct{})
	w := drainTestWorker(t, addr, slowHandler(running, time.Hour))
	done := startWorker(w.Start)
	<-running
	w.Drain()
	time.Sleep(100 * time.Millisecond)
	w.Stop()
	if err := waitStart(t, done); err != context.Canceled {
		t.Errorf("Start = %v after a stop, want context.Canceled", err)
	}
	checkSends(t, s, 0, 1)
}

func TestAWorkerRestartsAfterADrain(t *testing.T) {
	fastHeartbeats(t)
	var polls atomic.Int32
	s, addr := newFrameServer(t, func(op OpCode, n int) reply {
		if op == OpActionAwait {
			polls.Add(1)
			time.Sleep(20 * time.Millisecond)
		}
		return reply{}
	})
	w := drainTestWorker(t, addr, succeed)
	for i := 0; i < 2; i++ {
		done := startWorker(w.Start)
		for before := polls.Load(); polls.Load() == before; {
			time.Sleep(5 * time.Millisecond)
		}
		w.Drain()
		if err := waitStart(t, done); err != nil {
			t.Fatalf("Start = %v after a drain, want nil", err)
		}
	}
	if n := len(s.got(OpWorkerRegister)); n < 2 {
		t.Errorf("registered %d times over two starts", n)
	}
}

// streamDrainWorker is a stream worker on addr whose handler signals running,
// then works for d whatever its context says.
func streamDrainWorker(t *testing.T, addr string, d time.Duration) (*StreamWorker, <-chan struct{}, *atomic.Bool) {
	t.Helper()
	running := make(chan struct{})
	finished := &atomic.Bool{}
	sw, err := NewClient(addr, WithTimeout(time.Second)).NewStreamWorker(StreamWorkerOptions{
		Stream:                      "s",
		RedeliverPendingOnReconnect: new(bool),
		Logger:                      &captureLogger{},
	}, func(*StreamContext) error {
		close(running)
		time.Sleep(d)
		finished.Store(true)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sw.Close() })
	return sw, running, finished
}

// ackedBeforeLeave reports whether the server saw the ack before the leave.
func ackedBeforeLeave(s *frameServer) bool {
	order := s.arrivals()
	ack, leave := slices.Index(order, OpStreamGroupAck), slices.Index(order, OpStreamGroupLeave)
	return ack >= 0 && leave > ack
}

func TestStreamWorkerStartWaitsForItsHandlersOnStop(t *testing.T) {
	s, addr := newFrameServer(t, pollOnce(OpStreamGroupRead, oneRecord, nil))
	sw, running, finished := streamDrainWorker(t, addr, 300*time.Millisecond)
	done := startWorker(sw.Start)
	<-running
	sw.Stop()
	waitStart(t, done)
	if !finished.Load() {
		t.Error("Start returned before its record handler finished")
	}
	if !ackedBeforeLeave(s) {
		t.Errorf("want the ack before the group leave, saw %v", s.arrivals())
	}
}

func TestStreamWorkerDrainFinishesItsRecordsThenLeaves(t *testing.T) {
	fastHeartbeats(t)
	s, addr := newFrameServer(t, pollOnce(OpStreamGroupRead, oneRecord, nil))
	sw, running, finished := streamDrainWorker(t, addr, 300*time.Millisecond)
	done := startWorker(sw.Start)
	<-running
	sw.Drain()
	if err := waitStart(t, done); err != nil {
		t.Errorf("Start = %v after a drain, want nil", err)
	}
	if !finished.Load() {
		t.Error("Start returned before its record handler finished")
	}
	if !ackedBeforeLeave(s) {
		t.Errorf("want the ack before the group leave, saw %v", s.arrivals())
	}
	if n := len(s.got(OpWorkerDrain)); n != 1 {
		t.Errorf("sent %d drain notices, want 1", n)
	}
}

func TestStreamWorkerADrainingHeartbeatLetsHandlersFinish(t *testing.T) {
	fastHeartbeats(t)
	s, addr := newFrameServer(t, pollOnce(OpStreamGroupRead, oneRecord, drainingHeartbeat))
	running := make(chan struct{})
	var cancelled atomic.Bool
	sw, err := NewClient(addr, WithTimeout(time.Second)).NewStreamWorker(StreamWorkerOptions{
		Stream:                      "s",
		RedeliverPendingOnReconnect: new(bool),
		Logger:                      &captureLogger{},
	}, func(ctx *StreamContext) error {
		close(running)
		select {
		case <-time.After(300 * time.Millisecond):
			return nil
		case <-ctx.Context().Done():
			cancelled.Store(true)
			return ctx.Context().Err()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sw.Close() })
	done := startWorker(sw.Start)
	<-running
	if err := waitStart(t, done); err != nil {
		t.Errorf("Start = %v after a drain, want nil", err)
	}
	if cancelled.Load() {
		t.Error("the drain cancelled a running handler")
	}
	if n := len(s.got(OpStreamGroupNack)); n != 0 {
		t.Errorf("saw %d nacks, want 0", n)
	}
	if !ackedBeforeLeave(s) {
		t.Errorf("want the ack before the group leave, saw %v", s.arrivals())
	}
}

func TestADrainBeforeStartEndsTheRunWithoutPolling(t *testing.T) {
	fastHeartbeats(t)
	s, addr := newFrameServer(t, pollOnce(OpActionAwait, oneTask(), nil))
	w := drainTestWorker(t, addr, succeed)
	w.Drain()
	if err := waitStart(t, startWorker(w.Start)); err != nil {
		t.Errorf("Start = %v after an early drain, want nil", err)
	}
	if n := len(s.got(OpActionAwait)); n != 0 {
		t.Errorf("polled %d times after an early drain, want 0", n)
	}
}

func TestStreamWorkerADrainBeforeStartEndsTheRunWithoutReading(t *testing.T) {
	fastHeartbeats(t)
	s, addr := newFrameServer(t, pollOnce(OpStreamGroupRead, oneRecord, nil))
	sw, _, _ := streamDrainWorker(t, addr, 0)
	sw.Drain()
	if err := waitStart(t, startWorker(sw.Start)); err != nil {
		t.Errorf("Start = %v after an early drain, want nil", err)
	}
	if n := len(s.got(OpStreamGroupRead)); n != 0 {
		t.Errorf("read %d times after an early drain, want 0", n)
	}
}

// drainRefused checks that a drain finishes, its handler done, while every
// connect is refused: it drains once the poll loop is backing off between
// refused reconnects, which then must end at once.
func drainRefused(t *testing.T, s *frameServer, running <-chan struct{}, finished func() bool, drain func(), done <-chan error) {
	t.Helper()
	<-running
	s.refuse()
	time.Sleep(300 * time.Millisecond)
	drain()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Start = %v after a drain, want nil", err)
		}
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("drain did not finish while connects were refused")
	}
	if !finished() {
		t.Error("Start returned before its handler finished")
	}
}

func TestADrainFinishesWhileConnectsAreRefused(t *testing.T) {
	fastHeartbeats(t)
	s, addr := newFrameServer(t, pollOnce(OpActionAwait, oneTask(), nil))
	running := make(chan struct{})
	var finished atomic.Bool
	w := drainTestWorker(t, addr, func(ctx *ActionContext) (interface{}, error) {
		close(running)
		time.Sleep(600 * time.Millisecond)
		finished.Store(true)
		return nil, nil
	})
	drainRefused(t, s, running, finished.Load, w.Drain, startWorker(w.Start))
}

func TestStreamWorkerADrainFinishesWhileConnectsAreRefused(t *testing.T) {
	fastHeartbeats(t)
	s, addr := newFrameServer(t, pollOnce(OpStreamGroupRead, oneRecord, nil))
	sw, running, finished := streamDrainWorker(t, addr, 600*time.Millisecond)
	drainRefused(t, s, running, finished.Load, sw.Drain, startWorker(sw.Start))
}
