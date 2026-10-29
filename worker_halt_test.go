package flo

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

// haltedFor refuses the first n requests with op as a halted shard does,
// then answers as usual; polls pause a little, as a held one would.
func haltedFor(op OpCode, n int) func(OpCode, int) reply {
	return func(o OpCode, i int) reply {
		if o == op && i < n {
			return reply{status: StatusUnavailable, body: refusalBody(ReasonShardHalted, RanNo, "shard halted at entry 7")}
		}
		switch o {
		case OpWorkerHeartbeat:
			return activeBeat()
		case OpActionAwait, OpStreamGroupRead:
			time.Sleep(10 * time.Millisecond)
		}
		return reply{}
	}
}

func warnedHalted(t *testing.T, l *captureLogger, n int) {
	t.Helper()
	got := 0
	for _, line := range l.all() {
		if strings.HasPrefix(line, "Warning: ") && strings.Contains(line, "shard halted: shard halted at entry 7") {
			got++
		}
	}
	if got < n {
		t.Errorf("%d halt warnings, want %d: %q", got, n, l.all())
	}
}

// A worker whose shard is halted waits it out and carries on once it's
// repaired: at registration, at the group join, and at a poll.
func TestWorkersWaitOutAHaltedShard(t *testing.T) {
	fastHeartbeats(t)
	for _, tc := range []struct {
		name   string
		stream bool
		halted OpCode
	}{
		{"action worker registration", false, OpWorkerRegister},
		{"action worker poll", false, OpActionAwait},
		{"stream worker registration", true, OpWorkerRegister},
		{"stream worker group join", true, OpStreamGroupJoin},
		{"stream worker poll", true, OpStreamGroupRead},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, addr := newFrameServer(t, haltedFor(tc.halted, 3))
			var done <-chan error
			var logger *captureLogger
			var stop func()
			poll := OpActionAwait
			if tc.stream {
				sw, _, _ := streamDrainWorker(t, addr, 0)
				logger, stop, poll = sw.logger.(*captureLogger), sw.Stop, OpStreamGroupRead
				done = startWorker(sw.Start)
			} else {
				w := drainTestWorker(t, addr, succeed)
				logger, stop = w.logger.(*captureLogger), w.Stop
				done = startWorker(w.Start)
			}
			waitFor(t, "the halt to pass", func() bool { return len(s.got(tc.halted)) > 3 })
			waitFor(t, "polls after the halt", func() bool { return len(s.got(poll)) > 5 })
			select {
			case err := <-done:
				t.Fatalf("Start returned %v during the halt", err)
			default:
			}
			stop()
			waitStart(t, done)
			warnedHalted(t, logger, 3)
			if order := s.arrivals(); !slices.Contains(order, OpWorkerRegister) {
				t.Errorf("never registered: %v", order)
			}
		})
	}
}

// A stop ends the wait on a shard that stays halted.
func TestAStopEndsTheWaitOnAHaltedShard(t *testing.T) {
	_, addr := newFrameServer(t, haltedFor(OpWorkerRegister, 1<<30))
	w := drainTestWorker(t, addr, succeed)
	done := startWorker(w.Start)
	time.Sleep(150 * time.Millisecond)
	w.Stop()
	if err := waitStart(t, done); err == nil {
		t.Error("Start = nil after a stop during the halt, want the stop's error")
	}
	sw, _, _ := streamDrainWorker(t, addr, 0)
	done = startWorker(sw.Start)
	time.Sleep(150 * time.Millisecond)
	sw.Drain()
	waitStart(t, done)
}

const pendingMessage = "committed at entry 7, where the shard halted: a repair skips it; don't resend it unchanged"

// pendingOn refuses every request with op outcome_pending, as a shard that
// halted at it does, and answers the rest as usual.
func pendingOn(op OpCode) func(OpCode, int) reply {
	return func(o OpCode, _ int) reply {
		if o == op {
			return reply{status: StatusOutcomePending, body: refusalBody(ReasonShardHalted, RanUnknown, pendingMessage)}
		}
		switch o {
		case OpWorkerHeartbeat:
			return activeBeat()
		case OpActionAwait, OpStreamGroupRead:
			time.Sleep(10 * time.Millisecond)
		}
		return reply{}
	}
}

// An outcome_pending is never resent: whichever supervision call gets one,
// Start ends with it after that one send, without waiting.
func TestOutcomePendingEndsTheWorker(t *testing.T) {
	fastHeartbeats(t)
	for _, tc := range []struct {
		name   string
		stream bool
		op     OpCode
	}{
		{"action worker registration", false, OpWorkerRegister},
		{"action worker poll", false, OpActionAwait},
		{"stream worker registration", true, OpWorkerRegister},
		{"stream worker group join", true, OpStreamGroupJoin},
		{"stream worker poll", true, OpStreamGroupRead},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, addr := newFrameServer(t, pendingOn(tc.op))
			var done <-chan error
			if tc.stream {
				sw, _, _ := streamDrainWorker(t, addr, 0)
				done = startWorker(sw.Start)
			} else {
				done = startWorker(drainTestWorker(t, addr, succeed).Start)
			}
			var err error
			select {
			case err = <-done:
			case <-time.After(2 * time.Second):
				t.Fatalf("Start still running; %v sent %d times", tc.op, len(s.got(tc.op)))
			}
			var se *ServerError
			if !errors.As(err, &se) || se.Status != StatusOutcomePending || se.Reason != ReasonShardHalted || se.Message != pendingMessage {
				t.Fatalf("Start = %v, want the outcome_pending refusal", err)
			}
			if n := len(s.got(tc.op)); n != 1 {
				t.Errorf("%v sent %d times, want once", tc.op, n)
			}
		})
	}
}

// The halted wait's ceiling doubles up to 30 s exactly, and no wait is
// longer.
func TestTheHaltedWaitIsCappedAt30s(t *testing.T) {
	b := newHaltedBackoff()
	var top time.Duration
	for i := 0; i < 40; i++ {
		if d := b.next(); d > 30*time.Second {
			t.Fatalf("wait %d is %v, over 30 s", i, d)
		}
		top = max(top, b.ceiling)
	}
	if top != 30*time.Second || b.ceiling != 30*time.Second {
		t.Errorf("ceiling reached %v and ended at %v, want 30 s", top, b.ceiling)
	}
}

// A stop or a drain during a halted wait at registration or the group join
// ends Start promptly, and the halted call isn't sent again.
func TestAStopOrDrainEndsAHaltedWait(t *testing.T) {
	fastHeartbeats(t)
	for _, tc := range []struct {
		name   string
		stream bool
		op     OpCode
		drain  bool
	}{
		{"action registration, stop", false, OpWorkerRegister, false},
		{"action registration, drain", false, OpWorkerRegister, true},
		{"stream registration, stop", true, OpWorkerRegister, false},
		{"stream registration, drain", true, OpWorkerRegister, true},
		{"stream group join, stop", true, OpStreamGroupJoin, false},
		{"stream group join, drain", true, OpStreamGroupJoin, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, addr := newFrameServer(t, haltedFor(tc.op, 1<<30))
			var done <-chan error
			var end func()
			poll := OpActionAwait
			if tc.stream {
				sw, _, _ := streamDrainWorker(t, addr, 0)
				end, poll = sw.Stop, OpStreamGroupRead
				if tc.drain {
					end = sw.Drain
				}
				done = startWorker(sw.Start)
			} else {
				w := drainTestWorker(t, addr, succeed)
				end = w.Stop
				if tc.drain {
					end = w.Drain
				}
				done = startWorker(w.Start)
			}
			waitFor(t, "the halted wait", func() bool { return len(s.got(tc.op)) >= 2 })
			end()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatalf("Start still running a second after the %s", tc.name)
			}
			sent := len(s.got(tc.op))
			time.Sleep(200 * time.Millisecond)
			if n := len(s.got(tc.op)); n != sent {
				t.Errorf("%v sent %d more times after Start returned", tc.op, n-sent)
			}
			if n := len(s.got(poll)); n != 0 {
				t.Errorf("polled %d times", n)
			}
		})
	}
}

// A result or an ack refused by a halted shard isn't sent again: the
// record or task comes back by its own timeout.
func TestHaltedRefusalsOfResultsAreNotResent(t *testing.T) {
	refusals := map[string]reply{
		"shard halted":    {status: StatusUnavailable, body: refusalBody(ReasonShardHalted, RanNo, "halted")},
		"outcome pending": {status: StatusOutcomePending, body: refusalBody(ReasonShardHalted, RanUnknown, pendingMessage)},
	}
	for name, refusal := range refusals {
		t.Run(name, func(t *testing.T) {
			refuse := func(ops ...OpCode) func(OpCode, int) reply {
				return func(op OpCode, _ int) reply {
					if slices.Contains(ops, op) {
						return refusal
					}
					return reply{}
				}
			}
			s, addr := newFrameServer(t, refuse(OpActionComplete))
			w, _ := resultTestWorker(t, addr, time.Second, time.Minute, succeed)
			runTask(w)
			checkSends(t, s, 1, 0)

			s, addr = newFrameServer(t, refuse(OpActionFail))
			w, _ = resultTestWorker(t, addr, time.Second, time.Minute, func(*ActionContext) (interface{}, error) {
				return nil, errors.New("boom")
			})
			runTask(w)
			checkSends(t, s, 0, 1)

			s, addr = newFrameServer(t, refuse(OpStreamGroupAck))
			sw, err := NewClient(addr, WithTimeout(time.Second)).NewStreamWorker(
				StreamWorkerOptions{Stream: "s"}, func(*StreamContext) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { sw.Close() })
			sw.ctx, sw.cancel = context.WithCancel(context.Background())
			sw.pollCtx = sw.ctx
			sw.processRecord("s", StreamRecord{ID: testID})
			if n := len(s.got(OpStreamGroupAck)); n != 1 {
				t.Errorf("ack sent %d times, want once", n)
			}
		})
	}
}

// The halted wait ends with its context, not with its backoff: one drawn
// from an hour's ceiling returns at once once a stop or drain ends ctx.
func TestTheHaltedWaitEndsWithItsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	b := backoff{initial: time.Hour, max: time.Hour}
	refused := &ServerError{Status: StatusUnavailable, Reason: ReasonShardHalted, Ran: RanNo, Message: "halted"}
	done := make(chan bool, 1)
	go func() { done <- waitIfHalted(ctx, &captureLogger{}, "registration", refused, &b) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case waited := <-done:
		if !waited {
			t.Error("an unavailable shard_halted refusal wasn't waited on")
		}
	case <-time.After(time.Second):
		t.Fatal("the wait outlived its context")
	}
}
