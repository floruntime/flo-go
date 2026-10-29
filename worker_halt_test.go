package flo

import (
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
