package flo

import (
	"context"
	"encoding/binary"
	"strings"
	"testing"
	"time"
)

// notServedYet refuses as the server does a group read or claim of records
// it is reading back from disk: unavailable, ran no.
var notServedYet = reply{status: StatusUnavailable, body: refusalBody(ReasonUnclassified, RanNo, "unavailable: records past the warm store are being read back from disk; retry")}

// A group read refused unavailable (ran no) claimed nothing, so the worker
// reads again, past the client's own retries, and handles the record once
// it's served.
func TestAStreamWorkerRereadsAGroupReadRefusedUnavailable(t *testing.T) {
	fastHeartbeats(t)
	s, addr := newFrameServer(t, func(op OpCode, n int) reply {
		switch op {
		case OpWorkerHeartbeat:
			return activeBeat()
		case OpStreamGroupRead:
			if n < 6 {
				return notServedYet
			}
			if n == 6 {
				return reply{body: oneRecord}
			}
			time.Sleep(20 * time.Millisecond)
		}
		return reply{}
	})
	sw, running, _ := streamDrainWorker(t, addr, 0)
	done := startWorker(sw.Start)
	select {
	case <-running:
	case <-time.After(5 * time.Second):
		t.Fatalf("the record was never handled; %d group reads", len(s.got(OpStreamGroupRead)))
	}
	sw.Stop()
	waitStart(t, done)
	logger := sw.logger.(*captureLogger)
	if !logger.has("GroupRead not served yet") {
		t.Errorf("no routine retry logged: %q", logger.all())
	}
	for _, line := range logger.all() {
		if strings.Contains(line, "GroupRead error") {
			t.Errorf("a refusal that ran nothing was logged as an error: %q", line)
		}
	}
}

// A pending-drain claim refused unavailable (ran no) is sent again from the
// same cursor, not abandoned until the next reconnect.
func TestAPendingDrainClaimRefusedUnavailableIsSentAgain(t *testing.T) {
	done := make([]byte, 16)
	binary.LittleEndian.PutUint64(done[0:], ^uint64(0))
	binary.LittleEndian.PutUint64(done[8:], ^uint64(0))
	s, addr := newFrameServer(t, func(op OpCode, n int) reply {
		if op == OpStreamGroupClaim {
			if n < 6 {
				return notServedYet
			}
			return reply{body: append(append([]byte{}, oneRecord...), done...)}
		}
		return reply{}
	})
	sw, running, _ := streamDrainWorker(t, addr, 0)
	if err := sw.client.Connect(); err != nil {
		t.Fatal(err)
	}
	if err := sw.ackClient.Connect(); err != nil {
		t.Fatal(err)
	}
	sw.ctx, sw.cancel = context.WithCancel(context.Background())
	sw.pollCtx = sw.ctx
	sw.sem = make(chan struct{}, 1)
	sw.drainPending()
	select {
	case <-running:
	case <-time.After(5 * time.Second):
		t.Fatalf("the claimed record was never handled; %d claims", len(s.got(OpStreamGroupClaim)))
	}
	sw.handlers.Wait()
	if n := len(s.got(OpStreamGroupClaim)); n != 7 {
		t.Errorf("%d claims, want 7: six refused, then the one served", n)
	}
}
