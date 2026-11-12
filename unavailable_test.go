package flo

import (
	"strings"
	"testing"
	"time"
)

// notServedYet refuses as the server does a group read of records
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
