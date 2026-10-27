package flo

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWorkerRefusalReasonsDecode(t *testing.T) {
	for _, c := range []struct {
		reason Reason
		name   string
	}{{ReasonWorkerUnhealthy, "worker_unhealthy"}, {ReasonWorkerDraining, "worker_draining"}} {
		err := checkStatus(StatusBadRequest, refusalBody(c.reason, RanNo, "x"), false)
		var se *ServerError
		if !errors.As(err, &se) || se.Reason != c.reason || se.Ran != RanNo || se.Reason.String() != c.name {
			t.Errorf("reason %d: %v", c.reason, err)
		}
	}
}

// workerRow is one worker.list row as the server encodes it.
func workerRow(id string, typ WorkerType, status WorkerStatus, load uint32, machine *string) []byte {
	b := binary.LittleEndian.AppendUint16(nil, uint16(len(id)))
	b = append(b, id...)
	b = append(b, byte(typ), byte(status))
	b = binary.LittleEndian.AppendUint32(b, load)
	b = binary.LittleEndian.AppendUint32(b, 8)    // max_concurrency
	b = binary.LittleEndian.AppendUint64(b, 5)    // tasks_completed
	b = binary.LittleEndian.AppendUint64(b, 2)    // tasks_failed
	b = binary.LittleEndian.AppendUint64(b, 1000) // registered_at_ms
	b = binary.LittleEndian.AppendUint64(b, 2000) // last_heartbeat_ms
	if machine == nil {
		return append(b, 0)
	}
	return append(append(b, 1, byte(len(*machine))), *machine...)
}

func workerListAnswer(cursor *string, rows ...[]byte) []byte {
	b := binary.LittleEndian.AppendUint16(nil, uint16(len(rows)))
	for _, r := range rows {
		b = append(b, r...)
	}
	if cursor == nil {
		return append(b, 0)
	}
	return append(binary.LittleEndian.AppendUint16(append(b, 1), uint16(len(*cursor))), *cursor...)
}

func TestWorkerListEncodesLimitAndCursor(t *testing.T) {
	body, err := encodeWorkerListBody(25, []byte("w-07"))
	if err != nil {
		t.Fatal(err)
	}
	if want := []byte{1, 25, 0, 0, 0, 4, 0, 'w', '-', '0', '7'}; !bytes.Equal(body, want) {
		t.Errorf("body % x, want % x", body, want)
	}
	body, _ = encodeWorkerListBody(0, nil)
	if want := []byte{1, 0, 4, 0, 0, 0, 0}; !bytes.Equal(body, want) {
		t.Errorf("default body % x, want % x", body, want)
	}
	if _, err := encodeWorkerListBody(MaxWorkerListLimit+1, nil); err == nil {
		t.Error("limit past the most was sent")
	}
	if _, err := encodeWorkerListBody(1, make([]byte, maxWorkerID+1)); err == nil {
		t.Error("cursor past the longest id was sent")
	}
	if !isTableOp(OpWorkerList) {
		t.Error("worker.list is a table op")
	}
}

func TestWorkerListDecodesRowsAndCursor(t *testing.T) {
	host, next := "host-a", "w-2"
	got, err := decodeWorkerListAnswer(workerListAnswer(&next,
		workerRow("w-1", WorkerTypeAction, WorkerStatusActive, 3, &host),
		workerRow("w-2", WorkerTypeStream, WorkerStatusDraining, 0, nil)))
	if err != nil {
		t.Fatal(err)
	}
	want := []WorkerInfo{
		{ID: "w-1", Type: WorkerTypeAction, Status: WorkerStatusActive, CurrentLoad: 3, MaxConcurrency: 8, TasksCompleted: 5, TasksFailed: 2, RegisteredAtMS: 1000, LastHeartbeatMS: 2000, MachineID: "host-a"},
		{ID: "w-2", Type: WorkerTypeStream, Status: WorkerStatusDraining, MaxConcurrency: 8, TasksCompleted: 5, TasksFailed: 2, RegisteredAtMS: 1000, LastHeartbeatMS: 2000},
	}
	if !slices.Equal(got.Workers, want) || string(got.Cursor) != "w-2" {
		t.Fatalf("decoded %+v", got)
	}
	last, err := decodeWorkerListAnswer(workerListAnswer(nil))
	if err != nil || len(last.Workers) != 0 || last.Cursor != nil {
		t.Fatalf("last page %+v, %v", last, err)
	}
}

func TestWorkerListRefusesACutAnswerAndUnknownValues(t *testing.T) {
	host, next := "h", "c"
	whole := workerListAnswer(&next, workerRow("w", WorkerTypeAction, WorkerStatusIdle, 0, &host))
	for n := range whole {
		if _, err := decodeWorkerListAnswer(whole[:n]); !errors.Is(err, ErrIncompleteResponse) {
			t.Fatalf("cut at %d: %v", n, err)
		}
	}
	for _, row := range [][]byte{
		workerRow("w", WorkerType(2), WorkerStatusIdle, 0, nil),
		workerRow("w", WorkerTypeAction, WorkerStatus(4), 0, nil),
	} {
		if _, err := decodeWorkerListAnswer(workerListAnswer(nil, row)); !errors.Is(err, ErrIncompleteResponse) {
			t.Errorf("unknown type or status: %v", err)
		}
	}
}

// ListWorkers sends [namespace][empty key][body] and reads the page.
func TestListWorkersSendsATableRequest(t *testing.T) {
	s, addr := newFrameServer(t, func(op OpCode, n int) reply {
		return reply{body: workerListAnswer(nil, workerRow("w-1", WorkerTypeAction, WorkerStatusIdle, 0, nil))}
	})
	c := NewClient(addr, WithNamespace("ns"), WithTimeout(time.Second))
	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	page, err := c.ListWorkers(&WorkerListOptions{Limit: 2, Cursor: []byte("w-0")})
	if err != nil || len(page.Workers) != 1 || page.Workers[0].ID != "w-1" || page.Cursor != nil {
		t.Fatalf("page %+v, %v", page, err)
	}
	frames := s.got(OpWorkerList)
	if len(frames) != 1 {
		t.Fatalf("sent %d worker.list requests", len(frames))
	}
	want := []byte{2, 0, 'n', 's', 0, 0, 1, 2, 0, 0, 0, 3, 0, 'w', '-', '0'}
	if got := frames[0][HeaderSize:]; !bytes.Equal(got, want) {
		t.Errorf("payload % x, want % x", got, want)
	}
}

// processRow is one worker.info process row as the server encodes it.
func processRow(name string, kind ProcessKind, runs, fails uint64, lastRun int64) []byte {
	b := binary.LittleEndian.AppendUint16(nil, uint16(len(name)))
	b = append(append(b, name...), byte(kind))
	b = binary.LittleEndian.AppendUint64(b, runs)
	b = binary.LittleEndian.AppendUint64(b, fails)
	return binary.LittleEndian.AppendUint64(b, uint64(lastRun))
}

func workerInfoAnswer(row []byte, metadata *string, processes ...[]byte) []byte {
	b := append([]byte{}, row...)
	if metadata == nil {
		b = append(b, 0)
	} else {
		b = append(binary.LittleEndian.AppendUint16(append(b, 1), uint16(len(*metadata))), *metadata...)
	}
	b = binary.LittleEndian.AppendUint16(b, uint16(len(processes)))
	for _, p := range processes {
		b = append(b, p...)
	}
	return b
}

func TestWorkerInfoDecodesRowMetadataAndProcesses(t *testing.T) {
	host, meta := "host-a", `{"zone":"a"}`
	got, err := decodeWorkerInfoAnswer(workerInfoAnswer(workerRow("w-1", WorkerTypeAction, WorkerStatusActive, 3, &host), &meta,
		processRow("resize", ProcessKindAction, 7, 1, 1500),
		processRow("events/g", ProcessKindStreamConsumer, 0, 0, 0)))
	if err != nil {
		t.Fatal(err)
	}
	want := WorkerDetails{
		Worker:   WorkerInfo{ID: "w-1", Type: WorkerTypeAction, Status: WorkerStatusActive, CurrentLoad: 3, MaxConcurrency: 8, TasksCompleted: 5, TasksFailed: 2, RegisteredAtMS: 1000, LastHeartbeatMS: 2000, MachineID: "host-a"},
		Metadata: meta,
		Processes: []WorkerProcess{
			{Name: "resize", Kind: ProcessKindAction, RunCount: 7, FailCount: 1, LastRunAtMS: 1500},
			{Name: "events/g", Kind: ProcessKindStreamConsumer},
		},
	}
	if got.Worker != want.Worker || got.Metadata != want.Metadata || !slices.Equal(got.Processes, want.Processes) {
		t.Fatalf("decoded %+v, want %+v", got, want)
	}

	bare, err := decodeWorkerInfoAnswer(workerInfoAnswer(workerRow("w-2", WorkerTypeStream, WorkerStatusIdle, 0, nil), nil))
	if err != nil || bare.Worker.ID != "w-2" || bare.Worker.MachineID != "" || bare.Metadata != "" || len(bare.Processes) != 0 {
		t.Fatalf("no metadata or processes: %+v, %v", bare, err)
	}
}

func TestWorkerInfoRefusesACutAnswerAndUnknownKind(t *testing.T) {
	meta := "m"
	whole := workerInfoAnswer(workerRow("w", WorkerTypeAction, WorkerStatusIdle, 0, nil), &meta, processRow("p", ProcessKindAction, 1, 0, 9))
	for n := range whole {
		if _, err := decodeWorkerInfoAnswer(whole[:n]); !errors.Is(err, ErrIncompleteResponse) {
			t.Fatalf("cut at %d: %v", n, err)
		}
	}
	if _, err := decodeWorkerInfoAnswer(append(whole, 0)); !errors.Is(err, ErrIncompleteResponse) {
		t.Errorf("trailing byte: %v", err)
	}
	bad := workerInfoAnswer(workerRow("w", WorkerTypeAction, WorkerStatusIdle, 0, nil), nil, processRow("p", ProcessKind(2), 0, 0, 0))
	if _, err := decodeWorkerInfoAnswer(bad); !errors.Is(err, ErrIncompleteResponse) {
		t.Errorf("unknown process kind: %v", err)
	}
}

// WorkerInfo sends [namespace][worker id] and no body, and a missing worker
// is not found.
func TestWorkerInfoSendsATableRequest(t *testing.T) {
	s, addr := newFrameServer(t, func(op OpCode, n int) reply {
		if n == 0 {
			return reply{body: workerInfoAnswer(workerRow("w-1", WorkerTypeAction, WorkerStatusIdle, 0, nil), nil)}
		}
		return notFound("worker not found")
	})
	c := NewClient(addr, WithNamespace("ns"), WithTimeout(time.Second))
	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	got, err := c.WorkerInfo("w-1", nil)
	if err != nil || got.Worker.ID != "w-1" {
		t.Fatalf("info %+v, %v", got, err)
	}
	frames := s.got(OpWorkerInfo)
	want := []byte{2, 0, 'n', 's', 3, 0, 'w', '-', '1'}
	if len(frames) != 1 || !bytes.Equal(frames[0][HeaderSize:], want) {
		t.Fatalf("sent %d requests, first payload % x, want % x", len(frames), frames, want)
	}
	if _, err := c.WorkerInfo("gone", nil); !IsNotFound(err) {
		t.Errorf("missing worker: %v, want not found", err)
	}
}

func notFound(msg string) reply {
	return reply{status: StatusNotFound, body: refusalBody(ReasonNotFound, RanNo, msg)}
}

func activeBeat() reply { return reply{body: []byte{byte(WorkerStatusActive)}} }

// registerBodies is the payload of each registration the server saw.
func registerBodies(s *frameServer) [][]byte {
	var out [][]byte
	for _, f := range s.got(OpWorkerRegister) {
		out = append(out, f[HeaderSize:])
	}
	return out
}

// waitFor polls cond for up to 5 s.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// removedOnce answers the first heartbeat not_found, as flo does for a
// worker it removed for its silence, and later ones active.
func removedOnce(pollOp OpCode, work []byte) func(OpCode, int) reply {
	return func(op OpCode, n int) reply {
		switch op {
		case OpWorkerHeartbeat:
			if n == 0 {
				return notFound("worker not found")
			}
			return activeBeat()
		case pollOp:
			if n == 0 && work != nil {
				return reply{body: work}
			}
			time.Sleep(20 * time.Millisecond)
		}
		return reply{}
	}
}

func checkReregistered(t *testing.T, s *frameServer, pollOp OpCode) {
	t.Helper()
	bodies := registerBodies(s)
	if len(bodies) != 2 {
		t.Fatalf("registered %d times, want 2", len(bodies))
	}
	if !bytes.Equal(bodies[0], bodies[1]) {
		t.Errorf("re-registered with % x, first registered with % x", bodies[1], bodies[0])
	}
	order := s.arrivals()
	reg := slices.Index(order[slices.Index(order, OpWorkerHeartbeat):], OpWorkerRegister)
	if reg < 0 {
		t.Errorf("no registration after the not_found heartbeat: %v", order)
	}
}

func TestAnActionWorkerRemovedByTheServerRegistersAgain(t *testing.T) {
	fastHeartbeats(t)
	s, addr := newFrameServer(t, removedOnce(OpActionAwait, nil))
	w := drainTestWorker(t, addr, succeed)
	w.config.MachineID = "host-a"
	done := startWorker(w.Start)
	waitFor(t, "a second registration", func() bool { return len(s.got(OpWorkerRegister)) == 2 })
	polls := len(s.got(OpActionAwait))
	waitFor(t, "polls to go on", func() bool { return len(s.got(OpActionAwait)) > polls+1 })
	w.Stop()
	waitStart(t, done)
	checkReregistered(t, s, OpActionAwait)
}

func TestAStreamWorkerRemovedByTheServerRegistersAgain(t *testing.T) {
	fastHeartbeats(t)
	s, addr := newFrameServer(t, removedOnce(OpStreamGroupRead, nil))
	sw, _, _ := streamDrainWorker(t, addr, 0)
	done := startWorker(sw.Start)
	waitFor(t, "a second registration", func() bool { return len(s.got(OpWorkerRegister)) == 2 })
	reads := len(s.got(OpStreamGroupRead))
	waitFor(t, "reads to go on", func() bool { return len(s.got(OpStreamGroupRead)) > reads+1 })
	sw.Stop()
	waitStart(t, done)
	checkReregistered(t, s, OpStreamGroupRead)
}

// A draining worker the server removes registers again, which marks it
// active, so it tells the server it is draining once more.
func TestADrainingWorkerRemovedByTheServerSendsItsDrainNoticeAgain(t *testing.T) {
	fastHeartbeats(t)
	var mu sync.Mutex
	told, removed := false, false
	s, addr := newFrameServer(t, func(op OpCode, n int) reply {
		mu.Lock()
		defer mu.Unlock()
		switch op {
		case OpWorkerDrain:
			told = true
		case OpWorkerHeartbeat:
			if told && !removed {
				removed = true
				return notFound("worker not found")
			}
			return activeBeat()
		case OpActionAwait:
			if n == 0 {
				return reply{body: oneTask()}
			}
			mu.Unlock()
			time.Sleep(20 * time.Millisecond)
			mu.Lock()
		}
		return reply{}
	})
	running := make(chan struct{})
	w := drainTestWorker(t, addr, slowHandler(running, 400*time.Millisecond))
	done := startWorker(w.Start)
	<-running
	w.Drain()
	if err := waitStart(t, done); err != nil {
		t.Errorf("Start = %v after a drain, want nil", err)
	}
	checkSends(t, s, 1, 0)
	order := s.arrivals()
	lastReg, lastDrain := lastIndex(order, OpWorkerRegister), lastIndex(order, OpWorkerDrain)
	// The first registration, the drain's own reconnect, and the one after
	// the not_found heartbeat.
	if n := len(s.got(OpWorkerRegister)); n != 3 {
		t.Fatalf("registered %d times, want 3", n)
	}
	if lastDrain < lastReg {
		t.Errorf("no drain notice after the re-registration: %v", order)
	}
}

func lastIndex(ops []OpCode, op OpCode) int {
	for i := len(ops) - 1; i >= 0; i-- {
		if ops[i] == op {
			return i
		}
	}
	return -1
}

// heartbeatLoad is the current_load a heartbeat frame reports.
func heartbeatLoad(f []byte) uint32 {
	p := f[HeaderSize:]
	off := 2 + int(binary.LittleEndian.Uint16(p))
	off += 2 + int(binary.LittleEndian.Uint16(p[off:]))
	return binary.LittleEndian.Uint32(p[off+4:])
}

// Heartbeats count running handlers, not the slot the outstanding poll holds.
func TestHeartbeatsReportOnlyRunningHandlers(t *testing.T) {
	fastHeartbeats(t)
	var finished sync.WaitGroup
	finished.Add(1)
	s, addr := newFrameServer(t, pollOnce(OpActionAwait, oneTask(), func(op OpCode, n int) reply {
		if op == OpWorkerHeartbeat {
			return activeBeat()
		}
		return reply{}
	}))
	running := make(chan struct{})
	w := drainTestWorker(t, addr, func(ctx *ActionContext) (interface{}, error) {
		defer finished.Done()
		close(running)
		time.Sleep(300 * time.Millisecond)
		return nil, nil
	})
	done := startWorker(w.Start)
	<-running
	finished.Wait()
	during := len(s.got(OpWorkerHeartbeat))
	waitFor(t, "heartbeats after the handler", func() bool { return len(s.got(OpWorkerHeartbeat)) > during+3 })
	w.Stop()
	waitStart(t, done)
	var loads []uint32
	for _, f := range s.got(OpWorkerHeartbeat) {
		loads = append(loads, heartbeatLoad(f))
	}
	if !slices.Contains(loads[:during], 1) || slices.ContainsFunc(loads, func(l uint32) bool { return l > 1 }) || loads[len(loads)-1] != 0 {
		t.Errorf("heartbeat loads %v, want 1 while the handler ran and 0 after", loads)
	}
}

// An await refused worker_unhealthy heartbeats at once, then polls again on
// the failure backoff, never in a hot loop.
func TestAnUnhealthyRefusalHeartbeatsAndBacksOff(t *testing.T) {
	hb := heartbeatInterval
	heartbeatInterval = time.Hour // only the refusal heartbeats
	t.Cleanup(func() { heartbeatInterval = hb })
	s, addr := newFrameServer(t, func(op OpCode, n int) reply {
		switch op {
		case OpActionAwait:
			return reply{status: StatusBadRequest, body: refusalBody(ReasonWorkerUnhealthy, RanNo, "worker missed its heartbeats; send one, or register again")}
		case OpWorkerHeartbeat:
			return activeBeat()
		}
		return reply{}
	})
	w := drainTestWorker(t, addr, succeed)
	done := startWorker(w.Start)
	time.Sleep(500 * time.Millisecond)
	w.Stop()
	waitStart(t, done)
	order := s.arrivals()
	if i := slices.Index(order, OpActionAwait); i < 0 || i+1 >= len(order) || order[i+1] != OpWorkerHeartbeat {
		t.Errorf("want a heartbeat right after the refused await, saw %v", order)
	}
	awaits := len(s.got(OpActionAwait))
	if awaits < 2 || awaits > 12 {
		t.Errorf("%d awaits in 500ms, want a few on the backoff", awaits)
	}
}

// An await refused worker_draining stops taking work as a Draining
// heartbeat does: the running action finishes and Start returns.
func TestADrainingRefusalDrainsTheWorker(t *testing.T) {
	fastHeartbeats(t)
	s, addr := newFrameServer(t, func(op OpCode, n int) reply {
		switch op {
		case OpActionAwait:
			if n == 0 {
				return reply{body: oneTask()}
			}
			return reply{status: StatusBadRequest, body: refusalBody(ReasonWorkerDraining, RanNo, "worker is draining")}
		case OpWorkerHeartbeat:
			return activeBeat()
		}
		return reply{}
	})
	running := make(chan struct{})
	w := drainTestWorker(t, addr, slowHandler(running, 300*time.Millisecond))
	logger := w.logger.(*captureLogger)
	done := startWorker(w.Start)
	<-running
	if err := waitStart(t, done); err != nil {
		t.Errorf("Start = %v after a draining refusal, want nil", err)
	}
	checkSends(t, s, 1, 0)
	if n := len(s.got(OpActionAwait)); n != 2 {
		t.Errorf("polled %d times, want 2 (no poll after the refusal)", n)
	}
	if !slices.ContainsFunc(logger.all(), func(l string) bool { return strings.Contains(l, "draining") }) {
		t.Errorf("no drain logged: %q", logger.all())
	}
}
