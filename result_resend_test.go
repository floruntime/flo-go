package flo

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// reply says how frameServer answers one request.
type reply struct {
	hangUp bool       // close the connection without answering
	stall  bool       // never answer
	status StatusCode // answer with this status...
	body   []byte     // ...and body
	// Unreadable answers, whose outcome the client can't know.
	badCRC  bool
	wrongID bool
}

// frameServer records every request frame it receives and answers each as
// decide says; n counts earlier requests with the same op.
type frameServer struct {
	mu       sync.Mutex
	frames   map[OpCode][][]byte
	order    []OpCode // every request's op, in arrival order
	accepted atomic.Int32
	decide   func(op OpCode, n int) reply
}

func newFrameServer(t *testing.T, decide func(op OpCode, n int) reply) (*frameServer, string) {
	t.Helper()
	s := &frameServer{frames: map[OpCode][][]byte{}, decide: decide}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.accepted.Add(1)
			t.Cleanup(func() { conn.Close() })
			go s.serve(conn)
		}
	}()
	return s, ln.Addr().String()
}

func (s *frameServer) serve(conn net.Conn) {
	defer conn.Close()
	for {
		header := make([]byte, HeaderSize)
		if _, err := io.ReadFull(conn, header); err != nil {
			return
		}
		payload := make([]byte, binary.LittleEndian.Uint32(header[4:8]))
		if _, err := io.ReadFull(conn, payload); err != nil {
			return
		}
		op := OpCode(binary.LittleEndian.Uint16(header[20:22]))
		s.mu.Lock()
		n := len(s.frames[op])
		s.frames[op] = append(s.frames[op], append(header, payload...))
		s.order = append(s.order, op)
		s.mu.Unlock()
		r := s.decide(op, n)
		switch {
		case r.hangUp:
			return
		case r.stall:
			continue
		}
		id := binary.LittleEndian.Uint64(header[8:16])
		if r.wrongID {
			id++
		}
		f := statusFrame(id, r.status, r.body)
		if r.badCRC {
			f[16] ^= 0xff
		}
		if _, err := conn.Write(f); err != nil {
			return
		}
	}
}

// arrivals is every request's op so far, in arrival order.
func (s *frameServer) arrivals() []OpCode {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]OpCode(nil), s.order...)
}

func (s *frameServer) got(op OpCode) [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]byte(nil), s.frames[op]...)
}

// resultTestWorker is an action worker on addr, with request timeout
// netTimeout and ActionTimeout actionTimeout, whose handler for action "a"
// is h, ready for executeTask without Start.
func resultTestWorker(t *testing.T, addr string, netTimeout, actionTimeout time.Duration, h ActionHandler) (*ActionWorker, *captureLogger) {
	t.Helper()
	logger := &captureLogger{}
	w, err := NewClient(addr, WithTimeout(netTimeout)).NewActionWorker(ActionWorkerOptions{ActionTimeout: actionTimeout, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	w.handlers["a"] = h
	w.ctx, w.cancel = context.WithCancel(context.Background())
	return w, logger
}

func runTask(w *ActionWorker) {
	w.executeTask(w.resultClient.workerClient(w.config.WorkerID), &TaskAssignment{TaskType: "a", TaskID: "t1"})
}

func succeed(*ActionContext) (interface{}, error) {
	return &ActionResult{Outcome: "done", Data: []byte(`{"n":1}`)}, nil
}

func checkSends(t *testing.T, s *frameServer, completes, fails int) {
	t.Helper()
	if n := len(s.got(OpActionComplete)); n != completes {
		t.Errorf("server saw %d completes, want %d", n, completes)
	}
	if n := len(s.got(OpActionFail)); n != fails {
		t.Errorf("server saw %d fails, want %d", n, fails)
	}
}

// hangUpFirst drops the connection on the first request with op.
func hangUpFirst(op OpCode) func(OpCode, int) reply {
	return func(got OpCode, n int) reply {
		return reply{hangUp: got == op && n == 0}
	}
}

func TestDroppedCompleteIsResentNeverFailed(t *testing.T) {
	s, addr := newFrameServer(t, hangUpFirst(OpActionComplete))
	w, _ := resultTestWorker(t, addr, time.Second, time.Minute, succeed)
	runTask(w)
	checkSends(t, s, 2, 0)
}

func TestTimedOutCompleteIsResentNeverFailed(t *testing.T) {
	s, addr := newFrameServer(t, func(op OpCode, n int) reply {
		return reply{stall: op == OpActionComplete && n == 0}
	})
	// A long ActionTimeout: results time out on the client's own timeout.
	w, _ := resultTestWorker(t, addr, 200*time.Millisecond, time.Hour, succeed)
	runTask(w)
	checkSends(t, s, 2, 0)
}

func TestDroppedAckIsResentNeverNacked(t *testing.T) {
	s, addr := newFrameServer(t, hangUpFirst(OpStreamGroupAck))
	sw, err := NewClient(addr, WithTimeout(time.Second)).NewStreamWorker(
		StreamWorkerOptions{Stream: "s"}, func(*StreamContext) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sw.Close() })
	sw.ctx, sw.cancel = context.WithCancel(context.Background())
	sw.processRecord("s", StreamRecord{ID: testID})
	acks := s.got(OpStreamGroupAck)
	if len(acks) != 2 {
		t.Errorf("server saw %d acks, want 2", len(acks))
	}
	if n := len(s.got(OpStreamGroupNack)); n != 0 {
		t.Errorf("server saw %d nacks, want 0", n)
	}
	if len(acks) == 2 && !sameApartFromID(acks[0], acks[1]) {
		t.Error("resent ack differs from the first")
	}
}

func TestRefusedCompleteIsNotResent(t *testing.T) {
	for _, status := range []StatusCode{StatusNotFound, StatusBadRequest} {
		t.Run(status.String(), func(t *testing.T) {
			s, addr := newFrameServer(t, func(op OpCode, n int) reply {
				if op == OpActionComplete {
					return reply{status: status, body: refusalBody(ReasonUnclassified, RanNo, "no")}
				}
				return reply{}
			})
			w, logger := resultTestWorker(t, addr, time.Second, time.Minute, succeed)
			runTask(w)
			checkSends(t, s, 1, 0)
			if !logger.has("Error: dropped complete for task t1") {
				t.Errorf("no error naming the task; log: %q", logger.lines)
			}
			if logger.has("Action completed") {
				t.Error("a refused complete was logged as completed")
			}
		})
	}
}

// A handler that gives up because the worker is stopping is failed with
// retry, so the task runs again elsewhere.
func TestStopDuringHandlerFailsWithRetry(t *testing.T) {
	s, addr := newFrameServer(t, func(op OpCode, n int) reply {
		if op == OpActionAwait && n == 0 {
			return reply{body: taskT1()}
		}
		return reply{}
	})
	started := make(chan struct{})
	w, _ := resultTestWorker(t, addr, time.Second, time.Minute, func(actx *ActionContext) (interface{}, error) {
		close(started)
		<-actx.Ctx().Done()
		return nil, actx.Ctx().Err()
	})
	done := make(chan struct{})
	go func() {
		w.Start(context.Background())
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never ran")
	}
	w.Stop()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after Stop")
	}
	fails := s.got(OpActionFail)
	if len(fails) != 1 {
		t.Fatalf("server saw %d fails, want 1", len(fails))
	}
	// The value ends [retry:u8][message], then an empty options field.
	if want := []byte("\x01worker stopped\x00\x00"); !bytes.HasSuffix(fails[0], want) {
		t.Errorf("fail frame ends %q, want %q", fails[0][len(fails[0])-len(want):], want)
	}
	checkSends(t, s, 0, 1)
}

// A handler that hits ActionTimeout is still failed without retry.
func TestActionTimeoutFailsWithoutRetry(t *testing.T) {
	s, addr := newFrameServer(t, func(OpCode, int) reply { return reply{} })
	w, _ := resultTestWorker(t, addr, time.Second, 50*time.Millisecond, func(actx *ActionContext) (interface{}, error) {
		<-actx.Ctx().Done()
		return nil, actx.Ctx().Err()
	})
	runTask(w)
	fails := s.got(OpActionFail)
	if len(fails) != 1 {
		t.Fatalf("server saw %d fails, want 1", len(fails))
	}
	if want := []byte("\x00action timed out\x00\x00"); !bytes.HasSuffix(fails[0], want) {
		t.Errorf("fail frame ends %q, want %q", fails[0][len(fails[0])-len(want):], want)
	}
}

// sameApartFromID compares two request frames, ignoring the request id
// (header bytes 8..16) and the CRC that covers it (16..20).
func sameApartFromID(a, b []byte) bool {
	if len(a) != len(b) || len(a) < HeaderSize {
		return false
	}
	mask := func(f []byte) []byte {
		f = append([]byte(nil), f...)
		clear(f[8:20])
		return f
	}
	return bytes.Equal(mask(a), mask(b))
}

func TestResentCompleteIsByteIdentical(t *testing.T) {
	s, addr := newFrameServer(t, hangUpFirst(OpActionComplete))
	w, _ := resultTestWorker(t, addr, time.Second, time.Minute, succeed)
	runTask(w)
	completes := s.got(OpActionComplete)
	if len(completes) != 2 {
		t.Fatalf("server saw %d completes, want 2", len(completes))
	}
	if bytes.Equal(completes[0][8:16], completes[1][8:16]) {
		t.Error("resend reused the request id")
	}
	if !sameApartFromID(completes[0], completes[1]) {
		t.Errorf("resent complete differs:\n%x\n%x", completes[0], completes[1])
	}
}

func TestCompleteDroppedAfterThreeSends(t *testing.T) {
	s, addr := newFrameServer(t, func(op OpCode, n int) reply {
		return reply{hangUp: op == OpActionComplete}
	})
	w, logger := resultTestWorker(t, addr, time.Second, time.Minute, succeed)
	runTask(w)
	checkSends(t, s, 3, 0)
	if !logger.has("dropped complete for task t1") {
		t.Errorf("no drop logged naming the task; log: %q", logger.lines)
	}
	// Two connections from NewActionWorker, one reconnect between each pair
	// of sends, and none after the last.
	if n := s.accepted.Load(); n != 4 {
		t.Errorf("server accepted %d connections, want 4", n)
	}
}

// taskT1 is an await answer assigning task "t1" of action "a".
func taskT1() []byte {
	task := []byte{2, 0, 't', '1', 1, 0, 'a'}
	task = binary.LittleEndian.AppendUint64(task, 0) // created_at
	task = binary.LittleEndian.AppendUint32(task, 1) // attempt
	return append(task, 0)                           // has_caller
}

// A result send that stalls when Stop is called is given up within the
// stop deadline, so Start returns promptly, and the drop is logged.
func TestStopDoesNotWaitOutAStalledResult(t *testing.T) {
	s, addr := newFrameServer(t, func(op OpCode, n int) reply {
		switch op {
		case OpActionAwait:
			if n == 0 {
				return reply{body: taskT1()}
			}
		case OpActionComplete:
			return reply{stall: true}
		}
		return reply{}
	})
	// The request timeout alone would hold the send for a minute.
	w, logger := resultTestWorker(t, addr, time.Minute, time.Minute, succeed)
	done := make(chan struct{})
	go func() {
		w.Start(context.Background())
		close(done)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for len(s.got(OpActionComplete)) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("complete never sent")
		}
		time.Sleep(5 * time.Millisecond)
	}
	stopAt := time.Now()
	w.Stop()
	select {
	case <-done:
	case <-time.After(stopResultWait + 3*time.Second):
		t.Fatalf("Start still running %v after Stop", time.Since(stopAt))
	}
	if !logger.has("Error: dropped complete for task t1 while the worker stops") {
		t.Errorf("no drop logged; log: %q", logger.lines)
	}
	checkSends(t, s, 1, 0)
}

// An answer the client can't read leaves the outcome unknown, so the
// complete is resent.
func TestUnreadableCompleteAnswerIsResent(t *testing.T) {
	for name, bad := range map[string]reply{"bad crc": {badCRC: true}, "wrong request id": {wrongID: true}} {
		t.Run(name, func(t *testing.T) {
			s, addr := newFrameServer(t, func(op OpCode, n int) reply {
				if op == OpActionComplete && n == 0 {
					return bad
				}
				return reply{}
			})
			w, _ := resultTestWorker(t, addr, time.Second, time.Minute, succeed)
			runTask(w)
			checkSends(t, s, 2, 0)
		})
	}
}
