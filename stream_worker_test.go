package flo

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// oneRecordGroupRead is a GroupRead body carrying a single record.
func oneRecordGroupRead() []byte {
	b := make([]byte, 0, 64)
	b = binary.LittleEndian.AppendUint32(b, 1) // count
	b = binary.LittleEndian.AppendUint64(b, 1) // sequence
	b = binary.LittleEndian.AppendUint64(b, 1) // timestamp_ms
	b = append(b, 0)                           // tier
	b = binary.LittleEndian.AppendUint32(b, 0) // partition
	b = append(b, 0)                           // key_present
	b = binary.LittleEndian.AppendUint32(b, 1) // payload length
	b = append(b, 'x')                         // payload
	b = binary.LittleEndian.AppendUint32(b, 0) // header count
	return b
}

func writeFakeResponse(conn net.Conn, requestID uint64, data []byte) error {
	frame := make([]byte, HeaderSize+len(data))
	binary.LittleEndian.PutUint32(frame[0:4], Magic)
	binary.LittleEndian.PutUint32(frame[4:8], uint32(len(data)))
	binary.LittleEndian.PutUint64(frame[8:16], requestID)
	frame[20] = Version
	frame[21] = byte(StatusOK)
	copy(frame[HeaderSize:], data)
	binary.LittleEndian.PutUint32(frame[16:20], computeCRC32(frame[:HeaderSize], data))
	_, err := conn.Write(frame)
	return err
}

// An ack must not queue behind the worker's next blocking GroupRead: that
// read can hold the polling connection for the whole BlockMS.
func TestStreamWorkerAckDoesNotWaitForPoll(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	const block = 3 * time.Second
	var reads atomic.Int32
	acked := make(chan time.Time, 1)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
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
					id := binary.LittleEndian.Uint64(header[8:16])
					var data []byte
					switch OpCode(binary.LittleEndian.Uint16(header[20:22])) {
					case OpStreamGroupRead:
						if reads.Add(1) == 1 {
							data = oneRecordGroupRead()
						} else {
							time.Sleep(block) // nothing new: the server parks the read
						}
					case OpStreamGroupAck:
						select {
						case acked <- time.Now():
						default:
						}
					}
					if writeFakeResponse(conn, id, data) != nil {
						return
					}
				}
			}(conn)
		}
	}()

	c := NewClient(ln.Addr().String(), WithTimeout(time.Second))
	delivered := make(chan time.Time, 1)
	handler := func(*StreamContext) error {
		delivered <- time.Now()
		// Let the poll loop issue its next blocking read first.
		time.Sleep(100 * time.Millisecond)
		return nil
	}
	sw, err := c.NewStreamWorker(StreamWorkerOptions{
		Stream:                      "s",
		BlockMS:                     uint32(block / time.Millisecond),
		RedeliverPendingOnReconnect: new(bool),
	}, handler)
	if err != nil {
		t.Fatal(err)
	}
	defer sw.Close()
	go sw.Start(context.Background())

	got := <-delivered
	select {
	case at := <-acked:
		if wait := at.Sub(got); wait > block/2 {
			t.Fatalf("ack arrived %v after delivery, behind the blocking read", wait)
		}
	case <-time.After(2 * block):
		t.Fatal("no ack")
	}
}

// opServer answers every request with an empty OK and records the opcodes it
// sees. With stall set, acks and nacks are never answered.
type opServer struct {
	ln       net.Listener
	ops      chan OpCode
	eof      chan struct{}
	accepted atomic.Int32
	stall    bool
}

func newOpServer(t *testing.T, stall bool) *opServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &opServer{ln: ln, ops: make(chan OpCode, 64), eof: make(chan struct{}, 64), stall: stall}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.accepted.Add(1)
			t.Cleanup(func() { conn.Close() })
			go func() {
				defer func() { s.eof <- struct{}{} }()
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
					s.ops <- op
					if s.stall && (op == OpStreamGroupAck || op == OpStreamGroupNack) {
						continue
					}
					if writeFakeResponse(conn, binary.LittleEndian.Uint64(header[8:16]), nil) != nil {
						return
					}
				}
			}()
		}
	}()
	return s
}

func (s *opServer) addr() string { return s.ln.Addr().String() }

// expectOp waits for op, skipping others.
func (s *opServer) expectOp(t *testing.T, op OpCode) {
	t.Helper()
	timeout := time.After(2 * time.Second)
	for {
		select {
		case got := <-s.ops:
			if got == op {
				return
			}
		case <-timeout:
			t.Fatalf("server never saw op %d", op)
		}
	}
}

type captureLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *captureLogger) Printf(format string, v ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, v...))
}

func (l *captureLogger) has(sub string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if strings.Contains(line, sub) {
			return true
		}
	}
	return false
}

// newSplitWorker returns a worker whose poll connection goes to poll and
// whose ack connection goes to ack.
func newSplitWorker(t *testing.T, poll, ack *opServer, logger Logger) *StreamWorker {
	t.Helper()
	c := NewClient(poll.addr(), WithTimeout(5*time.Second))
	sw, err := c.NewStreamWorker(StreamWorkerOptions{Stream: "s", Logger: logger}, func(*StreamContext) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	sw.ackClient.Close()
	sw.ackClient = NewClient(ack.addr(), WithTimeout(5*time.Second))
	if err := sw.ackClient.Connect(); err != nil {
		t.Fatal(err)
	}
	sw.ctx, sw.cancel = context.WithCancel(context.Background())
	t.Cleanup(func() { sw.Close() })
	return sw
}

var testID = StreamID{TimestampMS: 7, Sequence: 1}

func TestStreamWorkerNackUsesAckConnection(t *testing.T) {
	poll, ack := newOpServer(t, false), newOpServer(t, false)
	sw := newSplitWorker(t, poll, ack, &captureLogger{})
	sw.ackWithRetry("s", testID, false)
	ack.expectOp(t, OpStreamGroupNack)
}

// A dropped ack connection is reconnected and the ack retried on it; the
// poll connection is left alone.
func TestStreamWorkerReconnectsAckConnection(t *testing.T) {
	poll, ack := newOpServer(t, false), newOpServer(t, false)
	sw := newSplitWorker(t, poll, ack, &captureLogger{})
	before := poll.accepted.Load()
	sw.ackClient.Close()
	sw.ackWithRetry("s", testID, true)
	ack.expectOp(t, OpStreamGroupAck)
	if n := poll.accepted.Load() - before; n != 0 {
		t.Errorf("poll server accepted %d new connections, want 0", n)
	}
	if !sw.client.IsConnected() {
		t.Error("poll connection was dropped")
	}
}

func TestStreamWorkerCloseClosesAckConnection(t *testing.T) {
	poll, ack := newOpServer(t, false), newOpServer(t, false)
	sw := newSplitWorker(t, poll, ack, &captureLogger{})
	sw.Close()
	select {
	case <-ack.eof:
	case <-time.After(2 * time.Second):
		t.Fatal("ack connection still open after Close")
	}
}

// Close must not wait out a stalled ack, but Stop leaves the ack connection
// up so acks sent while draining still land.
func TestStreamWorkerStopKeepsAcksCloseInterrupts(t *testing.T) {
	poll, ack := newOpServer(t, false), newOpServer(t, true)
	sw := newSplitWorker(t, poll, ack, &captureLogger{})

	sw.Stop()
	done := make(chan struct{})
	go func() {
		defer close(done)
		sw.ackWithRetry("s", testID, true)
	}()
	ack.expectOp(t, OpStreamGroupAck)

	start := time.Now()
	sw.Close()
	if d := time.Since(start); d > time.Second {
		t.Errorf("Close waited %v for a stalled ack", d)
	}
	<-done
}

// A failed ack is logged with the record id so its redelivery is explicable.
func TestStreamWorkerLogsFailedAck(t *testing.T) {
	cases := map[string]func(t *testing.T, sw *StreamWorker){
		"not retried": func(t *testing.T, sw *StreamWorker) {
			sw.ackClient.Close()
			sw.cancel() // stopping: a connection error is not retried
		},
		"out of attempts": func(t *testing.T, sw *StreamWorker) {
			sw.ackClient = NewClient(hangUpServer(t), WithTimeout(time.Second))
			if err := sw.ackClient.Connect(); err != nil {
				t.Fatal(err)
			}
		},
		"reconnect failed": func(t *testing.T, sw *StreamWorker) {
			sw.ackClient = NewClient("127.0.0.1:1", WithTimeout(time.Second))
			sw.ctx, sw.cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			poll, ack := newOpServer(t, false), newOpServer(t, false)
			logger := &captureLogger{}
			sw := newSplitWorker(t, poll, ack, logger)
			setup(t, sw)
			sw.ackWithRetry("s", testID, true)
			if !logger.has("Warning: [s] ack for " + testID.String()) {
				t.Errorf("no warning naming %s in %q", testID, logger.lines)
			}
		})
	}
}

// hangUpServer closes every connection as soon as a request arrives.
func hangUpServer(t *testing.T) string {
	t.Helper()
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
			go func() {
				conn.Read(make([]byte, 1))
				conn.Close()
			}()
		}
	}()
	return ln.Addr().String()
}

// When the ack connection can't be opened, the poll connection opened just
// before it is closed rather than leaked.
func TestNewStreamWorkerClosesPollConnOnAckConnectFailure(t *testing.T) {
	for try := 0; try < 20; try++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		eof := make(chan struct{})
		go func() {
			conn, err := ln.Accept()
			ln.Close() // refuse the ack connection
			if err != nil {
				return
			}
			defer conn.Close()
			io.Copy(io.Discard, conn)
			close(eof)
		}()
		c := NewClient(ln.Addr().String(), WithTimeout(time.Second))
		sw, err := c.NewStreamWorker(StreamWorkerOptions{Stream: "s"}, func(*StreamContext) error { return nil })
		if err == nil {
			// The ack dial beat the listener close; try again.
			sw.Close()
			continue
		}
		select {
		case <-eof:
			return
		case <-time.After(2 * time.Second):
			t.Fatal("poll connection left open after the ack connection failed")
		}
	}
	t.Skip("ack dial never failed")
}
