package flo

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// refusalFrame is a refusal of requestID with status and ran.
func refusalFrame(requestID uint64, status StatusCode, ran Ran) []byte {
	body := refusalBody(ReasonUnclassified, ran, "busy")
	return statusFrame(requestID, status, body)
}

func statusFrame(requestID uint64, status StatusCode, body []byte) []byte {
	f := fakeReply(requestID, "")
	f = append(f[:HeaderSize], body...)
	binary.LittleEndian.PutUint32(f[4:8], uint32(len(body)))
	f[21] = byte(status)
	binary.LittleEndian.PutUint32(f[16:20], computeCRC32C(f[:HeaderSize], body))
	return f
}

// scripted answers the nth request (from 0) with reply(n, id), and records
// every request id it saw.
type scripted struct {
	mu  sync.Mutex
	ids []uint64
}

func (s *scripted) serve(t *testing.T, reply func(n int, id uint64) []byte) string {
	ln := serveAll(t, func(id uint64) []byte {
		s.mu.Lock()
		n := len(s.ids)
		s.ids = append(s.ids, id)
		s.mu.Unlock()
		return reply(n, id)
	})
	return ln.Addr().String()
}

func (s *scripted) seen() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.ids...)
}

var fastRetry = RetryPolicy{MaxAttempts: 4, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}

func connectTo(t *testing.T, addr string, p RetryPolicy) *Client {
	t.Helper()
	c := NewClient(addr, WithTimeout(time.Second), WithRetry(p))
	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestRefusedUnrunIsRetriedAsANewRequest(t *testing.T) {
	for _, status := range []StatusCode{StatusOverloaded, StatusUnavailable} {
		var s scripted
		addr := s.serve(t, func(n int, id uint64) []byte {
			if n == 0 {
				return refusalFrame(id, status, RanNo)
			}
			return fakeReply(id, "v")
		})
		c := connectTo(t, addr, fastRetry)
		got, err := c.KV.Get("k", nil)
		if err != nil {
			t.Fatalf("%s: %v", status, err)
		}
		if string(got.Value) != "v" {
			t.Fatalf("%s: got %q", status, got.Value)
		}
		ids := s.seen()
		if len(ids) != 2 || ids[0] == ids[1] {
			t.Fatalf("%s: request ids %v, want two different ones", status, ids)
		}
	}
}

func TestOnlyRefusalsThatDidNotRunAreRetried(t *testing.T) {
	cases := []struct {
		name  string
		reply func(id uint64) []byte
		want  error
	}{
		{"overloaded, may have run", func(id uint64) []byte { return refusalFrame(id, StatusOverloaded, RanUnknown) }, ErrOverloaded},
		{"unavailable, ran", func(id uint64) []byte { return refusalFrame(id, StatusUnavailable, RanYes) }, ErrUnavailable},
		{"conflict, not run", func(id uint64) []byte { return refusalFrame(id, StatusConflict, RanNo) }, ErrConflict},
		{"overloaded, no ran byte", func(id uint64) []byte { return statusFrame(id, StatusOverloaded, []byte{0, 0}) }, ErrIncompleteResponse},
	}
	for _, tc := range cases {
		var s scripted
		addr := s.serve(t, func(_ int, id uint64) []byte { return tc.reply(id) })
		c := connectTo(t, addr, fastRetry)
		if _, err := c.KV.Get("k", nil); !errors.Is(err, tc.want) {
			t.Fatalf("%s: got %v, want %v", tc.name, err, tc.want)
		}
		if n := len(s.seen()); n != 1 {
			t.Fatalf("%s: sent %d times, want once", tc.name, n)
		}
	}
}

func TestRetriesStopAtMaxAttempts(t *testing.T) {
	for _, attempts := range []int{4, 1} {
		var s scripted
		addr := s.serve(t, func(_ int, id uint64) []byte { return refusalFrame(id, StatusOverloaded, RanNo) })
		p := fastRetry
		p.MaxAttempts = attempts
		c := connectTo(t, addr, p)
		_, err := c.KV.Get("k", nil)
		var se *ServerError
		if !errors.As(err, &se) || se.Status != StatusOverloaded || se.Ran != RanNo {
			t.Fatalf("MaxAttempts %d: got %v, want the overloaded refusal", attempts, err)
		}
		if n := len(s.seen()); n != attempts {
			t.Fatalf("MaxAttempts %d: sent %d times", attempts, n)
		}
	}
}

func TestBackoffStaysUnderItsDoublingCeiling(t *testing.T) {
	b := backoff{initial: 50 * time.Millisecond, max: time.Second}
	ceilings := []time.Duration{50, 100, 200, 400, 800, 1000, 1000}
	for i := 0; i < 200; i++ {
		b.reset()
		for j, c := range ceilings {
			if d := b.next(); d < 0 || d > c*time.Millisecond {
				t.Fatalf("round %d wait %d: %v, want at most %v", i, j, d, c*time.Millisecond)
			}
		}
	}
}

func TestOutOfRangeBackoffsAreNormalized(t *testing.T) {
	for _, p := range []RetryPolicy{
		{MaxAttempts: 4, InitialBackoff: -time.Second, MaxBackoff: -time.Millisecond},
		{MaxAttempts: 4, InitialBackoff: 50 * time.Millisecond, MaxBackoff: 0},
		{MaxAttempts: 4, InitialBackoff: 50 * time.Millisecond, MaxBackoff: 10 * time.Millisecond},
	} {
		c := NewClient("127.0.0.1:1", WithRetry(p))
		b := backoff{initial: c.retry.InitialBackoff, max: c.retry.MaxBackoff}
		ceiling := max(p.InitialBackoff, 0)
		for i := 0; i < 50; i++ {
			if d := b.next(); d < 0 || d > ceiling {
				t.Fatalf("%+v wait %d: %v, want within [0, %v]", p, i, d, ceiling)
			}
		}
	}
}

func TestAHugeMaxBackoffNeverWrapsNegative(t *testing.T) {
	b := backoff{initial: time.Second, max: time.Duration(1<<63 - 1)}
	for i := 0; i < 80; i++ {
		if d := b.next(); d < 0 {
			t.Fatalf("wait %d: %v", i, d)
		}
	}
}

func TestStreamWorkerRetriesARefusedGroupJoin(t *testing.T) {
	var joins atomic.Int64
	var s scripted
	addr := s.serve(t, func(_ int, id uint64) []byte {
		if joins.Add(1) <= 3 {
			return refusalFrame(id, StatusOverloaded, RanNo)
		}
		return fakeReply(id, "")
	})
	sw, err := NewClient(addr, WithRetry(RetryPolicy{MaxAttempts: 1})).NewStreamWorker(StreamWorkerOptions{
		Stream:                      "s",
		RedeliverPendingOnReconnect: new(bool),
	}, func(*StreamContext) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sw.Close() })
	sw.ctx, sw.cancel = context.WithCancel(context.Background())
	sw.pollCtx = sw.ctx
	if err := sw.client.Connect(); err != nil {
		t.Fatal(err)
	}
	if err := sw.joinGroups(); err != nil {
		t.Fatalf("join: %v", err)
	}
	if n := joins.Load(); n != 4 {
		t.Fatalf("joined after %d tries, want 4", n)
	}
}

func TestWorkerConnectionsKeepTheClientsRetryPolicy(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 7, InitialBackoff: 3 * time.Millisecond, MaxBackoff: 9 * time.Millisecond}
	addr := serveAll(t, func(id uint64) []byte { return fakeReply(id, "") }).Addr().String()
	sw, err := NewClient(addr, WithRetry(p)).NewStreamWorker(StreamWorkerOptions{
		Stream:                      "s",
		RedeliverPendingOnReconnect: new(bool),
	}, func(*StreamContext) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sw.Close() })
	aw, err := NewClient(addr, WithRetry(p)).NewActionWorker(ActionWorkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { aw.Close() })
	for name, c := range map[string]*Client{"stream": sw.client, "stream ack": sw.ackClient, "action": aw.client, "action result": aw.resultClient} {
		if c.retry != p {
			t.Errorf("%s connection has %+v, want %+v", name, c.retry, p)
		}
	}
}
