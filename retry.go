package flo

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

// RetryPolicy says how often a request the server refused without running
// it is sent again. Only a refusal whose status is overloaded or
// unavailable and whose Ran is RanNo is retried: the server says it never
// ran, so resending is safe for any operation, writes included. Each retry
// is a new request with a new id; the server routes it afresh.
type RetryPolicy struct {
	// MaxAttempts counts the first try; 1 (or less) turns retrying off.
	MaxAttempts int
	// InitialBackoff is the ceiling of the first wait; each later ceiling
	// doubles, up to MaxBackoff. Each wait is uniform in [0, ceiling].
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// DefaultRetryPolicy waits at most about 350 ms in all before returning the
// refusal.
var DefaultRetryPolicy = RetryPolicy{
	MaxAttempts:    4,
	InitialBackoff: 50 * time.Millisecond,
	MaxBackoff:     time.Second,
}

// WithRetry sets how refused-but-not-run requests are retried.
// WithRetry(RetryPolicy{MaxAttempts: 1}) turns retrying off. A negative
// backoff counts as 0, and a MaxBackoff below InitialBackoff as
// InitialBackoff.
func WithRetry(p RetryPolicy) ClientOption {
	return func(c *Client) {
		c.retry = p.normalized()
	}
}

func (p RetryPolicy) normalized() RetryPolicy {
	p.InitialBackoff = max(p.InitialBackoff, 0)
	p.MaxBackoff = max(p.MaxBackoff, p.InitialBackoff)
	return p
}

// refusedUnrun reports whether a response is a refusal the server says it
// didn't run, of a kind worth sending again. A refusal whose body doesn't
// say whether it ran is not retried.
func refusedUnrun(resp *rawResponse) bool {
	if resp.Status != StatusOverloaded && resp.Status != StatusUnavailable {
		return false
	}
	return isRefusedUnrun(newServerError(resp.Status, resp.Data))
}

// isRefusedUnrun reports whether err is a refusal the server says it didn't
// run, of a kind worth trying again later.
func isRefusedUnrun(err error) bool {
	var se *ServerError
	if !errors.As(err, &se) {
		return false
	}
	return (se.Status == StatusOverloaded || se.Status == StatusUnavailable) && se.Ran == RanNo
}

// backoff is a capped, doubling, fully jittered wait.
type backoff struct {
	initial, max time.Duration
	ceiling      time.Duration
}

// next returns the next wait and doubles the ceiling for the one after.
func (b *backoff) next() time.Duration {
	if b.ceiling == 0 {
		b.ceiling = b.initial
	}
	d := time.Duration(rand.Int63n(int64(b.ceiling) + 1))
	b.ceiling = min(b.ceiling*2, b.max)
	return d
}

// reset starts the waits again from the initial ceiling.
func (b *backoff) reset() { b.ceiling = 0 }

// Worker poll loops wait this way after a failed poll, and start again
// from the bottom after a poll that succeeds.
const (
	workerInitialBackoff = 100 * time.Millisecond
	workerMaxBackoff     = 5 * time.Second
)

func newWorkerBackoff() backoff {
	return backoff{initial: workerInitialBackoff, max: workerMaxBackoff}
}

// sleepCtx waits d, or until ctx is done.
func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
