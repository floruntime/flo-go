package flo

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// heartbeatInterval is how often a worker heartbeats; tests shorten it.
var heartbeatInterval = 30 * time.Second

// drainNoticeInterval is how often an unsent drain notice is tried again.
var drainNoticeInterval = time.Second

// drainState is a worker's drain: set by Drain or a Draining heartbeat, it
// stops new work while running handlers finish. It is cleared when Start
// returns, not when it begins, so a Drain that lands before or during
// startup still ends the run.
type drainState struct {
	mu sync.Mutex
	on atomic.Bool
	// endPoll ends the current run's poll context.
	endPoll context.CancelFunc
	// wake unblocks a poll loop waiting for a free slot.
	wake chan struct{}
}

func newDrainState() drainState { return drainState{wake: make(chan struct{}, 1)} }

// begin returns the run's poll context: it ends on a drain as well as with
// parent, so a reconnect or backoff stops at once when a drain starts.
func (d *drainState) begin(parent context.Context) context.Context {
	ctx, cancel := context.WithCancel(parent)
	d.mu.Lock()
	d.endPoll = cancel
	d.mu.Unlock()
	if d.on.Load() {
		cancel()
	}
	return ctx
}

// end clears the drain when a run ends.
func (d *drainState) end() {
	d.mu.Lock()
	if d.endPoll != nil {
		d.endPoll()
		d.endPoll = nil
	}
	d.on.Store(false)
	d.mu.Unlock()
	select {
	case <-d.wake:
	default:
	}
}

// start begins a drain, reporting whether it began one in a run under way:
// the poll then needs interrupting.
func (d *drainState) start() (began bool) {
	d.mu.Lock()
	began = !d.on.Swap(true) && d.endPoll != nil
	if d.endPoll != nil {
		d.endPoll()
	}
	d.mu.Unlock()
	select {
	case d.wake <- struct{}{}:
	default:
	}
	return began
}

func (d *drainState) active() bool { return d.on.Load() }

// finish waits for running handlers (wg), keeping the server told of the
// drain: the notice goes at the start, even for a drain the server announced
// (a reconnect may have registered the worker again since), and again after
// any reconnect, whose registration marks the worker active. A
// lost connection is reconnected, briefly, once every drainNoticeInterval, so
// heartbeats go on and a Stop is never held up; an idle worker tries once.
// It returns ctx's error if a Stop ends the drain early, after the handlers
// return.
func (d *drainState) finish(ctx context.Context, wg *sync.WaitGroup, logger Logger, connected func() bool, notice func() error, reconnect func(context.Context) error) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	sent := false
	ticker := time.NewTicker(drainNoticeInterval)
	defer ticker.Stop()
	for {
		if !connected() {
			rctx, cancel := context.WithTimeout(ctx, drainNoticeInterval)
			if reconnect(rctx) == nil {
				sent = false
			}
			cancel()
		}
		if !sent && connected() {
			sent = sendDrainNotice(logger, notice)
		}
		select {
		case <-done:
			return ctx.Err()
		case <-ctx.Done():
			<-done
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// sendDrainNotice sends the notice, reporting whether it is settled: sent, or
// refused for a reason a retry won't fix.
func sendDrainNotice(logger Logger, notice func() error) bool {
	err := notice()
	if err == nil {
		return true
	}
	if IsConnectionError(err) {
		return false
	}
	logger.Printf("Warning: drain notice refused: %v", err)
	return true
}

// beater sends a worker's heartbeats. A heartbeat answered not_found means
// the server removed the worker for its silence: it registers again with
// the same options, and, if draining, tells the server again, since a
// registration marks it active.
type beater struct {
	wc     *WorkerClient
	reg    *WorkerRegisterOptions
	drain  *drainState
	logger Logger
	// load is the number of handlers running.
	load func() uint32
	// draining is called once, when the server first answers Draining.
	draining func()
	told     atomic.Bool
}

// beat sends one heartbeat, unless done closed while it was out: a drain
// then would reach the next run, and a registration outlive this one.
func (b *beater) beat(done <-chan struct{}) {
	status, err := b.wc.Heartbeat(b.load(), nil)
	select {
	case <-done:
		return
	default:
	}
	switch {
	case IsNotFound(err):
		b.logger.Printf("Worker not found at heartbeat, registering again")
		// worker.register isn't logged on the server, so it can't be
		// refused outcome_pending: resending it is always safe.
		if err := b.wc.Register(b.reg); err != nil {
			b.logger.Printf("Warning: failed to re-register worker: %v", err)
			return
		}
		if b.drain.active() {
			sendDrainNotice(b.logger, func() error { return b.wc.Drain(nil) })
		}
	case err == nil && status == WorkerStatusDraining && !b.told.Swap(true):
		b.draining()
	}
}

// start beats every interval until the returned stop, which waits for a
// beat in flight.
func (b *beater) start(interval time.Duration) (stop func()) {
	done := make(chan struct{})
	var running sync.WaitGroup
	running.Add(1)
	go func() {
		defer running.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				b.beat(done)
			}
		}
	}()
	return func() {
		close(done)
		running.Wait()
	}
}

// sendOnPoll sends a closing request (deregister, group leave) on a poll
// connection that a Stop or drain may have cut, reconnecting once.
func sendOnPoll(c *Client, closeCtx context.Context, send func() error) {
	if err := send(); err == nil || !IsConnectionError(err) {
		return
	}
	ctx, cancel := context.WithTimeout(closeCtx, drainNoticeInterval)
	defer cancel()
	if c.ReconnectWithContext(ctx) == nil {
		_ = send()
	}
}
