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
	// told is set when the server announced the drain, so no notice is due.
	told atomic.Bool
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
	d.told.Store(false)
	d.mu.Unlock()
	select {
	case <-d.wake:
	default:
	}
}

// start begins a drain, reporting whether a run is under way.
func (d *drainState) start(fromServer bool) (running bool) {
	d.mu.Lock()
	if fromServer {
		d.told.Store(true)
	}
	d.on.Store(true)
	running = d.endPoll != nil
	if running {
		d.endPoll()
	}
	d.mu.Unlock()
	select {
	case d.wake <- struct{}{}:
	default:
	}
	return running
}

func (d *drainState) active() bool { return d.on.Load() }

// finish waits for running handlers (wg), sending the drain notice unless the
// server announced the drain, and keeps the connection up for heartbeats. The notice is tried at most once every
// drainNoticeInterval while handlers run, with a short reconnect between
// tries so a Stop is never held up; an idle worker tries once. It returns
// ctx's error if a Stop ends the drain early, after the handlers return.
func (d *drainState) finish(ctx context.Context, wg *sync.WaitGroup, logger Logger, connected func() bool, notice func() error, reconnect func(context.Context) error) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	sent := d.told.Load()
	ticker := time.NewTicker(drainNoticeInterval)
	defer ticker.Stop()
	for {
		if !sent {
			sent = tryDrainNotice(ctx, logger, notice, reconnect)
		} else if !connected() {
			// Heartbeats share the poll connection, which the drain cut.
			rctx, cancel := context.WithTimeout(ctx, drainNoticeInterval)
			_ = reconnect(rctx)
			cancel()
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

// tryDrainNotice sends the notice once, reporting whether it is settled: sent,
// or refused for a reason a retry won't fix.
func tryDrainNotice(ctx context.Context, logger Logger, notice func() error, reconnect func(context.Context) error) bool {
	err := notice()
	if err == nil {
		return true
	}
	if !IsConnectionError(err) {
		logger.Printf("Warning: drain notice refused: %v", err)
		return true
	}
	rctx, cancel := context.WithTimeout(ctx, drainNoticeInterval)
	defer cancel()
	if reconErr := reconnect(rctx); reconErr == nil {
		if err := notice(); err == nil || !IsConnectionError(err) {
			return true
		}
	}
	return false
}

// heartbeat sends wc's heartbeat every interval until done closes, calling
// draining once when the server answers Draining.
func heartbeat(wc *WorkerClient, interval time.Duration, done <-chan struct{}, load func() uint32, draining func()) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	told := false
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			status, err := wc.Heartbeat(load(), nil)
			if err == nil && status == WorkerStatusDraining && !told {
				told = true
				draining()
			}
		}
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
