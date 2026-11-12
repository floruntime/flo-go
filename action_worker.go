package flo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Logger is a simple logging interface.
type Logger interface {
	Printf(format string, v ...interface{})
}

// ActionWorkerOptions holds action worker configuration.
// Endpoint and namespace are inherited from the parent Client.
type ActionWorkerOptions struct {
	// WorkerID uniquely identifies this worker instance (optional, auto-generated if empty)
	WorkerID string

	// MachineID groups workers on the same machine (optional, defaults to hostname)
	MachineID string

	// Concurrency is the maximum number of concurrent actions (default: 10)
	Concurrency int

	// ActionTimeout is the longest a handler may run: its context is then
	// cancelled and the run failed with retry, like a returned error.
	ActionTimeout time.Duration

	// BlockMS is the long-poll wait per await, at most MaxBlockMS.
	// 0 means 30000.
	BlockMS uint32

	// AutoTouch touches each run every sixth of its lease while the handler
	// runs; a touch renews the lease once half of it is gone. Off by
	// default: a handler that runs past the lease without calling Touch is
	// taken back and run again. However often it is touched, the server
	// holds a delivery at most MaxHoldMS (12 h) from when it was given.
	AutoTouch bool

	// Logger for worker output (optional, defaults to log.Printf)
	Logger Logger
}

// ActionWorker manages action execution using the Flo wire protocol.
type ActionWorker struct {
	config ActionWorkerOptions

	// Protocol client (dedicated connection for polling/Await)
	client *Client

	// Separate connection for sending Complete/Fail results.
	// This avoids mutex contention with the blocking Await call
	// on the polling connection, which holds the lock for up to BlockMS.
	resultClient *Client
	// resultMu serializes reconnects of resultClient, so handlers that hit
	// the same dropped connection reconnect it once.
	resultMu sync.Mutex
	// closeCtx ends when Close is called.
	closeCtx    context.Context
	closeCancel context.CancelFunc
	// resultCtx bounds result reconnects. It ends stopResultWait after the
	// worker starts stopping, or at Close, so Start isn't held up for long
	// by a result that can't be sent.
	resultCtx context.Context

	// Action handlers
	mu       sync.RWMutex
	handlers map[string]ActionHandler

	// Worker state. ctx ends on Stop or when Start's context does; handler
	// contexts derive from it, so a drain, which doesn't end it, lets
	// handlers finish. runMu guards cancel, which Stop reads while Start
	// sets it.
	runMu  sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
	drain  drainState
	wg     sync.WaitGroup
	logger Logger
}

// NewActionWorker creates a new Flo action worker from an existing client.
// The worker creates a dedicated connection using the client's endpoint and namespace,
// so it does not interfere with other operations on the parent client.
func (c *Client) NewActionWorker(opts ActionWorkerOptions) (*ActionWorker, error) {
	// Apply defaults
	if opts.Concurrency == 0 {
		opts.Concurrency = 10
	}
	if opts.ActionTimeout == 0 {
		opts.ActionTimeout = 5 * time.Minute
	}
	blockMS, err := workerBlockMS(opts.BlockMS)
	if err != nil {
		return nil, err
	}
	opts.BlockMS = blockMS
	if opts.Logger == nil {
		opts.Logger = &stdLogger{}
	}

	// Generate WorkerID if not provided
	if opts.WorkerID == "" {
		hostname, _ := os.Hostname()
		if hostname == "" {
			hostname = "unknown"
		}
		opts.WorkerID = fmt.Sprintf("%s-%s", hostname, randomID(8))
	}

	// Default MachineID to hostname
	if opts.MachineID == "" {
		hostname, _ := os.Hostname()
		if hostname != "" {
			opts.MachineID = hostname
		}
	}

	// Create a dedicated connection for the worker using the parent client's settings
	workerClient := NewClient(c.endpoint,
		WithNamespace(c.namespace),
		WithTimeout(opts.ActionTimeout),
		WithDebug(c.debug),
		WithRetry(c.retry),
	)

	if err := workerClient.Connect(); err != nil {
		return nil, fmt.Errorf("failed to connect worker: %w", err)
	}

	// Create a second connection for sending Complete/Fail results.
	// The polling connection holds its mutex during blocking Await calls
	// (up to BlockMS), so a separate connection prevents contention. Its
	// timeout is the parent's: a result is a short request, and a stalled
	// one must not hold up Stop for an ActionTimeout.
	resultClient := NewClient(c.endpoint,
		WithNamespace(c.namespace),
		WithTimeout(c.timeout),
		WithDebug(c.debug),
		WithRetry(c.retry),
	)

	if err := resultClient.Connect(); err != nil {
		workerClient.Close()
		return nil, fmt.Errorf("failed to connect result client: %w", err)
	}

	closeCtx, closeCancel := context.WithCancel(context.Background())
	w := &ActionWorker{
		closeCtx:     closeCtx,
		closeCancel:  closeCancel,
		resultCtx:    closeCtx,
		config:       opts,
		client:       workerClient,
		resultClient: resultClient,
		handlers:     make(map[string]ActionHandler),
		drain:        newDrainState(),
		logger:       opts.Logger,
	}

	return w, nil
}

// RegisterAction registers an action handler, and the action with the
// server with exactly opts (nil takes the server's defaults: a 300 s lease
// and 3 attempts). Registering replaces the action's settings, so workers that
// register one action with different options leave it with whichever
// registered last. opts.Namespace is ignored: the action goes in the
// worker's.
func (w *ActionWorker) RegisterAction(actionName string, handler ActionHandler, opts *ActionRegisterOptions) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, exists := w.handlers[actionName]; exists {
		return fmt.Errorf("action '%s' is already registered", actionName)
	}

	var reg ActionRegisterOptions
	if opts != nil {
		reg = *opts
	}
	reg.Namespace = ""
	if err := w.client.Action.Register(actionName, ActionTypeUser, &reg); err != nil {
		return fmt.Errorf("failed to register action '%s': %w", actionName, err)
	}

	w.handlers[actionName] = handler

	w.logger.Printf("Registered action: %s", actionName)
	return nil
}

// MustRegisterAction registers an action handler and panics on error.
func (w *ActionWorker) MustRegisterAction(actionName string, handler ActionHandler, opts *ActionRegisterOptions) {
	if err := w.RegisterAction(actionName, handler, opts); err != nil {
		panic(fmt.Sprintf("failed to register action: %v", err))
	}
}

// Start begins polling and executing actions. Its registration waits out a
// halted shard, and an overloaded or unavailable refusal that didn't run,
// with a backoff; any other refusal fails Start with that error. Start fails
// with the limit error; a limit hit while running is logged and backed off.
func (w *ActionWorker) Start(ctx context.Context) error {
	w.runMu.Lock()
	w.ctx, w.cancel = context.WithCancel(ctx)
	runCtx := w.ctx // goroutines that may outlive this run read this, not w.ctx
	pollCtx := w.drain.begin(runCtx)
	w.runMu.Unlock()
	defer w.cancel()
	defer w.drain.end()

	// Once stopping, results may still be sent for stopResultWait; then the
	// result connection is cut so Start returns promptly.
	resultCtx, resultCancel := context.WithCancel(w.closeCtx)
	defer resultCancel()
	w.resultCtx = resultCtx
	started := make(chan struct{})
	defer close(started)
	go func() {
		select {
		case <-started:
			return
		case <-runCtx.Done():
		}
		select {
		case <-started:
		case <-time.After(stopResultWait):
			resultCancel()
			w.resultClient.Interrupt()
		}
	}()

	w.logger.Printf("Starting Flo worker (id=%s, namespace=%s, concurrency=%d)",
		w.config.WorkerID, w.client.Namespace(), w.config.Concurrency)

	// Check that we have handlers
	w.mu.RLock()
	handlerCount := len(w.handlers)
	actionNames := make([]string, 0, handlerCount)
	for name := range w.handlers {
		actionNames = append(actionNames, name)
	}
	w.mu.RUnlock()

	if handlerCount == 0 {
		return fmt.Errorf("no action handlers registered")
	}

	// Build process entries from registered actions
	processes := make([]ProcessEntry, 0, handlerCount)
	for _, name := range actionNames {
		processes = append(processes, ProcessEntry{Name: name, Kind: ProcessKindAction})
	}

	// Build registration options (reused on reconnect)
	regOpts := &WorkerRegisterOptions{
		WorkerType:     WorkerTypeAction,
		MaxConcurrency: uint32(w.config.Concurrency),
		Processes:      processes,
		MachineID:      w.config.MachineID,
	}

	// Register worker in the worker registry
	workerClient := w.client.workerClient(w.config.WorkerID)
	// Before registering: a registration whose answer was lost still counts.
	defer sendOnPoll(w.client, w.closeCtx, func() error { return workerClient.Deregister(nil) })
	halted := newHaltedBackoff()
	refused := newWorkerBackoff()
	for {
		err := workerClient.Register(regOpts)
		if err == nil {
			break
		}
		if waitAtStart(pollCtx, w.logger, err, &halted, &refused) && pollCtx.Err() == nil {
			continue
		}
		if w.drain.active() {
			return nil // drained before it took any work
		}
		if pollCtx.Err() != nil {
			return w.ctx.Err()
		}
		return fmt.Errorf("failed to register worker: %w", err)
	}
	halted.reset()

	// Result worker client uses the separate connection for Complete/Fail
	resultWorkerClient := w.resultClient.workerClient(w.config.WorkerID)

	// Semaphore for concurrency control
	sem := make(chan struct{}, w.config.Concurrency)

	// Handlers running: sem also holds a slot for the outstanding poll.
	var running atomic.Int32

	// Heartbeats go on until Start returns, so a draining worker's handlers
	// still count as alive.
	beats := &beater{
		wc:     workerClient,
		reg:    regOpts,
		drain:  &w.drain,
		logger: w.logger,
		load:   func() uint32 { return uint32(running.Load()) },
		draining: func() {
			w.logger.Printf("Worker is draining, finishing running actions...")
			if w.drain.start() {
				w.client.Interrupt()
			}
		},
	}
	defer beats.start(heartbeatInterval)()

	// Main polling loop. Each poll carries a request key, kept until a poll
	// is answered: a poll whose answer was lost, or that a new leader may
	// have served, is sent again with it and gets the delivery it got.
	var backoff emptyPollBackoff
	failed := newWorkerBackoff()
	var limited limitNotice
	requestKey := randomID(32)
	for {
		select {
		case <-w.ctx.Done():
			w.wg.Wait()
			w.logger.Printf("Worker stopped")
			return w.ctx.Err()
		default:
			if w.drain.active() {
				return w.finishDrain(workerClient, regOpts)
			}
			// Wait for a free slot, or a drain or stop.
			select {
			case sem <- struct{}{}:
			case <-w.drain.wake:
				continue
			case <-w.ctx.Done():
				continue
			}

			// Await task
			polled := time.Now()
			task, err := workerClient.Await(actionNames, &WorkerAwaitOptions{
				BlockMS:    &w.config.BlockMS,
				RequestKey: requestKey,
			})
			if err != nil {
				<-sem // Release slot
				if w.drain.active() {
					// A drain cut the poll; finishDrain reconnects as needed.
					continue
				}
				if IsConnectionError(err) {
					w.logger.Printf("Connection lost, reconnecting...")
					if reconErr := w.client.ReconnectWithContext(pollCtx); reconErr != nil {
						if pollCtx.Err() != nil {
							continue // a stop or drain; the loop top handles it
						}
						w.logger.Printf("Reconnect failed: %v, retrying...", reconErr)
						continue
					}
					// Also reconnect result client
					if reconErr := w.reconnectResult(); reconErr != nil {
						w.logger.Printf("Warning: failed to reconnect result client: %v", reconErr)
					}
					// Re-register worker after reconnect
					if regErr := workerClient.Register(regOpts); regErr != nil {
						w.logger.Printf("Warning: failed to re-register worker: %v", regErr)
					}
					w.logger.Printf("Reconnected, resuming work")
				} else {
					var se *ServerError
					if errors.As(err, &se) && se.Reason == ReasonWorkerDraining {
						w.logger.Printf("Server refused work: worker is draining, finishing running actions...")
						w.drain.start()
						continue
					}
					if errors.As(err, &se) && se.Reason == ReasonWorkerUnhealthy {
						// The server missed our heartbeats; one now lets the
						// next poll take work.
						beats.beat(runCtx.Done())
					}
					if waitIfHalted(pollCtx, w.logger, "await", err, &halted) || limited.wait(pollCtx, w.logger, "await", err) {
						continue
					}
					if IsOutcomePending(err) {
						// Polling again would resend it; the worker stops.
						w.logger.Printf("Error: await committed but not applied, stopping: %v", err)
						w.cancel()
						w.wg.Wait()
						return err
					}
					d := failed.next()
					w.logger.Printf("Await error: %v, retrying in %v...", err, d)
					sleepCtx(pollCtx, d)
				}
				continue
			}
			failed.reset()
			halted.reset()
			limited.reset()
			requestKey = randomID(32)

			if task == nil {
				<-sem // Release slot - no task available
				backoff.afterEmpty(pollCtx, polled, w.config.BlockMS)
				continue
			}
			backoff.reset()
			arrived := time.Now()

			// Execute task in background
			w.wg.Add(1)
			go func(t *Task) {
				defer w.wg.Done()
				defer func() { <-sem }()
				running.Add(1)
				defer running.Add(-1)
				w.executeTask(resultWorkerClient, t, arrived)
			}(task)
		}
	}
}

// Drain stops taking new tasks and lets running ones finish, then Start
// returns nil. The server is told, so it assigns the worker nothing more;
// the notice is retried while the worker is disconnected. A held Await
// doesn't hold it up: the poll connection is closed. A Stop during a drain
// still cancels the running handlers.
func (w *ActionWorker) Drain() {
	w.logger.Printf("Draining worker...")
	// Unblock an in-flight Await, as Stop does; with no run, the next Start
	// still needs the connection.
	if w.drain.start() && w.client != nil {
		w.client.Interrupt()
	}
}

// finishDrain waits for the running handlers, keeping the server told of the
// drain meanwhile.
func (w *ActionWorker) finishDrain(wc *WorkerClient, regOpts *WorkerRegisterOptions) error {
	reconnect := func(ctx context.Context) error {
		if err := w.client.ReconnectWithContext(ctx); err != nil {
			return err
		}
		if err := wc.Register(regOpts); err != nil {
			w.logger.Printf("Warning: failed to re-register worker: %v", err)
		}
		return nil
	}
	if err := w.drain.finish(w.ctx, &w.wg, w.logger, w.client.IsConnected, func() error { return wc.Drain(nil) }, reconnect); err != nil {
		return err
	}
	w.logger.Printf("Worker drained")
	return nil
}

// Stop stops the worker: it takes no more tasks and cancels the context of
// every running handler. A handler that returns the context's error hands
// its run back with no failure counted, to run again elsewhere; one that
// returns anything else is reported as usual. Results may still be sent for
// two seconds after Stop, then the result connection is cut, and a run
// whose result was not sent goes back when its lease runs out. Start
// returns once every handler has returned.
func (w *ActionWorker) Stop() {
	w.logger.Printf("Stopping worker...")
	w.runMu.Lock()
	if w.cancel != nil {
		w.cancel()
	}
	w.runMu.Unlock()
	// Interrupt the poll connection to unblock an in-flight Await at once.
	// Interrupt bypasses the mutex so it won't block behind a 30s Await. The
	// result connection stays up so handlers still running can report.
	if w.client != nil {
		w.client.Interrupt()
	}
}

// Close closes the connections.
func (w *ActionWorker) Close() error {
	w.Stop()
	w.closeCancel()
	if w.resultClient != nil {
		// Don't wait out a stalled result send.
		w.resultClient.Interrupt()
		w.resultClient.Close()
	}
	if w.client != nil {
		return w.client.Close()
	}
	return nil
}

// errActionTimeout is the cause of a handler context that hit ActionTimeout,
// telling it apart from a worker that is stopping.
var errActionTimeout = errors.New("flo: action timed out")

// executeTask runs a task's handler and reports its outcome. The handler's
// outcome alone decides complete or fail: a complete that can't be sent is
// never turned into a fail.
func (w *ActionWorker) executeTask(workerClient *WorkerClient, task *Task, arrived time.Time) {
	taskID := task.TaskID()
	sendFail := func(msg string, opts *WorkerFailOptions) {
		w.sendResult("fail", taskID, func() error {
			return workerClient.Fail(task.Delivery, msg, opts)
		})
	}
	fail := func(msg string, retry bool) { sendFail(msg, &WorkerFailOptions{Retry: retry}) }
	complete := func(data []byte, opts *WorkerCompleteOptions) {
		if w.sendResult("complete", taskID, func() error {
			return workerClient.Complete(task.Delivery, data, opts)
		}) == nil {
			w.logger.Printf("Action completed: %s (task=%s)", task.Action, taskID)
		}
	}

	defer func() {
		if r := recover(); r != nil {
			// Like a returned error: retried while attempts are left, so
			// the action's max attempts bounds a handler that always panics.
			w.logger.Printf("Action handler panicked: %v", r)
			fail(fmt.Sprintf("action panicked: %v", r), true)
		}
	}()

	w.logger.Printf("Executing action: %s (task=%s, attempt=%d)",
		task.Action, taskID, task.Attempt)

	// Get handler
	w.mu.RLock()
	handler := w.handlers[task.Action]
	w.mu.RUnlock()

	if handler == nil {
		w.logger.Printf("No handler registered for action: %s", task.Action)
		fail(fmt.Sprintf("no handler for: %s", task.Action), true)
		return
	}

	ctx, cancel := context.WithTimeoutCause(w.ctx, w.config.ActionTimeout, errActionTimeout)
	defer cancel()

	// Create action context
	actx := &ActionContext{
		ctx:                ctx,
		namespace:          w.client.Namespace(),
		actionName:         task.Action,
		input:              task.Input,
		delivery:           task.Delivery,
		attempt:            task.Attempt,
		callerRunID:        task.CallerRunID,
		callerWorkflowName: task.CallerWorkflowName,
		workerClient:       workerClient,
	}

	// Deferred too, so a panicking handler stops touching before its fail.
	stopTouching := w.startTouching(workerClient, task, arrived)
	defer stopTouching()
	result, err := handler(actx)
	stopTouching()

	if err != nil {
		ctxErr := errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
		switch {
		case ctxErr && context.Cause(ctx) == errActionTimeout:
			w.logger.Printf("Action timed out: %s (task=%s)", task.Action, taskID)
			fail("action timed out", true)
		case ctxErr && w.ctx.Err() != nil:
			// The worker is stopping, not the task failing: hand it back
			// with no failure counted, to run again elsewhere.
			w.logger.Printf("Action interrupted by worker stop: %s (task=%s)", task.Action, taskID)
			sendFail("worker stopped", &WorkerFailOptions{Retry: true, Abandoned: true})
		case IsNonRetryable(err):
			w.logger.Printf("Action failed (non-retryable): %s - %v", task.Action, err)
			fail(err.Error(), false)
		default:
			w.logger.Printf("Action failed: %s - %v", task.Action, err)
			fail(err.Error(), true)
		}
		return
	}

	// Success — dispatch based on result type
	switch v := result.(type) {
	case *ActionResult:
		complete(v.Data, &WorkerCompleteOptions{Outcome: v.Outcome})
	case []byte:
		complete(v, nil)
	default:
		// Auto-marshal to JSON
		data, marshalErr := json.Marshal(v)
		if marshalErr != nil {
			w.logger.Printf("Failed to marshal result: %v", marshalErr)
			fail(fmt.Sprintf("marshal error: %v", marshalErr), true)
			return
		}
		complete(data, nil)
	}
}

// minTouchInterval: AutoTouch never touches more often, whatever the lease.
const minTouchInterval = 100 * time.Millisecond

// touchInterval is how often AutoTouch touches a run with a lease of
// leaseMS. A touch renews the lease only once half of it is gone, so every
// sixth of it leaves two more tries before it runs out if one fails.
func touchInterval(leaseMS uint64) time.Duration {
	return max(time.Duration(leaseMS)*time.Millisecond/6, minTouchInterval)
}

// startTouching touches task's run every touchInterval from when it
// arrived, until the returned stop is called, which waits for a touch in
// flight, so the outcome is sent after it. The nth touch is due at arrived
// plus n intervals, so a slow touch doesn't push the later ones back; a
// slot that passed while one was in flight is skipped. Conflict (a stale delivery), not found or bad request means the
// run moved on and touching stops; any other refusal, or a lost touch, is
// tried again next interval, a lost connection reconnected first.
func (w *ActionWorker) startTouching(wc *WorkerClient, task *Task, arrived time.Time) (stop func()) {
	if !w.config.AutoTouch || task.LeaseMS == 0 {
		return func() {}
	}
	interval := touchInterval(task.LeaseMS)
	taskID := task.TaskID()
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		next := arrived.Add(interval)
		timer := time.NewTimer(time.Until(next))
		defer timer.Stop()
		for {
			select {
			case <-done:
				return
			case <-timer.C:
			}
			_, err := wc.Touch(task.Delivery, nil)
			var se *ServerError
			switch {
			case err == nil:
			case errors.As(err, &se) && (se.Status == StatusConflict || se.Status == StatusNotFound || se.Status == StatusBadRequest):
				w.logger.Printf("Touch for task %s was refused, stopping: %v", taskID, err)
				return
			case IsConnectionError(err):
				w.logger.Printf("Touch for task %s lost: %v; reconnecting", taskID, err)
				_ = w.reconnectResult()
			default:
				w.logger.Printf("Touch for task %s failed: %v; trying again", taskID, err)
			}
			for now := time.Now(); !next.After(now); {
				next = next.Add(interval)
			}
			timer.Reset(time.Until(next))
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(done)
			<-finished
		})
	}
}

// maxResultSends is how many times a result is sent before it is dropped.
const maxResultSends = 3

// Vars so tests can shorten them.
var (
	// resultReconnectWait bounds one reconnect of a result connection.
	resultReconnectWait = 30 * time.Second
	// stopResultWait is how long after a worker starts stopping its
	// results may still be sent.
	stopResultWait = 2 * time.Second
)

// sendResult sends a complete or fail. On a connection error, a timeout or
// an unreadable answer it reconnects and sends the same result again, up to
// maxResultSends in all: the worker can't tell whether a lost one arrived,
// and a task reported twice is safer than one never reported. A refusal is not resent. A
// result still unsent is dropped with a log line, and the server hands the
// task out again when its lease runs out.
func (w *ActionWorker) sendResult(op, taskID string, send func() error) error {
	var err error
	for n := 1; ; n++ {
		if err = send(); err == nil {
			return nil
		}
		if !resultLost(err) || n == maxResultSends {
			break
		}
		w.logger.Printf("Connection lost sending %s for task %s (send %d/%d), reconnecting...", op, taskID, n, maxResultSends)
		if rerr := w.reconnectResult(); rerr != nil {
			err = fmt.Errorf("reconnect: %w", rerr)
			break
		}
	}
	stopping := ""
	if w.ctx.Err() != nil {
		stopping = " while the worker stops"
	}
	w.logger.Printf("Error: dropped %s for task %s%s: %v", op, taskID, stopping, err)
	return err
}

// reconnectResult reconnects the result connection unless a concurrent
// caller already has.
func (w *ActionWorker) reconnectResult() error {
	w.resultMu.Lock()
	defer w.resultMu.Unlock()
	if w.resultClient.IsConnected() {
		return nil
	}
	ctx, cancel := context.WithTimeout(w.resultCtx, resultReconnectWait)
	defer cancel()
	return w.resultClient.ReconnectWithContext(ctx)
}

// ActionContext provides context to action handlers.
type ActionContext struct {
	ctx                context.Context
	namespace          string
	actionName         string
	input              []byte
	delivery           Delivery
	attempt            uint32
	callerRunID        string
	callerWorkflowName string
	workerClient       *WorkerClient
}

// Ctx returns the context for this action execution.
func (a *ActionContext) Ctx() context.Context {
	return a.ctx
}

// Context is an alias for Ctx.
func (a *ActionContext) Context() context.Context {
	return a.ctx
}

// Namespace returns the namespace.
func (a *ActionContext) Namespace() string {
	return a.namespace
}

// ActionName returns the action name.
func (a *ActionContext) ActionName() string {
	return a.actionName
}

// TaskID returns the task's id, run:deliveries:term:index:action.
func (a *ActionContext) TaskID() string {
	return a.delivery.TaskID()
}

// RunID returns the run's id.
func (a *ActionContext) RunID() string {
	return a.delivery.RunID
}

// Delivery returns the delivery the worker reports on.
func (a *ActionContext) Delivery() Delivery {
	return a.delivery
}

// Attempt returns the attempt number (starts at 1).
func (a *ActionContext) Attempt() uint32 {
	return a.attempt
}

// CallerRunID returns the run ID of the workflow that invoked this action,
// or an empty string if the action was invoked directly.
func (a *ActionContext) CallerRunID() string {
	return a.callerRunID
}

// CallerWorkflowName returns the name of the workflow that invoked this action,
// or an empty string if the action was invoked directly.
func (a *ActionContext) CallerWorkflowName() string {
	return a.callerWorkflowName
}

// IsCalledByWorkflow returns true if this action was invoked by a workflow.
func (a *ActionContext) IsCalledByWorkflow() bool {
	return a.callerRunID != ""
}

// Input returns the raw input bytes.
func (a *ActionContext) Input() []byte {
	return a.input
}

// Into unmarshals the action input into the provided value.
func (a *ActionContext) Into(v interface{}) error {
	if len(a.input) == 0 {
		return fmt.Errorf("no input data")
	}
	return json.Unmarshal(a.input, v)
}

// Bytes marshals the provided value to JSON bytes.
func (a *ActionContext) Bytes(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// Touch keeps this task's lease, so a long handler isn't taken back, and
// returns when the lease runs out, in Unix milliseconds. While more than
// half of the lease is left it changes nothing; after that it renews the
// lease in full, though never past MaxHoldMS (12 h) from when the task was
// given. A run that has moved on is a *StaleReportError.
func (a *ActionContext) Touch() (int64, error) {
	if a.workerClient == nil {
		return 0, fmt.Errorf("worker client not available")
	}
	return a.workerClient.Touch(a.delivery, nil)
}

// Result creates an ActionResult with a named outcome for workflow routing.
// The value is automatically JSON-marshaled to bytes.
func (a *ActionContext) Result(outcome string, value interface{}) (*ActionResult, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal result: %w", err)
	}
	return &ActionResult{Outcome: outcome, Data: data}, nil
}

// ActionHandler is a function that executes action logic.
// Return types:
//   - []byte: raw result bytes
//   - *ActionResult: named outcome for workflow routing
//   - any other type: auto-marshaled to JSON bytes
type ActionHandler func(actx *ActionContext) (result interface{}, err error)
