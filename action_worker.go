package flo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
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

	// ActionTimeout defines the maximum duration allowed for an action handler
	ActionTimeout time.Duration

	// BlockMS is the long-poll wait per await, at most MaxBlockMS.
	// 0 means 30000.
	BlockMS uint32

	// AutoTouch touches each run every third of its action's timeout while
	// the handler runs. Off by default: a handler that runs past the
	// action's timeout without calling Touch is taken back and run again.
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
	// timeouts holds each action's registered timeout_ms.
	timeouts map[string]uint32

	// Worker state
	ctx    context.Context
	cancel context.CancelFunc
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
		timeouts:     make(map[string]uint32),
		logger:       opts.Logger,
	}

	return w, nil
}

// RegisterAction registers an action handler, and the action with the
// server with exactly opts (nil sends a 30 s timeout and 3 attempts, the
// server's defaults today). Registering replaces the action's settings, so workers that
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
	w.timeouts[actionName] = reg.timeoutMS()

	w.logger.Printf("Registered action: %s", actionName)
	return nil
}

// MustRegisterAction registers an action handler and panics on error.
func (w *ActionWorker) MustRegisterAction(actionName string, handler ActionHandler, opts *ActionRegisterOptions) {
	if err := w.RegisterAction(actionName, handler, opts); err != nil {
		panic(fmt.Sprintf("failed to register action: %v", err))
	}
}

// Start begins polling and executing actions.
func (w *ActionWorker) Start(ctx context.Context) error {
	w.ctx, w.cancel = context.WithCancel(ctx)
	defer w.cancel()

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
		case <-w.ctx.Done():
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
	if err := workerClient.Register(regOpts); err != nil {
		return fmt.Errorf("failed to register worker: %w", err)
	}
	defer workerClient.Deregister(nil)

	// Result worker client uses the separate connection for Complete/Fail
	resultWorkerClient := w.resultClient.workerClient(w.config.WorkerID)

	// Semaphore for concurrency control
	sem := make(chan struct{}, w.config.Concurrency)

	// Start heartbeat goroutine
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-w.ctx.Done():
				return
			case <-ticker.C:
				currentLoad := uint32(len(sem))
				status, err := workerClient.Heartbeat(currentLoad, nil)
				if err == nil && status == WorkerStatusDraining {
					w.logger.Printf("Worker is draining, stopping...")
					w.cancel()
					return
				}
			}
		}
	}()

	// Main polling loop
	var backoff emptyPollBackoff
	failed := newWorkerBackoff()
	for {
		select {
		case <-w.ctx.Done():
			w.wg.Wait()
			w.logger.Printf("Worker stopped")
			return w.ctx.Err()
		default:
			// Wait for semaphore slot
			sem <- struct{}{}

			// Await task
			polled := time.Now()
			task, err := workerClient.Await(actionNames, &WorkerAwaitOptions{
				BlockMS: &w.config.BlockMS,
			})
			if err != nil {
				<-sem // Release slot
				if IsConnectionError(err) {
					w.logger.Printf("Connection lost, reconnecting...")
					if reconErr := w.client.ReconnectWithContext(w.ctx); reconErr != nil {
						if w.ctx.Err() != nil {
							w.wg.Wait()
							w.logger.Printf("Worker stopped")
							return w.ctx.Err()
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
					d := failed.next()
					w.logger.Printf("Await error: %v, retrying in %v...", err, d)
					sleepCtx(w.ctx, d)
				}
				continue
			}
			failed.reset()

			if task == nil {
				<-sem // Release slot - no task available
				backoff.afterEmpty(w.ctx, polled, w.config.BlockMS)
				continue
			}
			backoff.reset()

			// Execute task in background
			w.wg.Add(1)
			go func(t *TaskAssignment) {
				defer w.wg.Done()
				defer func() { <-sem }()
				w.executeTask(resultWorkerClient, t)
			}(task)
		}
	}
}

// Stop gracefully stops the worker.
func (w *ActionWorker) Stop() {
	w.logger.Printf("Stopping worker...")
	if w.cancel != nil {
		w.cancel()
	}
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
func (w *ActionWorker) executeTask(workerClient *WorkerClient, task *TaskAssignment) {
	fail := func(msg string, retry bool) {
		w.sendResult("fail", task.TaskID, func() error {
			return workerClient.Fail(task.TaskType, task.TaskID, msg, &WorkerFailOptions{Retry: retry})
		})
	}
	complete := func(data []byte, opts *WorkerCompleteOptions) {
		if w.sendResult("complete", task.TaskID, func() error {
			return workerClient.Complete(task.TaskType, task.TaskID, data, opts)
		}) == nil {
			w.logger.Printf("Action completed: %s (task=%s)", task.TaskType, task.TaskID)
		}
	}

	defer func() {
		if r := recover(); r != nil {
			w.logger.Printf("Action handler panicked: %v", r)
			fail(fmt.Sprintf("action panicked: %v", r), false)
		}
	}()

	w.logger.Printf("Executing action: %s (task=%s, attempt=%d)",
		task.TaskType, task.TaskID, task.Attempt)

	// Get handler
	w.mu.RLock()
	handler := w.handlers[task.TaskType]
	w.mu.RUnlock()

	if handler == nil {
		w.logger.Printf("No handler registered for action: %s", task.TaskType)
		fail(fmt.Sprintf("no handler for: %s", task.TaskType), false)
		return
	}

	ctx, cancel := context.WithTimeoutCause(w.ctx, w.config.ActionTimeout, errActionTimeout)
	defer cancel()

	// Create action context
	actx := &ActionContext{
		ctx:                ctx,
		namespace:          w.client.Namespace(),
		actionName:         task.TaskType,
		input:              task.Payload,
		taskID:             task.TaskID,
		attempt:            task.Attempt,
		callerRunID:        task.CallerRunID,
		callerWorkflowName: task.CallerWorkflowName,
		workerClient:       workerClient,
	}

	// Deferred too, so a panicking handler stops touching before its fail.
	stopTouching := w.startTouching(workerClient, task)
	defer stopTouching()
	result, err := handler(actx)
	stopTouching()

	if err != nil {
		ctxErr := errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
		switch {
		case ctxErr && context.Cause(ctx) == errActionTimeout:
			w.logger.Printf("Action timed out: %s (task=%s)", task.TaskType, task.TaskID)
			fail("action timed out", false)
		case ctxErr && w.ctx.Err() != nil:
			// The worker is stopping, not the task failing: let it run
			// again elsewhere.
			w.logger.Printf("Action interrupted by worker stop: %s (task=%s)", task.TaskType, task.TaskID)
			fail("worker stopped", true)
		case IsNonRetryable(err):
			w.logger.Printf("Action failed (non-retryable): %s - %v", task.TaskType, err)
			fail(err.Error(), false)
		default:
			w.logger.Printf("Action failed: %s - %v", task.TaskType, err)
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
			fail(fmt.Sprintf("marshal error: %v", marshalErr), false)
			return
		}
		complete(data, nil)
	}
}

const (
	// minTouchInterval: AutoTouch never touches more often, whatever the timeout.
	minTouchInterval = 100 * time.Millisecond
	// maxTouchExtendMS is the longest lease a touch may ask for; the server refuses more.
	maxTouchExtendMS = 3_600_000
)

// touchPace is how often to touch a run of an action with timeoutMS, and
// the lease each touch asks for: the timeout, at most an hour, touched every
// third of that (at least minTouchInterval apart) so two can fail in a row.
func touchPace(timeoutMS uint32) (interval time.Duration, extendMS uint32) {
	extendMS = min(timeoutMS, maxTouchExtendMS)
	return max(time.Duration(extendMS)*time.Millisecond/3, minTouchInterval), extendMS
}

// startTouching touches task's run every third of its action's timeout
// (of an hour, for longer ones) until the returned stop is called, which waits for a touch in flight, so
// the outcome is sent after it. Each touch asks for this worker's timeout
// (at most an hour), so the lease covers it even if another worker's
// registration shortened the action's. Conflict, not found or bad request
// means the run moved on and touching stops; any other refusal, or a lost
// touch, is tried again next interval, a lost connection reconnected first.
func (w *ActionWorker) startTouching(wc *WorkerClient, task *TaskAssignment) (stop func()) {
	w.mu.RLock()
	timeoutMS := w.timeouts[task.TaskType]
	w.mu.RUnlock()
	if !w.config.AutoTouch || timeoutMS == 0 {
		return func() {}
	}
	interval, extend := touchPace(timeoutMS)
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		timer := time.NewTimer(interval)
		defer timer.Stop()
		for {
			select {
			case <-done:
				return
			case <-timer.C:
			}
			err := wc.Touch(task.TaskType, task.TaskID, &WorkerTouchOptions{ExtendMS: &extend})
			var se *ServerError
			switch {
			case err == nil:
			case errors.As(err, &se) && (se.Status == StatusConflict || se.Status == StatusNotFound || se.Status == StatusBadRequest):
				w.logger.Printf("Touch for task %s was refused, stopping: %v", task.TaskID, err)
				return
			case IsConnectionError(err):
				w.logger.Printf("Touch for task %s lost: %v; reconnecting", task.TaskID, err)
				_ = w.reconnectResult()
			default:
				w.logger.Printf("Touch for task %s failed: %v; trying again", task.TaskID, err)
			}
			timer.Reset(interval)
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

const (
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
	taskID             string
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

// TaskID returns the task ID.
func (a *ActionContext) TaskID() string {
	return a.taskID
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

// Touch extends the lease on this task.
// Use this for long-running tasks to prevent the task from timing out.
// The extendMS parameter specifies how long to extend the lease (default: 30000ms).
func (a *ActionContext) Touch(extendMS uint32) error {
	if a.workerClient == nil {
		return fmt.Errorf("worker client not available")
	}
	return a.workerClient.Touch(a.actionName, a.taskID, &WorkerTouchOptions{
		ExtendMS: &extendMS,
	})
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
