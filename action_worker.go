package flo

import (
	"context"
	"encoding/json"
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

	// BlockMS is the timeout for blocking dequeue (default: 30000)
	BlockMS uint32

	// Logger for worker output (optional, defaults to log.Printf)
	Logger Logger
}

// ActionWorker manages action execution using the Flo wire protocol.
type ActionWorker struct {
	config ActionWorkerOptions

	// Protocol client (dedicated connection for the worker)
	client *Client

	// Action handlers
	mu       sync.RWMutex
	handlers map[string]ActionHandler

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
	if opts.BlockMS == 0 {
		opts.BlockMS = 30000
	}
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
	)

	if err := workerClient.Connect(); err != nil {
		return nil, fmt.Errorf("failed to connect worker: %w", err)
	}

	w := &ActionWorker{
		config:   opts,
		client:   workerClient,
		handlers: make(map[string]ActionHandler),
		logger:   opts.Logger,
	}

	return w, nil
}

// RegisterAction registers an action handler.
func (w *ActionWorker) RegisterAction(actionName string, handler ActionHandler) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, exists := w.handlers[actionName]; exists {
		return fmt.Errorf("action '%s' is already registered", actionName)
	}

	// Register the action with the server
	if err := w.client.Action.Register(actionName, ActionTypeUser, nil); err != nil {
		return fmt.Errorf("failed to register action '%s': %w", actionName, err)
	}

	w.handlers[actionName] = handler

	w.logger.Printf("Registered action: %s", actionName)
	return nil
}

// MustRegisterAction registers an action handler and panics on error.
func (w *ActionWorker) MustRegisterAction(actionName string, handler ActionHandler) {
	if err := w.RegisterAction(actionName, handler); err != nil {
		panic(fmt.Sprintf("failed to register action: %v", err))
	}
}

// Start begins polling and executing actions.
func (w *ActionWorker) Start(ctx context.Context) error {
	w.ctx, w.cancel = context.WithCancel(ctx)
	defer w.cancel()

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

	// Register worker in the worker registry
	workerClient := w.client.workerClient(w.config.WorkerID)
	if err := workerClient.Register(actionNames, &WorkerRegisterOptions{
		WorkerType:     WorkerTypeAction,
		MaxConcurrency: uint32(w.config.Concurrency),
		Processes:      processes,
		MachineID:      w.config.MachineID,
	}); err != nil {
		return fmt.Errorf("failed to register worker: %w", err)
	}
	defer workerClient.Deregister(nil)

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
			task, err := workerClient.Await(actionNames, &WorkerAwaitOptions{
				BlockMS: &w.config.BlockMS,
			})
			if err != nil {
				<-sem // Release slot
				w.logger.Printf("Await error: %v, retrying...", err)
				time.Sleep(time.Second)
				continue
			}

			if task == nil {
				<-sem // Release slot - no task available
				continue
			}

			// Execute task in background
			w.wg.Add(1)
			go func(t *TaskAssignment) {
				defer w.wg.Done()
				defer func() { <-sem }()
				w.executeTask(workerClient, t)
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
}

// Close closes the connection.
func (w *ActionWorker) Close() error {
	w.Stop()
	if w.client != nil {
		return w.client.Close()
	}
	return nil
}

// executeTask executes a task with error recovery.
func (w *ActionWorker) executeTask(workerClient *WorkerClient, task *TaskAssignment) {
	defer func() {
		if r := recover(); r != nil {
			w.logger.Printf("Action handler panicked: %v", r)
			_ = workerClient.Fail(task.TaskID, fmt.Sprintf("action panicked: %v", r), nil)
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
		_ = workerClient.Fail(task.TaskID, fmt.Sprintf("no handler for: %s", task.TaskType), nil)
		return
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(w.ctx, w.config.ActionTimeout)
	defer cancel()

	// Create action context
	actx := &ActionContext{
		ctx:          ctx,
		namespace:    w.client.Namespace(),
		actionName:   task.TaskType,
		input:        task.Payload,
		taskID:       task.TaskID,
		attempt:      task.Attempt,
		workerClient: workerClient,
	}

	// Execute handler
	result, err := handler(actx)

	// Determine outcome
	if err != nil {
		if err == context.DeadlineExceeded {
			w.logger.Printf("Action timed out: %s", task.TaskType)
			_ = workerClient.Fail(task.TaskID, "action timed out", &WorkerFailOptions{Retry: false})
		} else if err == context.Canceled {
			w.logger.Printf("Action cancelled: %s", task.TaskType)
			_ = workerClient.Fail(task.TaskID, "action cancelled", &WorkerFailOptions{Retry: false})
		} else {
			w.logger.Printf("Action failed: %s - %v", task.TaskType, err)
			_ = workerClient.Fail(task.TaskID, err.Error(), &WorkerFailOptions{Retry: true})
		}
		return
	}

	// Success
	if err := workerClient.Complete(task.TaskID, result, nil); err != nil {
		w.logger.Printf("Failed to report completion: %v", err)
	} else {
		w.logger.Printf("Action completed: %s", task.TaskType)
	}
}

// ActionContext provides context to action handlers.
type ActionContext struct {
	ctx          context.Context
	namespace    string
	actionName   string
	input        []byte
	taskID       string
	attempt      uint32
	workerClient *WorkerClient
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
	return a.workerClient.Touch(a.taskID, &WorkerTouchOptions{
		ExtendMS: &extendMS,
	})
}

// ActionHandler is a function that executes action logic.
type ActionHandler func(actx *ActionContext) (result []byte, err error)
