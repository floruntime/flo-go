package flo

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// Logger is a simple logging interface.
type Logger interface {
	Printf(format string, v ...interface{})
}

// WorkerConfig holds worker configuration.
type WorkerConfig struct {
	// Endpoint is the Flo server address (e.g., "localhost:3000")
	Endpoint string

	// Namespace is the namespace this worker operates in
	Namespace string

	// WorkerID uniquely identifies this worker instance (optional, auto-generated if empty)
	WorkerID string

	// Concurrency is the maximum number of concurrent actions (default: 10)
	Concurrency int

	// ActionTimeout defines the maximum duration allowed for an action handler
	ActionTimeout time.Duration

	// BlockMS is the timeout for blocking dequeue (default: 30000)
	BlockMS uint32

	// Logger for worker output (optional, defaults to log.Printf)
	Logger Logger
}

// Worker manages action execution using the Flo wire protocol.
type Worker struct {
	config WorkerConfig

	// Protocol client
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

// NewWorker creates a new Flo worker.
func NewWorker(cfg WorkerConfig) (*Worker, error) {
	// Validate required fields
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("Endpoint is required")
	}
	if cfg.Namespace == "" {
		return nil, fmt.Errorf("Namespace is required")
	}

	// Apply defaults
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 10
	}
	if cfg.ActionTimeout == 0 {
		cfg.ActionTimeout = 5 * time.Minute
	}
	if cfg.BlockMS == 0 {
		cfg.BlockMS = 30000
	}
	if cfg.Logger == nil {
		cfg.Logger = &stdLogger{}
	}

	// Generate WorkerID if not provided
	if cfg.WorkerID == "" {
		hostname, _ := os.Hostname()
		if hostname == "" {
			hostname = "unknown"
		}
		cfg.WorkerID = fmt.Sprintf("%s-%s", hostname, randomID(8))
	}

	// Create and connect client
	client := NewClient(cfg.Endpoint,
		WithNamespace(cfg.Namespace),
		WithTimeout(cfg.ActionTimeout),
	)

	if err := client.Connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to Flo: %w", err)
	}

	w := &Worker{
		config:   cfg,
		client:   client,
		handlers: make(map[string]ActionHandler),
		logger:   cfg.Logger,
	}

	return w, nil
}

// RegisterAction registers an action handler.
func (w *Worker) RegisterAction(actionName string, handler ActionHandler) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, exists := w.handlers[actionName]; exists {
		return fmt.Errorf("action '%s' is already registered", actionName)
	}

	// Register the action with the server
	actionClient := w.client.Action()
	if err := actionClient.Register(actionName, ActionTypeUser, nil); err != nil {
		return fmt.Errorf("failed to register action '%s': %w", actionName, err)
	}

	w.handlers[actionName] = handler
	w.logger.Printf("Registered action: %s", actionName)
	return nil
}

// MustRegisterAction registers an action handler and panics on error.
func (w *Worker) MustRegisterAction(actionName string, handler ActionHandler) {
	if err := w.RegisterAction(actionName, handler); err != nil {
		panic(fmt.Sprintf("failed to register action: %v", err))
	}
}

// Start begins polling and executing actions.
func (w *Worker) Start(ctx context.Context) error {
	w.ctx, w.cancel = context.WithCancel(ctx)
	defer w.cancel()

	w.logger.Printf("Starting Flo worker (id=%s, namespace=%s, concurrency=%d)",
		w.config.WorkerID, w.config.Namespace, w.config.Concurrency)

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

	// Register worker with all task types
	workerClient := w.client.Worker(w.config.WorkerID)
	if err := workerClient.Register(actionNames, nil); err != nil {
		return fmt.Errorf("failed to register worker: %w", err)
	}

	// Semaphore for concurrency control
	sem := make(chan struct{}, w.config.Concurrency)

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
func (w *Worker) Stop() {
	w.logger.Printf("Stopping worker...")
	if w.cancel != nil {
		w.cancel()
	}
}

// Close closes the connection.
func (w *Worker) Close() error {
	w.Stop()
	if w.client != nil {
		return w.client.Close()
	}
	return nil
}

// executeTask executes a task with error recovery.
func (w *Worker) executeTask(workerClient *WorkerClient, task *TaskAssignment) {
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
		namespace:    w.config.Namespace,
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

// =============================================================================
// WorkerClient - Low-level worker protocol operations
// =============================================================================

// WorkerClient provides low-level worker operations.
type WorkerClient struct {
	client   *Client
	workerID string
}

// Worker returns a WorkerClient for low-level worker operations.
func (c *Client) Worker(workerID string) *WorkerClient {
	return &WorkerClient{client: c, workerID: workerID}
}

// Register registers a worker with the given task types.
func (w *WorkerClient) Register(taskTypes []string, opts *WorkerRegisterOptions) error {
	if opts == nil {
		opts = &WorkerRegisterOptions{}
	}

	namespace := w.client.getNamespace(opts.Namespace)

	// Wire format: [count:u32][task_type_len:u16][task_type]...[has_caps:u8][caps]?
	size := 4 // count
	for _, tt := range taskTypes {
		size += 2 + len(tt)
	}
	size += 1 // has_caps flag
	if opts.Capabilities != "" {
		size += len(opts.Capabilities)
	}

	value := make([]byte, size)
	offset := 0

	// Task types count
	binary.LittleEndian.PutUint32(value[offset:], uint32(len(taskTypes)))
	offset += 4

	// Each task type: [len:u16][data]
	for _, tt := range taskTypes {
		binary.LittleEndian.PutUint16(value[offset:], uint16(len(tt)))
		offset += 2
		copy(value[offset:], tt)
		offset += len(tt)
	}

	// Capabilities flag and optional data
	if opts.Capabilities != "" {
		value[offset] = 1
		offset++
		copy(value[offset:], opts.Capabilities)
	} else {
		value[offset] = 0
	}

	_, err := w.client.sendAndCheck(OpWorkerRegister, namespace, []byte(w.workerID), value, nil, true)
	return err
}

// Await waits for a task assignment.
func (w *WorkerClient) Await(taskTypes []string, opts *WorkerAwaitOptions) (*TaskAssignment, error) {
	if opts == nil {
		opts = &WorkerAwaitOptions{}
	}

	namespace := w.client.getNamespace(opts.Namespace)

	// Wire format: [count:u32][task_type_len:u16][task_type]...
	size := 4 // count
	for _, tt := range taskTypes {
		size += 2 + len(tt)
	}

	value := make([]byte, size)
	offset := 0

	// Task types count
	binary.LittleEndian.PutUint32(value[offset:], uint32(len(taskTypes)))
	offset += 4

	// Each task type: [len:u16][data]
	for _, tt := range taskTypes {
		binary.LittleEndian.PutUint16(value[offset:], uint16(len(tt)))
		offset += 2
		copy(value[offset:], tt)
		offset += len(tt)
	}

	// Build options for blocking/timeout
	builder := NewOptionsBuilder()

	if opts.BlockMS != nil {
		builder.AddU32(OptBlockMS, *opts.BlockMS)
	}

	if opts.TimeoutMS != nil {
		builder.AddU64(OptTimeoutMS, *opts.TimeoutMS)
	}

	resp, err := w.client.sendAndCheck(OpWorkerAwait, namespace, []byte(w.workerID), value, builder.Build(), true)
	if err != nil {
		return nil, err
	}

	if len(resp.Data) == 0 {
		return nil, nil // No task available
	}

	return parseTaskAssignment(resp.Data)
}

// Complete marks a task as successfully completed.
func (w *WorkerClient) Complete(taskID string, result []byte, opts *WorkerCompleteOptions) error {
	if opts == nil {
		opts = &WorkerCompleteOptions{}
	}

	namespace := w.client.getNamespace(opts.Namespace)

	// Build value: [task_id_len:u16][task_id][result...]
	value := make([]byte, 2+len(taskID)+len(result))
	offset := 0

	binary.LittleEndian.PutUint16(value[offset:], uint16(len(taskID)))
	offset += 2
	copy(value[offset:], taskID)
	offset += len(taskID)
	copy(value[offset:], result)

	_, err := w.client.sendAndCheck(OpWorkerComplete, namespace, []byte(w.workerID), value, nil, true)
	return err
}

// Fail marks a task as failed.
func (w *WorkerClient) Fail(taskID string, errorMessage string, opts *WorkerFailOptions) error {
	if opts == nil {
		opts = &WorkerFailOptions{}
	}

	namespace := w.client.getNamespace(opts.Namespace)

	// Build value: [task_id_len:u16][task_id][error_message...]
	// Note: retry flag is handled via options, not in payload
	value := make([]byte, 2+len(taskID)+len(errorMessage))
	offset := 0

	binary.LittleEndian.PutUint16(value[offset:], uint16(len(taskID)))
	offset += 2
	copy(value[offset:], taskID)
	offset += len(taskID)
	copy(value[offset:], errorMessage)

	// Build options for retry flag
	var optionsData []byte
	if opts.Retry {
		builder := NewOptionsBuilder()
		builder.AddFlag(OptRetry)
		optionsData = builder.Build()
	}

	_, err := w.client.sendAndCheck(OpWorkerFail, namespace, []byte(w.workerID), value, optionsData, true)
	return err
}

// Touch extends the lease on a task.
func (w *WorkerClient) Touch(taskID string, opts *WorkerTouchOptions) error {
	if opts == nil {
		opts = &WorkerTouchOptions{}
	}

	namespace := w.client.getNamespace(opts.Namespace)

	// Build value: [task_id_len:u16][task_id][extend_ms:u32]
	extendMS := uint32(30000) // default 30 seconds
	if opts.ExtendMS != nil {
		extendMS = *opts.ExtendMS
	}

	value := make([]byte, 2+len(taskID)+4)
	offset := 0

	binary.LittleEndian.PutUint16(value[offset:], uint16(len(taskID)))
	offset += 2
	copy(value[offset:], taskID)
	offset += len(taskID)
	binary.LittleEndian.PutUint32(value[offset:], extendMS)

	_, err := w.client.sendAndCheck(OpWorkerTouch, namespace, []byte(w.workerID), value, nil, true)
	return err
}

func parseTaskAssignment(data []byte) (*TaskAssignment, error) {
	if len(data) < 10 { // Minimum size
		return nil, fmt.Errorf("incomplete task assignment response")
	}

	pos := 0

	// Read task_id
	if pos+2 > len(data) {
		return nil, fmt.Errorf("incomplete task assignment: missing task_id length")
	}
	taskIDLen := binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	if pos+int(taskIDLen) > len(data) {
		return nil, fmt.Errorf("incomplete task assignment: missing task_id")
	}
	taskID := string(data[pos : pos+int(taskIDLen)])
	pos += int(taskIDLen)

	// Read task_type
	if pos+2 > len(data) {
		return nil, fmt.Errorf("incomplete task assignment: missing task_type length")
	}
	taskTypeLen := binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	if pos+int(taskTypeLen) > len(data) {
		return nil, fmt.Errorf("incomplete task assignment: missing task_type")
	}
	taskType := string(data[pos : pos+int(taskTypeLen)])
	pos += int(taskTypeLen)

	// Read created_at
	if pos+8 > len(data) {
		return nil, fmt.Errorf("incomplete task assignment: missing created_at")
	}
	createdAt := int64(binary.LittleEndian.Uint64(data[pos:]))
	pos += 8

	// Read attempt
	if pos+4 > len(data) {
		return nil, fmt.Errorf("incomplete task assignment: missing attempt")
	}
	attempt := binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	// Read payload (rest of data)
	var payload []byte
	if pos < len(data) {
		payload = make([]byte, len(data)-pos)
		copy(payload, data[pos:])
	}

	return &TaskAssignment{
		TaskID:    taskID,
		TaskType:  taskType,
		Payload:   payload,
		CreatedAt: createdAt,
		Attempt:   attempt,
	}, nil
}

// stdLogger is a simple logger that uses log.Printf.
type stdLogger struct{}

func (l *stdLogger) Printf(format string, v ...interface{}) {
	log.Printf("[flo-worker] "+format, v...)
}

// randomID generates a random hex string of the given length.
func randomID(length int) string {
	b := make([]byte, (length+1)/2)
	rand.Read(b)
	return hex.EncodeToString(b)[:length]
}
