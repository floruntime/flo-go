package flo

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// StreamWorkerOptions holds stream worker configuration.
// Endpoint and namespace are inherited from the parent Client.
type StreamWorkerOptions struct {
	// WorkerID uniquely identifies this worker instance (optional, auto-generated if empty)
	WorkerID string

	// MachineID groups workers on the same machine (optional, defaults to hostname)
	MachineID string

	// Stream is a single stream to consume from (convenience shorthand).
	Stream string

	// Streams is a list of streams to consume from.
	// Can be used together with Stream — duplicates are removed.
	Streams []string

	// Group is the consumer group name (optional, defaults to "default").
	Group string

	// Consumer is the consumer name within the group (optional, defaults to WorkerID).
	Consumer string

	// Concurrency is the maximum number of concurrent message handlers (default: 10).
	Concurrency int

	// BatchSize is the number of messages to read per poll (default: 10).
	BatchSize uint32

	// BlockMS is the timeout for blocking read (default: 30000).
	BlockMS uint32

	// MessageTimeout defines the maximum duration allowed for a message handler.
	MessageTimeout time.Duration

	// Logger for worker output (optional, defaults to log.Printf).
	Logger Logger
}

// StreamRecordHandler processes a single stream record.
// Return nil for success (auto-ack) or an error to nack.
type StreamRecordHandler func(sctx *StreamContext) error

// StreamContext provides context and helpers to stream record handlers.
// Mirrors ActionContext — gives handlers access to metadata, deserialization,
// and the parent context without requiring manual json.Unmarshal.
type StreamContext struct {
	ctx       context.Context
	record    StreamRecord
	namespace string
	stream    string
	group     string
	consumer  string
}

// Ctx returns the context for this record processing.
func (sc *StreamContext) Ctx() context.Context { return sc.ctx }

// Context is an alias for Ctx.
func (sc *StreamContext) Context() context.Context { return sc.ctx }

// Record returns the underlying StreamRecord.
func (sc *StreamContext) Record() StreamRecord { return sc.record }

// StreamID returns the record's StreamID (timestamp + sequence).
func (sc *StreamContext) StreamID() StreamID { return sc.record.ID }

// Payload returns the raw record payload.
func (sc *StreamContext) Payload() []byte { return sc.record.Payload }

// Headers returns the record headers.
func (sc *StreamContext) Headers() map[string]string { return sc.record.Headers }

// Namespace returns the namespace.
func (sc *StreamContext) Namespace() string { return sc.namespace }

// Stream returns the stream name.
func (sc *StreamContext) Stream() string { return sc.stream }

// Group returns the consumer group name.
func (sc *StreamContext) Group() string { return sc.group }

// Consumer returns the consumer name.
func (sc *StreamContext) Consumer() string { return sc.consumer }

// Into unmarshals the record payload (JSON) into the provided value.
func (sc *StreamContext) Into(v interface{}) error {
	return sc.record.Into(v)
}

// Bytes marshals the provided value to JSON bytes.
func (sc *StreamContext) Bytes(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// StreamWorker processes stream records via consumer groups.
type StreamWorker struct {
	config  StreamWorkerOptions
	streams []string // resolved list of streams

	client  *Client
	handler StreamRecordHandler

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	logger Logger

	// Reconnection coordination
	reconnectMu   sync.Mutex
	lastReconnect time.Time

	// Local counters (not sent in heartbeats — server tracks authoritatively)
	messagesProcessed uint64
	messagesFailed    uint64
}

// resolveStreams merges Stream and Streams into a deduplicated list.
func resolveStreams(opts StreamWorkerOptions) ([]string, error) {
	seen := map[string]bool{}
	var streams []string
	// Stream first (backward compat), then Streams
	for _, s := range append([]string{opts.Stream}, opts.Streams...) {
		if s != "" && !seen[s] {
			seen[s] = true
			streams = append(streams, s)
		}
	}
	if len(streams) == 0 {
		return nil, fmt.Errorf("at least one stream is required (set Stream or Streams)")
	}
	return streams, nil
}

// NewStreamWorker creates a new stream worker from an existing client.
func (c *Client) NewStreamWorker(opts StreamWorkerOptions, handler StreamRecordHandler) (*StreamWorker, error) {
	streams, err := resolveStreams(opts)
	if err != nil {
		return nil, err
	}
	if opts.Group == "" {
		opts.Group = "default"
	}
	if handler == nil {
		return nil, fmt.Errorf("handler is required")
	}

	if opts.Concurrency == 0 {
		opts.Concurrency = 10
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 10
	}
	if opts.BlockMS == 0 {
		opts.BlockMS = 30000
	}
	if opts.MessageTimeout == 0 {
		opts.MessageTimeout = 5 * time.Minute
	}
	if opts.Logger == nil {
		opts.Logger = &streamStdLogger{}
	}
	if opts.WorkerID == "" {
		hostname, _ := os.Hostname()
		if hostname == "" {
			hostname = "unknown"
		}
		opts.WorkerID = fmt.Sprintf("%s-%s", hostname, randomID(8))
	}
	if opts.MachineID == "" {
		hostname, _ := os.Hostname()
		if hostname != "" {
			opts.MachineID = hostname
		}
	}
	if opts.Consumer == "" {
		opts.Consumer = opts.WorkerID
	}

	workerClient := NewClient(c.endpoint,
		WithNamespace(c.namespace),
		WithTimeout(opts.MessageTimeout),
		WithDebug(c.debug),
	)

	if err := workerClient.Connect(); err != nil {
		return nil, fmt.Errorf("failed to connect stream worker: %w", err)
	}

	// Backfill Stream for single-stream backward compat (used by processRecord/logging)
	if opts.Stream == "" {
		opts.Stream = streams[0]
	}

	return &StreamWorker{
		config:  opts,
		streams: streams,
		client:  workerClient,
		handler: handler,
		logger:  opts.Logger,
	}, nil
}

// Start begins consuming stream records from all configured streams.
func (sw *StreamWorker) Start(ctx context.Context) error {
	sw.ctx, sw.cancel = context.WithCancel(ctx)
	defer sw.cancel()

	sw.logger.Printf("Starting stream worker (id=%s, streams=%v, group=%s, consumer=%s)",
		sw.config.WorkerID, sw.streams, sw.config.Group, sw.config.Consumer)

	// Join consumer group on each stream
	for _, stream := range sw.streams {
		if err := sw.client.Stream.GroupJoin(stream, sw.config.Group, sw.config.Consumer, nil); err != nil {
			return fmt.Errorf("failed to join consumer group on stream %s: %w", stream, err)
		}
	}

	// Register in worker registry with a process entry per stream
	wc := sw.client.workerClient(sw.config.WorkerID)
	var processes []ProcessEntry
	for _, stream := range sw.streams {
		processes = append(processes, ProcessEntry{
			Name: stream + "/" + sw.config.Group,
			Kind: ProcessKindStreamConsumer,
		})
	}
	metadata := fmt.Sprintf(`{"streams":%q,"group":"%s","consumer":"%s"}`,
		sw.streams, sw.config.Group, sw.config.Consumer)
	if err := wc.Register(nil, &WorkerRegisterOptions{
		WorkerType:     WorkerTypeStream,
		MaxConcurrency: uint32(sw.config.Concurrency),
		Processes:      processes,
		Metadata:       metadata,
		MachineID:      sw.config.MachineID,
	}); err != nil {
		sw.logger.Printf("Warning: failed to register in worker registry: %v", err)
	} else {
		defer wc.Deregister(nil)
	}

	// Shared concurrency semaphore across all streams
	sem := make(chan struct{}, sw.config.Concurrency)

	// Start heartbeat
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-sw.ctx.Done():
				return
			case <-ticker.C:
				load := uint32(len(sem))
				status, err := wc.Heartbeat(load, nil)
				if err == nil && status == WorkerStatusDraining {
					sw.logger.Printf("Worker is draining, stopping...")
					sw.cancel()
					return
				}
			}
		}
	}()

	// One poll loop per stream, all sharing the concurrency semaphore
	errCh := make(chan error, len(sw.streams))
	for _, stream := range sw.streams {
		sw.wg.Add(1)
		go func(stream string) {
			defer sw.wg.Done()
			errCh <- sw.pollStream(stream, sem)
		}(stream)
	}

	// Wait for all poll loops to finish
	sw.wg.Wait()

	// Leave consumer groups
	for _, stream := range sw.streams {
		_ = sw.client.Stream.GroupLeave(stream, sw.config.Group, sw.config.Consumer, nil)
	}
	sw.logger.Printf("Stream worker stopped")

	// Return first non-context error, or the context error
	close(errCh)
	for err := range errCh {
		if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			return err
		}
	}
	return sw.ctx.Err()
}

// pollStream runs a single stream's poll loop.
func (sw *StreamWorker) pollStream(stream string, sem chan struct{}) error {
	for {
		select {
		case <-sw.ctx.Done():
			return sw.ctx.Err()
		default:
			result, err := sw.client.Stream.GroupRead(
				stream, sw.config.Group, sw.config.Consumer,
				&StreamGroupReadOptions{
					Count:   &sw.config.BatchSize,
					BlockMS: &sw.config.BlockMS,
				},
			)
			if err != nil {
				if IsConnectionError(err) {
					if reconErr := sw.handleReconnect(); reconErr != nil {
						if sw.ctx.Err() != nil {
							return sw.ctx.Err()
						}
						sw.logger.Printf("[%s] Reconnect failed: %v, retrying...", stream, reconErr)
					}
				} else {
					sw.logger.Printf("[%s] GroupRead error: %v, retrying...", stream, err)
				}
				time.Sleep(time.Second)
				continue
			}

			if result == nil || len(result.Records) == 0 {
				continue
			}

			for _, record := range result.Records {
				sem <- struct{}{} // concurrency gate
				go func(rec StreamRecord) {
					defer func() { <-sem }()
					sw.processRecord(stream, rec)
				}(record)
			}
		}
	}
}

// handleReconnect reconnects the client and re-joins consumer groups.
// Safe to call from multiple goroutines — only one reconnection happens at a time.
func (sw *StreamWorker) handleReconnect() error {
	sw.reconnectMu.Lock()
	defer sw.reconnectMu.Unlock()

	// If another goroutine just reconnected, skip
	if time.Since(sw.lastReconnect) < 2*time.Second {
		return nil
	}

	sw.logger.Printf("Connection lost, reconnecting...")
	if err := sw.client.ReconnectWithContext(sw.ctx); err != nil {
		return err
	}
	sw.lastReconnect = time.Now()

	// Re-join consumer groups
	for _, stream := range sw.streams {
		if err := sw.client.Stream.GroupJoin(stream, sw.config.Group, sw.config.Consumer, nil); err != nil {
			sw.logger.Printf("Warning: failed to re-join group on %s: %v", stream, err)
		}
	}

	// Re-register in worker registry
	wc := sw.client.workerClient(sw.config.WorkerID)
	var processes []ProcessEntry
	for _, stream := range sw.streams {
		processes = append(processes, ProcessEntry{
			Name: stream + "/" + sw.config.Group,
			Kind: ProcessKindStreamConsumer,
		})
	}
	metadata := fmt.Sprintf(`{"streams":%q,"group":"%s","consumer":"%s"}`,
		sw.streams, sw.config.Group, sw.config.Consumer)
	if err := wc.Register(nil, &WorkerRegisterOptions{
		WorkerType:     WorkerTypeStream,
		MaxConcurrency: uint32(sw.config.Concurrency),
		Processes:      processes,
		Metadata:       metadata,
		MachineID:      sw.config.MachineID,
	}); err != nil {
		sw.logger.Printf("Warning: failed to re-register: %v", err)
	}

	sw.logger.Printf("Reconnected, resuming work")
	return nil
}

// processRecord handles a single record with ack/nack.
func (sw *StreamWorker) processRecord(stream string, record StreamRecord) {
	defer func() {
		if r := recover(); r != nil {
			sw.logger.Printf("[%s] Stream handler panicked on id %s: %v", stream, record.ID, r)
			atomic.AddUint64(&sw.messagesFailed, 1)
			sw.ackWithRetry(stream, record.ID, false)
		}
	}()

	ctx, cancel := context.WithTimeout(sw.ctx, sw.config.MessageTimeout)
	defer cancel()

	sctx := &StreamContext{
		ctx:       ctx,
		record:    record,
		namespace: sw.client.Namespace(),
		stream:    stream,
		group:     sw.config.Group,
		consumer:  sw.config.Consumer,
	}

	if err := sw.handler(sctx); err != nil {
		atomic.AddUint64(&sw.messagesFailed, 1)
		sw.logger.Printf("[%s] Stream record %s failed: %v", stream, record.ID, err)
		sw.ackWithRetry(stream, record.ID, false)
		return
	}

	atomic.AddUint64(&sw.messagesProcessed, 1)
	sw.ackWithRetry(stream, record.ID, true)
}

// ackWithRetry sends an ack (or nack) to the server, reconnecting if the
// connection was lost while the record was being processed.
func (sw *StreamWorker) ackWithRetry(stream string, id StreamID, ack bool) {
	const maxAttempts = 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		var err error
		if ack {
			err = sw.client.Stream.GroupAck(stream, sw.config.Group, []StreamID{id}, nil)
		} else {
			err = sw.client.Stream.GroupNack(stream, sw.config.Group, []StreamID{id}, nil)
		}
		if err == nil {
			return
		}
		if !IsConnectionError(err) || sw.ctx.Err() != nil {
			return
		}
		op := "ack"
		if !ack {
			op = "nack"
		}
		sw.logger.Printf("[%s] Connection lost sending %s for %s (attempt %d/%d), reconnecting...", stream, op, id, attempt, maxAttempts)
		if reconErr := sw.handleReconnect(); reconErr != nil {
			return
		}
	}
}

// Stop gracefully stops the stream worker.
func (sw *StreamWorker) Stop() {
	sw.logger.Printf("Stopping stream worker...")
	if sw.cancel != nil {
		sw.cancel()
	}
	// Interrupt the connection to unblock any in-flight GroupRead call immediately.
	// Interrupt bypasses the mutex so it won't block behind a blocking read.
	if sw.client != nil {
		sw.client.Interrupt()
	}
}

// Close closes the connection.
func (sw *StreamWorker) Close() error {
	sw.Stop()
	if sw.client != nil {
		return sw.client.Close()
	}
	return nil
}

type streamStdLogger struct{}

func (l *streamStdLogger) Printf(format string, v ...interface{}) {
	log.Printf("[flo-stream-worker] "+format, v...)
}
