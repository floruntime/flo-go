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

	// Stream is the stream to consume from (required).
	Stream string

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
	config StreamWorkerOptions

	client  *Client
	handler StreamRecordHandler

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	logger Logger

	// Local counters (not sent in heartbeats — server tracks authoritatively)
	messagesProcessed uint64
	messagesFailed    uint64
}

// NewStreamWorker creates a new stream worker from an existing client.
func (c *Client) NewStreamWorker(opts StreamWorkerOptions, handler StreamRecordHandler) (*StreamWorker, error) {
	if opts.Stream == "" {
		return nil, fmt.Errorf("stream is required")
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

	return &StreamWorker{
		config:  opts,
		client:  workerClient,
		handler: handler,
		logger:  opts.Logger,
	}, nil
}

// Start begins consuming stream records.
func (sw *StreamWorker) Start(ctx context.Context) error {
	sw.ctx, sw.cancel = context.WithCancel(ctx)
	defer sw.cancel()

	sw.logger.Printf("Starting stream worker (id=%s, stream=%s, group=%s, consumer=%s)",
		sw.config.WorkerID, sw.config.Stream, sw.config.Group, sw.config.Consumer)

	// Join consumer group
	if err := sw.client.Stream.GroupJoin(sw.config.Stream, sw.config.Group, sw.config.Consumer, nil); err != nil {
		return fmt.Errorf("failed to join consumer group: %w", err)
	}

	// Register in worker registry with process entry
	wc := sw.client.workerClient(sw.config.WorkerID)
	processName := sw.config.Stream + "/" + sw.config.Group
	metadata := fmt.Sprintf(`{"stream":"%s","group":"%s","consumer":"%s"}`,
		sw.config.Stream, sw.config.Group, sw.config.Consumer)
	if err := wc.Register(nil, &WorkerRegisterOptions{
		WorkerType:     WorkerTypeStream,
		MaxConcurrency: uint32(sw.config.Concurrency),
		Processes:      []ProcessEntry{{Name: processName, Kind: ProcessKindStreamConsumer}},
		Metadata:       metadata,
		MachineID:      sw.config.MachineID,
	}); err != nil {
		sw.logger.Printf("Warning: failed to register in worker registry: %v", err)
	} else {
		defer wc.Deregister(nil)
	}

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

	// Main polling loop
	for {
		select {
		case <-sw.ctx.Done():
			sw.wg.Wait()
			// Leave consumer group
			_ = sw.client.Stream.GroupLeave(sw.config.Stream, sw.config.Group, sw.config.Consumer, nil)
			sw.logger.Printf("Stream worker stopped")
			return sw.ctx.Err()
		default:
			result, err := sw.client.Stream.GroupRead(
				sw.config.Stream, sw.config.Group, sw.config.Consumer,
				&StreamGroupReadOptions{
					Count:   &sw.config.BatchSize,
					BlockMS: &sw.config.BlockMS,
				},
			)
			if err != nil {
				sw.logger.Printf("GroupRead error: %v, retrying...", err)
				time.Sleep(time.Second)
				continue
			}

			if result == nil || len(result.Records) == 0 {
				continue
			}

			for _, record := range result.Records {
				sem <- struct{}{} // concurrency gate
				sw.wg.Add(1)
				go func(rec StreamRecord) {
					defer sw.wg.Done()
					defer func() { <-sem }()
					sw.processRecord(rec)
				}(record)
			}
		}
	}
}

// processRecord handles a single record with ack/nack.
func (sw *StreamWorker) processRecord(record StreamRecord) {
	defer func() {
		if r := recover(); r != nil {
			sw.logger.Printf("Stream handler panicked on id %s: %v", record.ID, r)
			atomic.AddUint64(&sw.messagesFailed, 1)
			_ = sw.client.Stream.GroupNack(sw.config.Stream, sw.config.Group, []StreamID{record.ID}, nil)
		}
	}()

	ctx, cancel := context.WithTimeout(sw.ctx, sw.config.MessageTimeout)
	defer cancel()

	sctx := &StreamContext{
		ctx:       ctx,
		record:    record,
		namespace: sw.client.Namespace(),
		stream:    sw.config.Stream,
		group:     sw.config.Group,
		consumer:  sw.config.Consumer,
	}

	if err := sw.handler(sctx); err != nil {
		atomic.AddUint64(&sw.messagesFailed, 1)
		sw.logger.Printf("Stream record %s failed: %v", record.ID, err)
		_ = sw.client.Stream.GroupNack(sw.config.Stream, sw.config.Group, []StreamID{record.ID}, nil)
		return
	}

	atomic.AddUint64(&sw.messagesProcessed, 1)
	_ = sw.client.Stream.GroupAck(sw.config.Stream, sw.config.Group, []StreamID{record.ID}, nil)
}

// Stop gracefully stops the stream worker.
func (sw *StreamWorker) Stop() {
	sw.logger.Printf("Stopping stream worker...")
	if sw.cancel != nil {
		sw.cancel()
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
