package flo

import (
	"context"
	"encoding/json"
	"errors"
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

	// BlockMS is the long-poll wait per read, at most MaxBlockMS.
	// 0 means 30000.
	BlockMS uint32

	// MessageTimeout defines the maximum duration allowed for a message handler.
	MessageTimeout time.Duration

	// NetworkTimeout is the TCP connect and per-request timeout for the
	// worker's connection. Blocking reads extend it by BlockMS automatically,
	// so it only needs to cover a normal request round-trip. Defaults to the
	// parent client's timeout (it is NOT tied to MessageTimeout).
	NetworkTimeout time.Duration

	// AutoTouch touches each record every sixth of its group's lease while
	// its handler runs; a touch renews the lease once half of it is gone.
	// Off by default: a handler that runs past the lease without calling
	// StreamContext.Touch has its record delivered again. However often it
	// is touched, the server holds a record at most MaxHoldMS.
	AutoTouch bool

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
	record    StreamGroupRecord
	lease     StreamGroupLease
	namespace string
	stream    string
	group     string
	consumer  string
	ack       *Client
}

// Ctx returns the context for this record processing.
func (sc *StreamContext) Ctx() context.Context { return sc.ctx }

// Context is an alias for Ctx.
func (sc *StreamContext) Context() context.Context { return sc.ctx }

// Record returns the underlying StreamRecord.
func (sc *StreamContext) Record() StreamRecord { return sc.record.StreamRecord }

// Deliveries counts the record's deliveries, this one included.
func (sc *StreamContext) Deliveries() uint32 { return sc.record.Deliveries }

// Touch keeps this record's lease, so a long handler doesn't have it
// delivered again. The report says whether it applied; a refusal means the
// delivery moved on.
func (sc *StreamContext) Touch() (*StreamGroupReport, error) {
	return sc.ack.Stream.GroupTouch(sc.stream, sc.group, sc.lease, []StreamGroupItem{sc.record.Item()}, nil)
}

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

	// ackClient carries acks, nacks and touches. client is held by a
	// blocking GroupRead for up to BlockMS, which can outlast the group's
	// lease and get finished records redelivered.
	ackClient        *Client
	ackReconnectMu   sync.Mutex
	lastAckReconnect time.Time
	// ackCtx bounds ack reconnects: it ends stopResultWait after the worker
	// starts stopping, or at Close.
	ackCtx      context.Context
	closeCtx    context.Context
	closeCancel context.CancelFunc

	// ctx ends on Stop or when Start's context does; handler contexts
	// derive from it, so a drain, which doesn't end it, lets handlers
	// finish. runMu guards cancel, which Stop reads while Start sets it.
	runMu  sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
	drain  drainState
	// pollCtx is ctx that also ends on a drain; reads and their reconnects
	// use it.
	pollCtx context.Context
	// wg covers the poll loops, handlers the record handlers Start waits for.
	wg       sync.WaitGroup
	handlers sync.WaitGroup
	logger   Logger

	// Reconnection coordination
	reconnectMu   sync.Mutex
	lastReconnect time.Time

	// running counts the record handlers running, for heartbeats.
	running atomic.Int32

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
//
// The worker opens two connections of its own (endpoint/namespace come from
// the parent): one for the blocking GroupRead, and one for acks and nacks so
// they don't wait behind it.
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
	blockMS, err := workerBlockMS(opts.BlockMS)
	if err != nil {
		return nil, err
	}
	opts.BlockMS = blockMS
	if opts.MessageTimeout == 0 {
		opts.MessageTimeout = 5 * time.Minute
	}
	if opts.NetworkTimeout == 0 {
		// Inherit the parent client's timeout — NOT MessageTimeout, which is
		// the handler execution budget and unrelated to the network deadline.
		opts.NetworkTimeout = c.timeout
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

	// Dedicated connection so blocking GroupRead does not hold the parent
	// client's mutex (mirrors ActionWorker). The parent can stay connected for
	// other RPCs while the worker long-polls.
	workerClient := NewClient(c.endpoint,
		WithNamespace(c.namespace),
		WithTimeout(opts.NetworkTimeout),
		WithDebug(c.debug),
		WithRetry(c.retry),
	)

	if err := workerClient.Connect(); err != nil {
		return nil, fmt.Errorf("failed to connect stream worker: %w", err)
	}

	ackClient := NewClient(c.endpoint,
		WithNamespace(c.namespace),
		WithTimeout(opts.NetworkTimeout),
		WithDebug(c.debug),
		WithRetry(c.retry),
	)
	if err := ackClient.Connect(); err != nil {
		workerClient.Close()
		return nil, fmt.Errorf("failed to connect stream worker ack client: %w", err)
	}

	// Backfill Stream for single-stream backward compat (used by processRecord/logging)
	if opts.Stream == "" {
		opts.Stream = streams[0]
	}

	closeCtx, closeCancel := context.WithCancel(context.Background())
	return &StreamWorker{
		ackCtx:      closeCtx,
		closeCtx:    closeCtx,
		closeCancel: closeCancel,
		config:      opts,
		streams:     streams,
		client:      workerClient,
		ackClient:   ackClient,
		handler:     handler,
		logger:      opts.Logger,
		drain:       newDrainState(),
	}, nil
}

// Start begins consuming stream records from all configured streams. Its
// registration waits out a halted shard, and an overloaded or unavailable
// refusal that didn't run, with a backoff; any other refusal fails Start
// with that error. Start fails with the limit error; a limit hit while
// running is logged and backed off.
func (sw *StreamWorker) Start(ctx context.Context) error {
	sw.runMu.Lock()
	sw.ctx, sw.cancel = context.WithCancel(ctx)
	sw.pollCtx = sw.drain.begin(sw.ctx)
	sw.runMu.Unlock()
	defer sw.cancel()
	defer sw.drain.end()

	// Handlers can outlive Start; once stopping, their acks may still
	// reconnect for stopResultWait.
	ackCtx, ackCancel := context.WithCancel(sw.closeCtx)
	sw.ackCtx = ackCtx
	context.AfterFunc(sw.ctx, func() { time.AfterFunc(stopResultWait, ackCancel) })

	sw.logger.Printf("Starting stream worker (id=%s, streams=%v, group=%s, consumer=%s)",
		sw.config.WorkerID, sw.streams, sw.config.Group, sw.config.Consumer)

	sem := make(chan struct{}, sw.config.Concurrency)

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
	regOpts := &WorkerRegisterOptions{
		WorkerType:     WorkerTypeStream,
		MaxConcurrency: uint32(sw.config.Concurrency),
		Processes:      processes,
		Metadata:       metadata,
		MachineID:      sw.config.MachineID,
	}
	// Whatever the registration's answer: one whose answer was lost, or a
	// drain's reconnect, still registers the worker.
	defer sendOnPoll(sw.client, sw.closeCtx, func() error { return wc.Deregister(nil) })
	// A halted shard, or a refusal that didn't run and may pass, holds the
	// start until it clears, or a stop or drain ends the wait. Any other
	// refusal fails Start.
	halted := newHaltedBackoff()
	refused := newWorkerBackoff()
	for {
		err := wc.Register(regOpts)
		if err == nil {
			break
		}
		if waitAtStart(sw.pollCtx, sw.logger, err, &halted, &refused) {
			if sw.pollCtx.Err() == nil {
				continue
			}
			break
		}
		var se *ServerError
		if errors.As(err, &se) {
			return fmt.Errorf("failed to register stream worker: %w", err)
		}
		// A lost connection: the poll loop reconnects, and registers again
		// when it does.
		sw.logger.Printf("Warning: failed to register in worker registry: %v", err)
		break
	}

	// Heartbeats go on until Start returns, so a draining worker's handlers
	// still count as alive.
	beats := &beater{
		wc:     wc,
		reg:    regOpts,
		drain:  &sw.drain,
		logger: sw.logger,
		load:   func() uint32 { return uint32(sw.running.Load()) },
		draining: func() {
			sw.logger.Printf("Worker is draining, finishing running records...")
			if sw.drain.start() {
				sw.client.Interrupt()
			}
		},
	}
	defer beats.start(heartbeatInterval)()

	// One poll loop per stream, all sharing the concurrency semaphore
	errCh := make(chan error, len(sw.streams))
	for _, stream := range sw.streams {
		sw.wg.Add(1)
		go func(stream string) {
			defer sw.wg.Done()
			errCh <- sw.pollStream(stream, sem)
		}(stream)
	}

	// Wait for all poll loops to finish, then for the records they handed
	// out, so their acks are sent.
	sw.wg.Wait()
	drained := false
	if sw.drain.active() && sw.ctx.Err() == nil {
		reconnect := func(ctx context.Context) error {
			if err := sw.client.ReconnectWithContext(ctx); err != nil {
				return err
			}
			if err := wc.Register(regOpts); err != nil {
				sw.logger.Printf("Warning: failed to re-register: %v", err)
			}
			return nil
		}
		drained = sw.drain.finish(sw.ctx, &sw.handlers, sw.logger, sw.client.IsConnected, func() error { return wc.Drain(nil) }, reconnect) == nil
	}
	sw.handlers.Wait()
	if drained {
		sw.logger.Printf("Stream worker drained")
		return nil
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
	var backoff emptyPollBackoff
	failed := newWorkerBackoff()
	halted := newHaltedBackoff()
	var limited limitNotice
	for {
		select {
		case <-sw.ctx.Done():
			return sw.ctx.Err()
		default:
			if sw.drain.active() {
				return nil
			}
			polled := time.Now()
			result, err := sw.client.Stream.GroupRead(
				stream, sw.config.Group, sw.config.Consumer,
				&StreamGroupReadOptions{
					Count:   &sw.config.BatchSize,
					BlockMS: &sw.config.BlockMS,
				},
			)
			if err != nil {
				if sw.drain.active() {
					// A drain cut the read; Start reconnects as needed.
					return nil
				}
				if IsConnectionError(err) {
					if reconErr := sw.handleReconnect(); reconErr != nil {
						if sw.pollCtx.Err() != nil {
							continue // a stop or drain; the loop top handles it
						}
						sw.logger.Printf("[%s] Reconnect failed: %v, retrying...", stream, reconErr)
					}
					sleepCtx(sw.pollCtx, time.Second)
				} else if IsOutcomePending(err) {
					// Reading again would resend it; the worker stops.
					sw.logger.Printf("Error: [%s] group read committed but not applied, stopping: %v", stream, err)
					sw.cancel()
					return err
				} else if !waitIfHalted(sw.pollCtx, sw.logger, "["+stream+"] group read", err, &halted) &&
					!limited.wait(sw.pollCtx, sw.logger, "["+stream+"] group read", err) {
					d := failed.next()
					if isRefusedUnrun(err) {
						// Routine, e.g. records being read back from disk: the
						// read leased nothing, so it's sent again.
						sw.logger.Printf("[%s] GroupRead not served yet: %v, retrying in %v...", stream, err, d)
					} else {
						sw.logger.Printf("[%s] GroupRead error: %v, retrying in %v...", stream, err, d)
					}
					sleepCtx(sw.pollCtx, d)
				}
				continue
			}
			failed.reset()
			halted.reset()
			limited.reset()

			if result == nil || len(result.Records) == 0 {
				backoff.afterEmpty(sw.pollCtx, polled, sw.config.BlockMS)
				continue
			}
			backoff.reset()
			// Touched from arrival, so records waiting for a handler slot
			// keep their lease too.
			touch := sw.startTouching(stream, result, time.Now())

			for i, record := range result.Records {
				// A record not started by a stop is handed back, not run
				// with a cancelled context.
				slot := false
				select {
				case sem <- struct{}{}: // concurrency gate
					slot = true
				case <-sw.ctx.Done():
				}
				if sw.ctx.Err() != nil {
					if slot {
						<-sem
					}
					sw.handBack(stream, result.Lease, result.Records[i:], touch)
					break
				}
				sw.handlers.Add(1)
				go func(rec StreamGroupRecord) {
					defer sw.handlers.Done()
					defer func() { <-sem }()
					sw.processRecord(stream, result.Lease, rec, touch)
				}(record)
			}
		}
	}
}

// handleReconnect reconnects the client and registers the worker again.
// Safe to call from multiple goroutines — only one reconnection happens at a time.
func (sw *StreamWorker) handleReconnect() error {
	sw.reconnectMu.Lock()
	defer sw.reconnectMu.Unlock()

	// If another goroutine just reconnected, skip. A client drops its
	// connection on any I/O error, so a recent reconnect only counts while
	// the connection is still up.
	if time.Since(sw.lastReconnect) < 2*time.Second && sw.client.IsConnected() {
		return nil
	}

	sw.logger.Printf("Connection lost, reconnecting...")
	if err := sw.client.ReconnectWithContext(sw.pollCtx); err != nil {
		return err
	}
	sw.lastReconnect = time.Now()

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
	if err := wc.Register(&WorkerRegisterOptions{
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

// processRecord handles a single record, then stops touching it and acks
// or nacks it. touch may be nil.
func (sw *StreamWorker) processRecord(stream string, lease StreamGroupLease, record StreamGroupRecord, touch *readToucher) {
	sw.running.Add(1)
	defer sw.running.Add(-1)
	defer touch.drop(record.ID)
	defer func() {
		if r := recover(); r != nil {
			sw.logger.Printf("[%s] Stream handler panicked on id %s: %v", stream, record.ID, r)
			atomic.AddUint64(&sw.messagesFailed, 1)
			touch.drop(record.ID)
			sw.reportWithRetry(stream, lease, record, false, fmt.Sprintf("handler panicked: %v", r))
		}
	}()

	ctx, cancel := context.WithTimeout(sw.ctx, sw.config.MessageTimeout)
	defer cancel()

	sctx := &StreamContext{
		ctx:       ctx,
		record:    record,
		lease:     lease,
		namespace: sw.client.Namespace(),
		stream:    stream,
		group:     sw.config.Group,
		consumer:  sw.config.Consumer,
		ack:       sw.ackClient,
	}

	err := sw.handler(sctx)
	if err != nil {
		atomic.AddUint64(&sw.messagesFailed, 1)
		sw.logger.Printf("[%s] Stream record %s failed: %v", stream, record.ID, err)
		touch.drop(record.ID)
		sw.reportWithRetry(stream, lease, record, false, err.Error())
		return
	}

	atomic.AddUint64(&sw.messagesProcessed, 1)
	touch.drop(record.ID)
	sw.reportWithRetry(stream, lease, record, true, "")
}

// readToucher keeps one read's unacked records leased: each touch names
// the read's delivery and the records not yet acked or nacked.
type readToucher struct {
	mu    sync.Mutex
	items map[StreamID]StreamGroupItem
	done  chan struct{}
	once  sync.Once
}

// drop stops touching id; with none left, touching ends. Safe on nil.
func (t *readToucher) drop(id StreamID) {
	if t == nil {
		return
	}
	t.mu.Lock()
	delete(t.items, id)
	empty := len(t.items) == 0
	t.mu.Unlock()
	if empty {
		t.once.Do(func() { close(t.done) })
	}
}

func (t *readToucher) pending() []StreamGroupItem {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]StreamGroupItem, 0, len(t.items))
	for _, it := range t.items {
		out = append(out, it)
	}
	return out
}

// startTouching touches a read's records every touchInterval of the lease
// the read's expiry gives, from when they arrived, whether their handlers
// have started or are waiting for a slot, until each is acked or nacked. The nth touch is
// due at arrival plus n intervals, a slot that passed during a slow touch
// skipped, and never later than an interval before the lease runs out by
// the server's last word: each touch's answer gives the expiry in the
// server's clock, read through the offset the read's own expiry showed, so a
// skewed clock doesn't let a lease lapse. A record a touch refuses (its
// delivery moved on) is dropped; a stale, conflict, not found or bad request
// refusal ends touching; anything else (overloaded, unavailable, a lost
// connection) is tried again next interval. Nil without AutoTouch.
func (sw *StreamWorker) startTouching(stream string, read *StreamGroupReadResult, arrived time.Time) *readToucher {
	if !sw.config.AutoTouch || len(read.Records) == 0 {
		return nil
	}
	t := &readToucher{items: make(map[StreamID]StreamGroupItem, len(read.Records)), done: make(chan struct{})}
	for _, r := range read.Records {
		t.items[r.ID] = r.Item()
	}
	// The lease is what the read's expiry leaves from arrival, within a
	// lease's bounds. serverAhead is how far the server's clock reads past
	// this one's, as far as the bounds show it.
	lease := time.UnixMilli(read.ExpiresAtMS).Sub(arrived)
	if lease <= 0 {
		lease = MinGroupAckTimeoutMS * time.Millisecond
	}
	lease = min(lease, MaxGroupAckTimeoutMS*time.Millisecond)
	interval := touchInterval(uint64(lease / time.Millisecond))
	localExpiry := arrived.Add(lease)
	serverAhead := time.UnixMilli(read.ExpiresAtMS).Sub(localExpiry)
	go func() {
		warned := map[Reason]bool{}
		next := arrived.Add(interval)
		timer := time.NewTimer(time.Until(next))
		defer timer.Stop()
		for {
			select {
			case <-t.done:
				return
			case <-sw.closeCtx.Done():
				return
			case <-timer.C:
			}
			items := t.pending()
			if len(items) == 0 {
				return
			}
			rep, err := sw.ackClient.Stream.GroupTouch(stream, sw.config.Group, read.Lease, items, nil)
			var se *ServerError
			switch {
			case err == nil:
				for _, f := range rep.Refused {
					if f.Why == QueueGone {
						// Acked or nacked as this touch went out.
						t.drop(f.ID)
						continue
					}
					sw.logger.Printf("[%s] Touch for %s was refused (%s), no longer touching it", stream, f.ID, f.Why)
					t.drop(f.ID)
				}
				if rep.ExpiresAtMS > 0 {
					localExpiry = time.UnixMilli(rep.ExpiresAtMS).Add(-serverAhead)
				}
			case errors.As(err, &se) && (se.Status == StatusConflict || se.Status == StatusNotFound || se.Status == StatusBadRequest):
				sw.logger.Printf("[%s] Touch was refused, stopping: %v", stream, err)
				return
			case IsConnectionError(err):
				sw.logger.Printf("[%s] Touch lost: %v; reconnecting", stream, err)
				_ = sw.reconnectAck()
			case IsLimit(err):
				if errors.As(err, &se) && !warned[se.Reason] {
					warned[se.Reason] = true
					sw.logger.Printf("[%s] Touch refused at a limit: %v; trying again", stream, err)
				}
			default:
				sw.logger.Printf("[%s] Touch failed: %v; trying again", stream, err)
			}
			now := time.Now()
			for !next.After(now) {
				next = next.Add(interval)
			}
			if latest := localExpiry.Add(-interval); next.After(latest) {
				next = latest
			}
			if earliest := now.Add(minTouchInterval); next.Before(earliest) {
				next = earliest
			}
			timer.Reset(time.Until(next))
		}
	}()
	return t
}

// reportWithRetry acks (or nacks, with why) a record, reconnecting if the
// connection was lost while the record was being processed and sending the
// same report again: one that already applied is refused, never applied
// twice.
func (sw *StreamWorker) reportWithRetry(stream string, lease StreamGroupLease, record StreamGroupRecord, ack bool, why string) {
	op := "ack"
	if !ack {
		op = "nack"
	}
	id := record.ID
	items := []StreamGroupItem{record.Item()}
	const maxAttempts = 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		var rep *StreamGroupReport
		var err error
		if ack {
			rep, err = sw.ackClient.Stream.GroupAck(stream, sw.config.Group, lease, items, nil)
		} else {
			rep, err = sw.ackClient.Stream.GroupNack(stream, sw.config.Group, lease, items, &StreamGroupNackOptions{Error: truncateError(why)})
		}
		if err == nil {
			if len(rep.Refused) > 0 && attempt == 1 {
				sw.logger.Printf("[%s] %s for %s applied nothing (%s): the record's delivery moved on", stream, op, id, rep.Refused[0].Why)
			}
			return
		}
		if !resultLost(err) {
			sw.logger.Printf("Error: [%s] dropped %s for %s, the record will be redelivered: %v", stream, op, id, err)
			return
		}
		if attempt == maxAttempts {
			sw.logger.Printf("Error: [%s] dropped %s for %s after %d sends, the record will be redelivered: %v", stream, op, id, maxAttempts, err)
			return
		}
		sw.logger.Printf("[%s] Connection lost sending %s for %s (attempt %d/%d), reconnecting...", stream, op, id, attempt, maxAttempts)
		if reconErr := sw.reconnectAck(); reconErr != nil {
			sw.logger.Printf("Error: [%s] dropped %s for %s, reconnect failed, the record will be redelivered: %v", stream, op, id, reconErr)
			return
		}
	}
}

// handBack nacks records a stop left unstarted, at once and in one
// report: each spends an attempt, so one at its last is dead.
func (sw *StreamWorker) handBack(stream string, lease StreamGroupLease, records []StreamGroupRecord, touch *readToucher) {
	items := make([]StreamGroupItem, len(records))
	for i, r := range records {
		touch.drop(r.ID)
		items[i] = r.Item()
	}
	opts := &StreamGroupNackOptions{Error: "worker stopped before handling it"}
	for attempt := 1; ; attempt++ {
		_, err := sw.ackClient.Stream.GroupNack(stream, sw.config.Group, lease, items, opts)
		if err == nil {
			sw.logger.Printf("[%s] Handed back %d records not started before the stop", stream, len(items))
			return
		}
		if !resultLost(err) || attempt == 3 || sw.reconnectAck() != nil {
			sw.logger.Printf("Error: [%s] could not hand back %d records, they come back once their lease runs out: %v", stream, len(items), err)
			return
		}
	}
}

// truncateError cuts a nack's error to what the server takes.
func truncateError(s string) string {
	if len(s) > maxGroupNackError {
		return s[:maxGroupNackError]
	}
	return s
}

// reconnectAck reconnects the ack connection. Concurrent handlers that hit
// the same broken connection reconnect it once rather than tearing down each
// other's fresh connection.
func (sw *StreamWorker) reconnectAck() error {
	sw.ackReconnectMu.Lock()
	defer sw.ackReconnectMu.Unlock()
	if time.Since(sw.lastAckReconnect) < 2*time.Second && sw.ackClient.IsConnected() {
		return nil
	}
	ctx, cancel := context.WithTimeout(sw.ackCtx, resultReconnectWait)
	defer cancel()
	if err := sw.ackClient.ReconnectWithContext(ctx); err != nil {
		return err
	}
	sw.lastAckReconnect = time.Now()
	return nil
}

// Drain stops reading new records and lets the records already read finish
// and ack, then Start returns nil. The server is told; the
// notice is retried while the worker is disconnected. A held GroupRead
// doesn't hold it up: the poll connection is closed, since the server
// doesn't answer it on a drain. A Stop during a drain still cancels the
// running handlers.
func (sw *StreamWorker) Drain() {
	sw.logger.Printf("Draining stream worker...")
	// Unblock an in-flight GroupRead, as Stop does; with no run, the next
	// Start still needs the connection.
	if sw.drain.start() && sw.client != nil {
		sw.client.Interrupt()
	}
}

// Stop stops the stream worker: it reads nothing more and cancels the
// running handlers' contexts. Records a read took that haven't started are
// not run: they are handed back at once with a nack, which spends one of
// their attempts, so with MaxDeliver 1 it leaves them dead.
func (sw *StreamWorker) Stop() {
	sw.logger.Printf("Stopping stream worker...")
	sw.runMu.Lock()
	if sw.cancel != nil {
		sw.cancel()
	}
	sw.runMu.Unlock()
	// Interrupt the connection to unblock any in-flight GroupRead call immediately.
	// Interrupt bypasses the mutex so it won't block behind a blocking read.
	if sw.client != nil {
		sw.client.Interrupt()
	}
}

// Close stops the worker and closes its dedicated connections.
func (sw *StreamWorker) Close() error {
	sw.Stop()
	sw.closeCancel()
	if sw.ackClient != nil {
		// Stop leaves the ack connection up so in-flight acks land during a
		// drain; Close doesn't wait out a stalled one.
		sw.ackClient.Interrupt()
		sw.ackClient.Close()
	}
	if sw.client != nil {
		return sw.client.Close()
	}
	return nil
}

type streamStdLogger struct{}

func (l *streamStdLogger) Printf(format string, v ...interface{}) {
	log.Printf("[flo-stream-worker] "+format, v...)
}
