package flo

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
)

// =============================================================================
// WorkerClient - Low-level worker protocol operations
// =============================================================================

// WorkerClient provides low-level worker operations.
type WorkerClient struct {
	client   *Client
	workerID string
}

// workerClient returns a WorkerClient for low-level worker protocol operations.
func (c *Client) workerClient(workerID string) *WorkerClient {
	return &WorkerClient{client: c, workerID: workerID}
}

// Register registers a worker in the worker registry.
// Wire format: [type:u8][max_concurrency:u32][process_count:u16]
//
//	([name_len:u16][name][kind:u8])*
//	[has_metadata:u8][metadata_len:u16][metadata]?
//	[has_machine_id:u8][machine_id_len:u16][machine_id]?
func (w *WorkerClient) Register(opts *WorkerRegisterOptions) error {
	if opts == nil {
		opts = &WorkerRegisterOptions{}
	}

	namespace := w.client.getNamespace(opts.Namespace)

	processes := opts.Processes

	metadata := opts.Metadata
	machineID := opts.MachineID
	maxConcurrency := opts.MaxConcurrency
	if maxConcurrency == 0 {
		maxConcurrency = 10
	}

	// Compute wire size
	size := 1 + 4 + 2 // type + max_concurrency + process_count
	for _, p := range processes {
		size += 2 + len(p.Name) + 1 // name_len + name + kind
	}
	size += 1 // has_metadata
	if metadata != "" {
		size += 2 + len(metadata)
	}
	size += 1 // has_machine_id
	if machineID != "" {
		size += 2 + len(machineID)
	}

	value := make([]byte, size)
	offset := 0

	value[offset] = uint8(opts.WorkerType)
	offset++

	binary.LittleEndian.PutUint32(value[offset:], maxConcurrency)
	offset += 4

	// Process list
	binary.LittleEndian.PutUint16(value[offset:], uint16(len(processes)))
	offset += 2
	for _, p := range processes {
		binary.LittleEndian.PutUint16(value[offset:], uint16(len(p.Name)))
		offset += 2
		copy(value[offset:], p.Name)
		offset += len(p.Name)
		value[offset] = uint8(p.Kind)
		offset++
	}

	// Metadata
	if metadata != "" {
		value[offset] = 1
		offset++
		binary.LittleEndian.PutUint16(value[offset:], uint16(len(metadata)))
		offset += 2
		copy(value[offset:], metadata)
		offset += len(metadata)
	} else {
		value[offset] = 0
		offset++
	}

	// Machine ID
	if machineID != "" {
		value[offset] = 1
		offset++
		binary.LittleEndian.PutUint16(value[offset:], uint16(len(machineID)))
		offset += 2
		copy(value[offset:], machineID)
	} else {
		value[offset] = 0
	}

	_, err := w.client.sendAndCheck(OpWorkerRegister, namespace, []byte(w.workerID), value, nil, true)
	return err
}

// Heartbeat sends a lightweight keep-alive to the worker registry.
// Wire format: [current_load:u32]
// Returns the worker's current status (e.g. draining) from the server
// response, or ErrNotFound when the server holds no such worker (it removes
// one silent for long enough): register again.
func (w *WorkerClient) Heartbeat(currentLoad uint32, opts *WorkerHeartbeatOptions) (WorkerStatus, error) {
	if opts == nil {
		opts = &WorkerHeartbeatOptions{}
	}

	namespace := w.client.getNamespace(opts.Namespace)

	value := make([]byte, 4)
	binary.LittleEndian.PutUint32(value[0:], currentLoad)

	resp, err := w.client.sendAndCheck(OpWorkerHeartbeat, namespace, []byte(w.workerID), value, nil, false)
	if err != nil {
		return WorkerStatusActive, err
	}

	// Server responds with [status:u8]
	if len(resp.Data) != 1 || WorkerStatus(resp.Data[0]) > WorkerStatusUnhealthy {
		return WorkerStatusActive, fmt.Errorf("%w: heartbeat answer % x", ErrIncompleteResponse, resp.Data)
	}
	return WorkerStatus(resp.Data[0]), nil
}

// Deregister removes the worker from the registry.
func (w *WorkerClient) Deregister(opts *WorkerDeregisterOptions) error {
	if opts == nil {
		opts = &WorkerDeregisterOptions{}
	}

	namespace := w.client.getNamespace(opts.Namespace)

	_, err := w.client.sendAndCheck(OpWorkerDeregister, namespace, []byte(w.workerID), nil, nil, true)
	return err
}

// Drain marks the worker as draining — no new tasks will be assigned.
// In-flight tasks continue to completion. The server will respond to
// subsequent heartbeats with WorkerStatusDraining.
func (w *WorkerClient) Drain(opts *WorkerDrainOptions) error {
	if opts == nil {
		opts = &WorkerDrainOptions{}
	}

	namespace := w.client.getNamespace(opts.Namespace)

	_, err := w.client.sendAndCheck(OpWorkerDrain, namespace, []byte(w.workerID), nil, nil, true)
	return err
}

// MaxWorkerListLimit is the most workers one ListWorkers page may ask for.
const MaxWorkerListLimit = 1024

// maxWorkerID is the longest worker id, and so the longest list cursor.
const maxWorkerID = 4096

// ListWorkers returns one page of the namespace's workers, in id order.
// Pass the result's Cursor in the next call's options to read the next page.
func (c *Client) ListWorkers(opts *WorkerListOptions) (*WorkerListResult, error) {
	if opts == nil {
		opts = &WorkerListOptions{}
	}
	body, err := encodeWorkerListBody(opts.Limit, opts.Cursor, opts.Action)
	if err != nil {
		return nil, err
	}
	resp, err := c.sendAndCheck(OpWorkerList, c.getNamespace(opts.Namespace), nil, body, nil, false)
	if err != nil {
		return nil, err
	}
	return decodeWorkerListAnswer(resp.Data)
}

// encodeWorkerListBody codes worker.list's request: limit, cursor, action.
func encodeWorkerListBody(limit uint32, cursor []byte, action string) ([]byte, error) {
	if limit == 0 {
		limit = MaxWorkerListLimit
	}
	if limit > MaxWorkerListLimit {
		return nil, fmt.Errorf("flo: worker list limit %d, at most %d", limit, MaxWorkerListLimit)
	}
	if len(cursor) > maxWorkerID {
		return nil, fmt.Errorf("flo: worker list cursor is %d bytes, at most %d", len(cursor), maxWorkerID)
	}
	if len(action) > maxWorkerID {
		return nil, fmt.Errorf("flo: worker list action is %d bytes, at most %d", len(action), maxWorkerID)
	}
	w := bodyWriter{buf: make([]byte, 0, 9+len(cursor)+len(action))}
	w.u8(1)
	w.u32(limit)
	w.bytes16(cursor)
	w.bytes16([]byte(action))
	return w.buf, nil
}

// decodeWorkerListAnswer reads worker.list's answer: workers, and the next
// page's cursor, nil on the last page.
func decodeWorkerListAnswer(data []byte) (*WorkerListResult, error) {
	r := bodyReader{buf: data}
	n := int(r.u16())
	if n > MaxWorkerListLimit {
		return nil, ErrIncompleteResponse
	}
	got := &WorkerListResult{Workers: make([]WorkerInfo, 0, n)}
	for i := 0; i < n && !r.bad; i++ {
		got.Workers = append(got.Workers, decodeWorkerRow(&r))
	}
	if r.present() {
		got.Cursor = r.bytes16()
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	return got, nil
}

// decodeWorkerRow reads one worker's row, as worker.list and worker.info
// both answer it.
func decodeWorkerRow(r *bodyReader) WorkerInfo {
	wi := WorkerInfo{
		ID:              string(r.bytes16()),
		Type:            WorkerType(r.u8()),
		Status:          WorkerStatus(r.u8()),
		CurrentLoad:     r.u32(),
		MaxConcurrency:  r.u32(),
		TasksCompleted:  r.u64(),
		TasksFailed:     r.u64(),
		RegisteredAtMS:  int64(r.u64()),
		LastHeartbeatMS: int64(r.u64()),
	}
	if r.present() {
		wi.MachineID = string(r.bytes8())
	}
	if wi.Type > WorkerTypeStream || wi.Status > WorkerStatusUnhealthy {
		r.bad = true
	}
	return wi
}

// maxWorkerProcesses is the most processes a worker registers, and so the
// most worker.info lists.
const maxWorkerProcesses = 1024

// WorkerInfo returns one worker's record: its row, the metadata it
// registered with, and its processes with their counts. A worker the server
// doesn't hold is an error that IsNotFound reports.
func (c *Client) WorkerInfo(workerID string, opts *WorkerInfoOptions) (*WorkerDetails, error) {
	if opts == nil {
		opts = &WorkerInfoOptions{}
	}
	resp, err := c.sendAndCheck(OpWorkerInfo, c.getNamespace(opts.Namespace), []byte(workerID), nil, nil, false)
	if err != nil {
		return nil, err
	}
	return decodeWorkerInfoAnswer(resp.Data)
}

// decodeWorkerInfoAnswer reads worker.info's answer: the worker's row,
// metadata, processes.
func decodeWorkerInfoAnswer(data []byte) (*WorkerDetails, error) {
	r := bodyReader{buf: data}
	got := &WorkerDetails{Worker: decodeWorkerRow(&r)}
	if r.present() {
		got.Metadata = string(r.bytes16())
	}
	n := int(r.u16())
	if n > maxWorkerProcesses {
		return nil, ErrIncompleteResponse
	}
	got.Processes = make([]WorkerProcess, 0, n)
	for i := 0; i < n && !r.bad; i++ {
		p := WorkerProcess{
			Name:        string(r.bytes16()),
			Kind:        ProcessKind(r.u8()),
			RunCount:    r.u64(),
			FailCount:   r.u64(),
			LastRunAtMS: int64(r.u64()),
		}
		if p.Kind > ProcessKindStreamConsumer {
			r.bad = true
		}
		got.Processes = append(got.Processes, p)
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	return got, nil
}

// The longest fields an await or a report carries.
const (
	maxAwaitActions = 64
	maxActionName   = 256
	maxOutcome      = 256
	maxResult       = 65535
	maxFailError    = 65535
	maxFailDelayMS  = 24 * 60 * 60 * 1000
	maxTaskInput    = 64 * 1024
	maxNamespaceLen = 63
)

// Await waits up to BlockMS for a task of one of actions, tried in order,
// and of the actions the worker registered. It returns nil when no run was
// ready in time.
func (w *WorkerClient) Await(actions []string, opts *WorkerAwaitOptions) (*Task, error) {
	if opts == nil {
		opts = &WorkerAwaitOptions{}
	}
	if err := checkBlockMS(opts.BlockMS); err != nil {
		return nil, err
	}
	// Send the server's 30000 default explicitly when unset, so the client
	// deadline covers the wait.
	blockMS := defaultWorkerBlockMS
	if opts.BlockMS != nil {
		blockMS = *opts.BlockMS
	}
	body, err := encodeAwaitBody(actions, blockMS, opts.RequestKey)
	if err != nil {
		return nil, err
	}
	resp, err := w.client.sendAndCheck(OpActionAwait, w.client.getNamespace(opts.Namespace), []byte(w.workerID), body, nil, false)
	if err != nil {
		return nil, err
	}
	return decodeAwaitAnswer(resp.Data)
}

// encodeAwaitBody codes action.await's request: actions, block_ms,
// request_key.
func encodeAwaitBody(actions []string, blockMS uint32, requestKey string) ([]byte, error) {
	if len(actions) == 0 || len(actions) > maxAwaitActions {
		return nil, fmt.Errorf("flo: an await names 1..%d actions, got %d", maxAwaitActions, len(actions))
	}
	if len(requestKey) > MaxRequestKey {
		return nil, fmt.Errorf("flo: request key is %d bytes, at most %d", len(requestKey), MaxRequestKey)
	}
	wr := bodyWriter{buf: make([]byte, 0, 16+len(requestKey))}
	wr.u8(uint8(len(actions)))
	for _, a := range actions {
		if a == "" || len(a) > maxActionName {
			return nil, fmt.Errorf("flo: action name %q: want 1..%d bytes", a, maxActionName)
		}
		wr.bytes16([]byte(a))
	}
	wr.u8(1)
	wr.u64(uint64(blockMS))
	if requestKey == "" {
		wr.u8(0)
	} else {
		wr.u8(1)
		wr.bytes8([]byte(requestKey))
	}
	return wr.buf, nil
}

// decodeAwaitAnswer reads action.await's answer: an optional task.
func decodeAwaitAnswer(data []byte) (*Task, error) {
	r := bodyReader{buf: data}
	if !r.present() {
		return nil, r.done()
	}
	t := &Task{}
	t.RunID = string(r.bytes16())
	t.Namespace = string(r.bytes8())
	t.Deliveries = r.u32()
	t.Term = r.u64()
	t.Index = r.u64()
	t.Action = string(r.bytes16())
	t.CreatedAtMS = int64(r.u64())
	t.Attempt = r.u32()
	t.LeaseMS = r.u64()
	if r.present() {
		t.CallerRunID = string(r.bytes16())
	}
	if r.present() {
		t.CallerWorkflowName = string(r.bytes16())
	}
	t.Input = r.bytes32()
	if len(t.Namespace) > maxNamespaceLen || len(t.Action) > maxActionName || len(t.Input) > maxTaskInput {
		r.bad = true
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	return t, nil
}

// reportBody starts a report's request with the delivery it names: action,
// worker, deliveries, term, index.
func (w *WorkerClient) reportBody(d Delivery, extra int) (*bodyWriter, error) {
	if d.RunID == "" {
		return nil, fmt.Errorf("flo: a report names a run")
	}
	if d.Action == "" || len(d.Action) > maxActionName {
		return nil, fmt.Errorf("flo: action name %q: want 1..%d bytes", d.Action, maxActionName)
	}
	if len(w.workerID) > maxWorkerID {
		return nil, fmt.Errorf("flo: worker id is %d bytes, at most %d", len(w.workerID), maxWorkerID)
	}
	wr := &bodyWriter{buf: make([]byte, 0, 2+len(d.Action)+2+len(w.workerID)+20+extra)}
	wr.bytes16([]byte(d.Action))
	wr.bytes16([]byte(w.workerID))
	wr.u32(d.Deliveries)
	wr.u64(d.Term)
	wr.u64(d.Index)
	return wr, nil
}

// Complete reports a delivery done with result. A resend of a complete that
// already applied answers ok; one for a delivery the run has moved on from,
// or with another result, is a *StaleReportError.
func (w *WorkerClient) Complete(d Delivery, result []byte, opts *WorkerCompleteOptions) error {
	if opts == nil {
		opts = &WorkerCompleteOptions{}
	}
	if len(opts.Outcome) > maxOutcome {
		return fmt.Errorf("flo: outcome is %d bytes, at most %d", len(opts.Outcome), maxOutcome)
	}
	if len(result) > maxResult {
		return fmt.Errorf("%w: a result is at most %d bytes, got %d", ErrValueTooLarge, maxResult, len(result))
	}
	wr, err := w.reportBody(d, 6+len(opts.Outcome)+len(result))
	if err != nil {
		return err
	}
	wr.u8(1)
	wr.bytes16([]byte(opts.Outcome))
	wr.u8(1)
	wr.bytes16(result)
	resp, err := w.client.sendAndCheck(OpActionComplete, w.client.getNamespace(opts.Namespace), []byte(d.RunID), wr.buf, nil, false)
	if err != nil {
		return err
	}
	return decodeEmptyAnswer(resp.Data)
}

// Fail reports a delivery failed with errorMessage. As for Complete, a
// resend that already applied answers ok.
func (w *WorkerClient) Fail(d Delivery, errorMessage string, opts *WorkerFailOptions) error {
	if opts == nil {
		opts = &WorkerFailOptions{}
	}
	if opts.DelayMS > maxFailDelayMS {
		return fmt.Errorf("flo: fail delay %d ms, at most %d", opts.DelayMS, maxFailDelayMS)
	}
	if len(errorMessage) > maxFailError {
		errorMessage = errorMessage[:maxFailError]
	}
	wr, err := w.reportBody(d, 16+len(errorMessage))
	if err != nil {
		return err
	}
	wr.u8(1)
	wr.u8(boolByte(opts.Retry))
	wr.u8(1)
	wr.u8(boolByte(opts.Abandoned))
	wr.u8(1)
	wr.u64(opts.DelayMS)
	wr.u8(1)
	wr.bytes16([]byte(errorMessage))
	resp, err := w.client.sendAndCheck(OpActionFail, w.client.getNamespace(opts.Namespace), []byte(d.RunID), wr.buf, nil, false)
	if err != nil {
		return err
	}
	return decodeEmptyAnswer(resp.Data)
}

// Touch keeps a delivery's lease and returns when it runs out, in Unix
// milliseconds. While more than half the lease is left it changes nothing
// and answers the current expiry; after that it renews the lease in full.
// No touch keeps a delivery past MaxHoldMS from when it was given.
func (w *WorkerClient) Touch(d Delivery, opts *WorkerTouchOptions) (int64, error) {
	if opts == nil {
		opts = &WorkerTouchOptions{}
	}
	wr, err := w.reportBody(d, 0)
	if err != nil {
		return 0, err
	}
	resp, err := w.client.sendAndCheck(OpActionTouch, w.client.getNamespace(opts.Namespace), []byte(d.RunID), wr.buf, nil, false)
	if err != nil {
		return 0, err
	}
	r := bodyReader{buf: resp.Data}
	at := int64(r.u64())
	return at, r.done()
}

func boolByte(b bool) uint8 {
	if b {
		return 1
	}
	return 0
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
