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
func (w *WorkerClient) Register(taskTypes []string, opts *WorkerRegisterOptions) error {
	if opts == nil {
		opts = &WorkerRegisterOptions{}
	}

	namespace := w.client.getNamespace(opts.Namespace)

	// Build process list: merge explicit Processes + legacy taskTypes
	processes := opts.Processes
	if len(processes) == 0 && len(taskTypes) > 0 {
		for _, name := range taskTypes {
			processes = append(processes, ProcessEntry{Name: name, Kind: ProcessKindAction})
		}
	}

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
// Returns the worker's current status (e.g. draining) from the server response.
func (w *WorkerClient) Heartbeat(currentLoad uint32, opts *WorkerHeartbeatOptions) (WorkerStatus, error) {
	if opts == nil {
		opts = &WorkerHeartbeatOptions{}
	}

	namespace := w.client.getNamespace(opts.Namespace)

	value := make([]byte, 4)
	binary.LittleEndian.PutUint32(value[0:], currentLoad)

	resp, err := w.client.sendAndCheck(OpWorkerHeartbeat, namespace, []byte(w.workerID), value, nil, true)
	if err != nil {
		return WorkerStatusActive, err
	}

	// Server responds with [status:u8]
	if resp != nil && len(resp.Data) >= 1 {
		return WorkerStatus(resp.Data[0]), nil
	}
	return WorkerStatusActive, nil
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

// Await waits for a task assignment (action_await).
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

	resp, err := w.client.sendAndCheck(OpActionAwait, namespace, []byte(w.workerID), value, builder.Build(), true)
	if err != nil {
		return nil, err
	}

	if len(resp.Data) == 0 {
		return nil, nil // No task available
	}

	return parseTaskAssignment(resp.Data)
}

// Complete marks a task as successfully completed.
// Wire format: [action_name_len:u16][action_name][task_id_len:u16][task_id][outcome_len:u16][outcome][result_len:u16][result]
func (w *WorkerClient) Complete(actionName string, taskID string, result []byte, opts *WorkerCompleteOptions) error {
	if opts == nil {
		opts = &WorkerCompleteOptions{}
	}

	namespace := w.client.getNamespace(opts.Namespace)

	outcome := opts.Outcome
	if outcome == "" {
		outcome = "success"
	}

	value := make([]byte, 2+len(actionName)+2+len(taskID)+2+len(outcome)+2+len(result))
	offset := 0

	// action_name
	binary.LittleEndian.PutUint16(value[offset:], uint16(len(actionName)))
	offset += 2
	copy(value[offset:], actionName)
	offset += len(actionName)

	// task_id
	binary.LittleEndian.PutUint16(value[offset:], uint16(len(taskID)))
	offset += 2
	copy(value[offset:], taskID)
	offset += len(taskID)

	// outcome
	binary.LittleEndian.PutUint16(value[offset:], uint16(len(outcome)))
	offset += 2
	copy(value[offset:], outcome)
	offset += len(outcome)

	// result
	binary.LittleEndian.PutUint16(value[offset:], uint16(len(result)))
	offset += 2
	copy(value[offset:], result)

	_, err := w.client.sendAndCheck(OpActionComplete, namespace, []byte(w.workerID), value, nil, true)
	return err
}

// Fail marks a task as failed.
// Wire format: [action_name_len:u16][action_name][task_id_len:u16][task_id][retry:u8][error_message...]
func (w *WorkerClient) Fail(actionName string, taskID string, errorMessage string, opts *WorkerFailOptions) error {
	if opts == nil {
		opts = &WorkerFailOptions{}
	}

	namespace := w.client.getNamespace(opts.Namespace)

	value := make([]byte, 2+len(actionName)+2+len(taskID)+1+len(errorMessage))
	offset := 0

	// action_name
	binary.LittleEndian.PutUint16(value[offset:], uint16(len(actionName)))
	offset += 2
	copy(value[offset:], actionName)
	offset += len(actionName)

	// task_id
	binary.LittleEndian.PutUint16(value[offset:], uint16(len(taskID)))
	offset += 2
	copy(value[offset:], taskID)
	offset += len(taskID)

	// retry flag
	if opts.Retry {
		value[offset] = 1
	} else {
		value[offset] = 0
	}
	offset++

	// error_message
	copy(value[offset:], errorMessage)

	_, err := w.client.sendAndCheck(OpActionFail, namespace, []byte(w.workerID), value, nil, true)
	return err
}

// Touch extends the lease on a task.
// Wire format: [action_name_len:u16][action_name][task_id_len:u16][task_id][extend_ms:u32]
func (w *WorkerClient) Touch(actionName string, taskID string, opts *WorkerTouchOptions) error {
	if opts == nil {
		opts = &WorkerTouchOptions{}
	}

	namespace := w.client.getNamespace(opts.Namespace)

	extendMS := uint32(30000)
	if opts.ExtendMS != nil {
		extendMS = *opts.ExtendMS
	}

	value := make([]byte, 2+len(actionName)+2+len(taskID)+4)
	offset := 0

	// action_name
	binary.LittleEndian.PutUint16(value[offset:], uint16(len(actionName)))
	offset += 2
	copy(value[offset:], actionName)
	offset += len(actionName)

	// task_id
	binary.LittleEndian.PutUint16(value[offset:], uint16(len(taskID)))
	offset += 2
	copy(value[offset:], taskID)
	offset += len(taskID)

	// extend_ms
	binary.LittleEndian.PutUint32(value[offset:], extendMS)

	_, err := w.client.sendAndCheck(OpActionTouch, namespace, []byte(w.workerID), value, nil, true)
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

	// caller block: [has_caller:u8][run_id_len:u16][run_id][wf_name_len:u16][wf_name]
	if pos+1 > len(data) {
		return nil, fmt.Errorf("incomplete task assignment: missing has_caller")
	}
	hasCaller := data[pos]
	pos += 1
	var callerRunID string
	var callerWorkflowName string
	if hasCaller == 1 {
		if pos+2 > len(data) {
			return nil, fmt.Errorf("incomplete task assignment: missing caller_run_id length")
		}
		cridLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if pos+cridLen > len(data) {
			return nil, fmt.Errorf("incomplete task assignment: missing caller_run_id")
		}
		callerRunID = string(data[pos : pos+cridLen])
		pos += cridLen

		if pos+2 > len(data) {
			return nil, fmt.Errorf("incomplete task assignment: missing caller_workflow_name length")
		}
		cwnLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if pos+cwnLen > len(data) {
			return nil, fmt.Errorf("incomplete task assignment: missing caller_workflow_name")
		}
		callerWorkflowName = string(data[pos : pos+cwnLen])
		pos += cwnLen
	}

	// Read payload (rest of data)
	var payload []byte
	if pos < len(data) {
		payload = make([]byte, len(data)-pos)
		copy(payload, data[pos:])
	}

	return &TaskAssignment{
		TaskID:             taskID,
		TaskType:           taskType,
		Payload:            payload,
		CreatedAt:          createdAt,
		Attempt:            attempt,
		CallerRunID:        callerRunID,
		CallerWorkflowName: callerWorkflowName,
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
