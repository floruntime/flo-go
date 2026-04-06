package flo

import (
	"encoding/binary"
	"fmt"
)

// ActionClient provides action operations.
type ActionClient struct {
	client *Client
}

// =============================================================================
// Action Operations
// =============================================================================

// Register registers an action.
func (a *ActionClient) Register(name string, actionType ActionType, opts *ActionRegisterOptions) error {
	if opts == nil {
		opts = &ActionRegisterOptions{}
	}

	namespace := a.client.getNamespace(opts.Namespace)

	// Build value: [action_type:u8][timeout_ms:u32][max_retries:u32]
	//              [has_desc:u8][desc_len:u16]?[desc]?
	//              [has_wasm_module:u8=0][has_wasm_entrypoint:u8=0][has_wasm_memory_limit:u8=0]
	//              [has_trigger_stream:u8]...[has_trigger_group:u8]
	value := make([]byte, 0, 256)

	// Action type
	value = append(value, byte(actionType))

	// Timeout (default 30000)
	timeoutMS := uint32(30000)
	if opts.TimeoutMS != nil {
		timeoutMS = uint32(*opts.TimeoutMS)
	}
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, timeoutMS)
	value = append(value, buf...)

	// Max retries (default 3)
	maxRetries := uint32(3)
	if opts.MaxRetries != nil {
		maxRetries = uint32(*opts.MaxRetries)
	}
	binary.LittleEndian.PutUint32(buf, maxRetries)
	value = append(value, buf...)

	// Description (optional)
	if opts.Description != "" {
		value = append(value, 1) // has_desc
		buf = make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, uint16(len(opts.Description)))
		value = append(value, buf...)
		value = append(value, []byte(opts.Description)...)
	} else {
		value = append(value, 0) // no desc
	}

	// WASM fields (reserved, always zero for wire compat)
	value = append(value, 0) // no wasm_module
	value = append(value, 0) // no wasm_entrypoint
	value = append(value, 0) // no wasm_memory_limit

	value = append(value, 0) // has_trigger_stream
	value = append(value, 0) // has_trigger_group

	_, err := a.client.sendAndCheck(OpActionRegister, namespace, []byte(name), value, nil, true)
	return err
}

// Invoke invokes an action and returns the result.
// Use Status to poll for results.
func (a *ActionClient) Invoke(name string, input []byte, opts *ActionInvokeOptions) (*ActionInvokeResult, error) {
	if opts == nil {
		opts = &ActionInvokeOptions{}
	}

	namespace := a.client.getNamespace(opts.Namespace)

	// Build value: [priority:u8][delay_ms:i64][has_caller:u8]
	//              [has_idempotency_key:u8][key_len:u16]?[key]?
	//              [has_labels:u8][labels_len:u16]?[labels]?[input...]
	value := make([]byte, 0, len(input)+32)

	// Priority (default 10)
	priority := uint8(10)
	if opts.Priority != nil {
		priority = *opts.Priority
	}
	value = append(value, priority)

	// Delay (default 0)
	delayMS := int64(0)
	if opts.DelayMS != nil {
		delayMS = int64(*opts.DelayMS)
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(delayMS))
	value = append(value, buf...)

	// Caller ID (none)
	value = append(value, 0)

	// Idempotency key (optional)
	if opts.IdempotencyKey != "" {
		value = append(value, 1)
		buf = make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, uint16(len(opts.IdempotencyKey)))
		value = append(value, buf...)
		value = append(value, []byte(opts.IdempotencyKey)...)
	} else {
		value = append(value, 0)
	}

	// Labels (none)
	value = append(value, 0)

	// Input
	value = append(value, input...)

	resp, err := a.client.sendAndCheck(OpActionInvoke, namespace, []byte(name), value, nil, false)
	if err != nil {
		return nil, err
	}

	return parseActionInvokeResult(resp.Data)
}

// Status gets the status of an action run.
func (a *ActionClient) Status(runID string, opts *ActionStatusOptions) (*ActionRunStatus, error) {
	if opts == nil {
		opts = &ActionStatusOptions{}
	}

	namespace := a.client.getNamespace(opts.Namespace)

	resp, err := a.client.sendAndCheck(OpActionStatus, namespace, []byte(runID), nil, nil, true)
	if err != nil {
		return nil, err
	}

	return parseActionRunStatus(resp.Data)
}

// Delete deletes an action.
func (a *ActionClient) Delete(name string, opts *ActionStatusOptions) error {
	if opts == nil {
		opts = &ActionStatusOptions{}
	}

	namespace := a.client.getNamespace(opts.Namespace)

	_, err := a.client.sendAndCheck(OpActionDelete, namespace, []byte(name), nil, nil, true)
	return err
}

// =============================================================================
// Response Parsers
// =============================================================================

// parseActionInvokeResult parses the invoke response.
// Wire format: [run_id_len:u16][run_id][has_output:u8][output_len:u32]?[output]?
func parseActionInvokeResult(data []byte) (*ActionInvokeResult, error) {
	if len(data) < 3 { // min: u16 len + at least 1 byte run_id
		// Fallback: treat entire data as run_id (backwards compat with older servers)
		return &ActionInvokeResult{RunID: string(data)}, nil
	}

	pos := 0

	// Read run_id (length-prefixed u16)
	if pos+2 > len(data) {
		return &ActionInvokeResult{RunID: string(data)}, nil
	}
	runIDLen := int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2

	// Sanity check: if runIDLen is impossibly large or doesn't look right,
	// this is likely an old-format response (raw run_id string)
	if runIDLen > len(data)-pos || runIDLen > 256 || runIDLen == 0 {
		return &ActionInvokeResult{RunID: string(data)}, nil
	}

	runID := string(data[pos : pos+runIDLen])
	pos += runIDLen

	result := &ActionInvokeResult{RunID: runID}

	// Skip optional output field (wire compat — always empty now)
	if pos < len(data) && data[pos] == 1 {
		pos++
		if pos+4 <= len(data) {
			outputLen := int(binary.LittleEndian.Uint32(data[pos:]))
			pos += 4 + outputLen // skip output bytes
			_ = pos              // suppress unused warning
		}
	}

	return result, nil
}

func parseActionRunStatus(data []byte) (*ActionRunStatus, error) {
	if len(data) < 14 { // Minimum size
		return nil, fmt.Errorf("incomplete action run status response")
	}

	pos := 0

	// Read run_id
	if pos+2 > len(data) {
		return nil, fmt.Errorf("incomplete action run status: missing run_id length")
	}
	runIDLen := binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	if pos+int(runIDLen) > len(data) {
		return nil, fmt.Errorf("incomplete action run status: missing run_id")
	}
	runID := string(data[pos : pos+int(runIDLen)])
	pos += int(runIDLen)

	// Read status
	if pos+1 > len(data) {
		return nil, fmt.Errorf("incomplete action run status: missing status")
	}
	status := RunStatus(data[pos])
	pos++

	// Read created_at
	if pos+8 > len(data) {
		return nil, fmt.Errorf("incomplete action run status: missing created_at")
	}
	createdAt := int64(binary.LittleEndian.Uint64(data[pos:]))
	pos += 8

	// Read started_at (optional)
	var startedAt *int64
	if pos+1 > len(data) {
		return nil, fmt.Errorf("incomplete action run status: missing started_at flag")
	}
	if data[pos] == 1 {
		pos++
		if pos+8 > len(data) {
			return nil, fmt.Errorf("incomplete action run status: missing started_at")
		}
		v := int64(binary.LittleEndian.Uint64(data[pos:]))
		startedAt = &v
		pos += 8
	} else {
		pos++
	}

	// Read completed_at (optional)
	var completedAt *int64
	if pos+1 > len(data) {
		return nil, fmt.Errorf("incomplete action run status: missing completed_at flag")
	}
	if data[pos] == 1 {
		pos++
		if pos+8 > len(data) {
			return nil, fmt.Errorf("incomplete action run status: missing completed_at")
		}
		v := int64(binary.LittleEndian.Uint64(data[pos:]))
		completedAt = &v
		pos += 8
	} else {
		pos++
	}

	// Read output (optional)
	var output []byte
	if pos+1 > len(data) {
		return nil, fmt.Errorf("incomplete action run status: missing output flag")
	}
	if data[pos] == 1 {
		pos++
		if pos+4 > len(data) {
			return nil, fmt.Errorf("incomplete action run status: missing output length")
		}
		outputLen := binary.LittleEndian.Uint32(data[pos:])
		pos += 4
		if pos+int(outputLen) > len(data) {
			return nil, fmt.Errorf("incomplete action run status: missing output")
		}
		output = make([]byte, outputLen)
		copy(output, data[pos:pos+int(outputLen)])
		pos += int(outputLen)
	} else {
		pos++
	}

	// Read error_message (optional)
	var errorMessage string
	if pos+1 > len(data) {
		return nil, fmt.Errorf("incomplete action run status: missing error flag")
	}
	if data[pos] == 1 {
		pos++
		if pos+4 > len(data) {
			return nil, fmt.Errorf("incomplete action run status: missing error length")
		}
		errorLen := binary.LittleEndian.Uint32(data[pos:])
		pos += 4
		if pos+int(errorLen) > len(data) {
			return nil, fmt.Errorf("incomplete action run status: missing error message")
		}
		errorMessage = string(data[pos : pos+int(errorLen)])
		pos += int(errorLen)
	} else {
		pos++
	}

	// Read retry_count
	if pos+4 > len(data) {
		return nil, fmt.Errorf("incomplete action run status: missing retry_count")
	}
	retryCount := binary.LittleEndian.Uint32(data[pos:])

	return &ActionRunStatus{
		RunID:        runID,
		Status:       status,
		CreatedAt:    createdAt,
		StartedAt:    startedAt,
		CompletedAt:  completedAt,
		Output:       output,
		ErrorMessage: errorMessage,
		RetryCount:   retryCount,
	}, nil
}
