package flo

import (
	"encoding/binary"
	"fmt"
	"math"
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

	// Value: [action_type:u8][timeout_ms:u32][max_retries:u32][owner_len:u16][owner]
	maxRetries := uint32(3)
	if opts.MaxRetries != nil {
		maxRetries = *opts.MaxRetries
	}
	if len(opts.Owner) > math.MaxUint16 {
		return fmt.Errorf("flo: action owner is longer than %d bytes", math.MaxUint16)
	}
	value := make([]byte, 11, 11+len(opts.Owner))
	value[0] = byte(actionType)
	binary.LittleEndian.PutUint32(value[1:], opts.timeoutMS())
	binary.LittleEndian.PutUint32(value[5:], maxRetries)
	binary.LittleEndian.PutUint16(value[9:], uint16(len(opts.Owner)))
	value = append(value, opts.Owner...)

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

	value, err := encodeInvokeValue(opts.Labels, input)
	if err != nil {
		return nil, err
	}

	resp, err := a.client.sendAndCheck(OpActionInvoke, namespace, []byte(name), value, nil, false)
	if err != nil {
		return nil, err
	}

	return parseActionInvokeResult(resp.Data)
}

// encodeInvokeValue builds [has_labels:u8]([labels_len:u16 LE][labels])?[input...].
func encodeInvokeValue(labels string, input []byte) ([]byte, error) {
	if labels == "" {
		return append([]byte{0}, input...), nil
	}
	if len(labels) > math.MaxUint16 {
		return nil, fmt.Errorf("invoke labels are %d bytes; the limit is %d", len(labels), math.MaxUint16)
	}
	value := make([]byte, 3, 3+len(labels)+len(input))
	value[0] = 1
	binary.LittleEndian.PutUint16(value[1:], uint16(len(labels)))
	value = append(value, labels...)
	return append(value, input...), nil
}

// Status gets the status of an action run.
func (a *ActionClient) Status(runID string, opts *ActionStatusOptions) (*ActionRunStatus, error) {
	if opts == nil {
		opts = &ActionStatusOptions{}
	}

	namespace := a.client.getNamespace(opts.Namespace)

	// A table op: the run id is the key, and the body is empty.
	resp, err := a.client.sendAndCheck(OpActionStatus, namespace, []byte(runID), nil, nil, false)
	if err != nil {
		return nil, err
	}

	st, err := decodeActionStatusAnswer(resp.Data)
	if err != nil {
		return nil, err
	}
	st.RunID = runID
	return st, nil
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
// Wire format: [run_id_len:u16][run_id][has_output:u8]; the server never sets output, so it is not read.
func parseActionInvokeResult(data []byte) (*ActionInvokeResult, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("incomplete action invoke response")
	}
	runIDLen := int(binary.LittleEndian.Uint16(data))
	if runIDLen == 0 || 2+runIDLen > len(data) {
		return nil, fmt.Errorf("malformed action invoke response: run_id length %d in %d bytes", runIDLen, len(data))
	}
	return &ActionInvokeResult{RunID: string(data[2 : 2+runIDLen])}, nil
}

// decodeActionStatusAnswer reads action.status's answer:
// [status:u8][created_at_ms:i64][started_at_ms?][completed_at_ms?][attempt:u32]
// [output?: u16 length][error_message?: u16 length], each ? a present byte first.
func decodeActionStatusAnswer(data []byte) (*ActionRunStatus, error) {
	r := bodyReader{buf: data}
	st := &ActionRunStatus{Status: RunStatus(r.u8()), CreatedAt: int64(r.u64())}
	if r.present() {
		v := int64(r.u64())
		st.StartedAt = &v
	}
	if r.present() {
		v := int64(r.u64())
		st.CompletedAt = &v
	}
	st.Attempt = r.u32()
	if r.present() {
		st.Output = append([]byte{}, r.bytes16()...)
	}
	if r.present() {
		st.ErrorMessage = string(r.bytes16())
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	if st.Status > RunStatusTimedOut {
		return nil, fmt.Errorf("flo: action status %d is not one this client knows", st.Status)
	}
	return st, nil
}
