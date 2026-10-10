package flo

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Base errors
var (
	// ErrNotConnected indicates the client is not connected to the server.
	ErrNotConnected = errors.New("flo: not connected to server")

	// ErrConnectionFailed indicates a connection failure.
	ErrConnectionFailed = errors.New("flo: connection failed")

	// ErrInvalidEndpoint indicates an invalid endpoint format.
	ErrInvalidEndpoint = errors.New("flo: invalid endpoint format")

	// ErrUnexpectedEOF indicates an unexpected end of stream.
	ErrUnexpectedEOF = errors.New("flo: unexpected end of stream")

	// ErrInvalidMagic indicates an invalid protocol magic number.
	ErrInvalidMagic = errors.New("flo: invalid protocol magic")

	// ErrTableMismatch matches a *TableMismatchError.
	ErrTableMismatch = errors.New("flo: server built from another protocol version or op table")

	// ErrInvalidChecksum indicates a CRC32 checksum validation failure.
	ErrInvalidChecksum = errors.New("flo: invalid checksum")

	// ErrIncompleteResponse indicates an incomplete response.
	ErrIncompleteResponse = errors.New("flo: incomplete response")

	// ErrNamespaceTooLarge indicates the namespace exceeds maximum size.
	ErrNamespaceTooLarge = errors.New("flo: namespace too large (max 255 bytes)")

	// ErrKeyTooLarge indicates the key exceeds maximum size.
	ErrKeyTooLarge = errors.New("flo: key too large (max 64 KB)")

	// ErrValueTooLarge indicates the value exceeds maximum size.
	ErrValueTooLarge = errors.New("flo: value too large (max 16 MB)")

	// ErrBlockTooLong indicates a BlockMS over MaxBlockMS. The server refuses
	// it with bad_request, so the SDK refuses it first.
	ErrBlockTooLong = fmt.Errorf("flo: a blocking wait (BlockMS) is at most %d ms (5 minutes)", MaxBlockMS)
)

// ServerError represents an error returned by the Flo server.
type ServerError struct {
	Status StatusCode
	// Reason says why; ReasonUnclassified when the server didn't say.
	Reason Reason
	// Ran says whether the request took effect.
	Ran     Ran
	Message string
}

// Error implements the error interface.
func (e *ServerError) Error() string {
	codes := e.Status.String()
	if e.Reason != ReasonUnclassified && e.Reason != 0 {
		codes += "/" + e.Reason.String()
	}
	if e.Message != "" {
		return fmt.Sprintf("flo: server error (%s): %s", codes, e.Message)
	}
	return fmt.Sprintf("flo: server error: %s", codes)
}

// Is implements error matching for errors.Is().
func (e *ServerError) Is(target error) bool {
	if t, ok := target.(*ServerError); ok {
		return e.Status == t.Status
	}
	return false
}

// Predefined server errors for use with errors.Is()
var (
	// ErrNotFound indicates the requested resource was not found.
	ErrNotFound = &ServerError{Status: StatusNotFound}

	// ErrBadRequest indicates invalid request parameters.
	ErrBadRequest = &ServerError{Status: StatusBadRequest}

	// ErrConflict indicates a conflict (e.g., CAS version mismatch).
	ErrConflict = &ServerError{Status: StatusConflict}

	// ErrUnauthorized indicates authentication required or failed.
	ErrUnauthorized = &ServerError{Status: StatusUnauthorized}

	// ErrOverloaded indicates the server is overloaded.
	ErrOverloaded = &ServerError{Status: StatusOverloaded}

	// ErrUnavailable indicates the write reached no leader, or the shard
	// stopped taking writes or is offline. Retryable, but an offline shard
	// stays unavailable until an operator acts; the server's message says
	// which case it is.
	ErrUnavailable = &ServerError{Status: StatusUnavailable}

	// ErrInternal indicates an internal server error.
	ErrInternal = &ServerError{Status: StatusInternalError}
)

// newServerError creates a ServerError from a status code and optional data.
// newServerError reads a refusal's body: [reason:u16][ran:u8][message].
func newServerError(status StatusCode, data []byte) error {
	if len(data) < 3 {
		return fmt.Errorf("%w: a refusal body of %d bytes", ErrIncompleteResponse, len(data))
	}
	reason := Reason(binary.LittleEndian.Uint16(data[0:2]))
	ran := Ran(data[2])
	if _, ok := reasonNames[reason]; !ok || ran > RanUnknown {
		return fmt.Errorf("%w: a refusal with reason %d and ran %d", ErrIncompleteResponse, reason, ran)
	}
	return &ServerError{
		Status:  status,
		Reason:  reason,
		Ran:     ran,
		Message: string(data[3:]),
	}
}

// IsConnectionError returns true if the error indicates a broken connection
// that may be resolved by reconnecting.
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, ErrUnexpectedEOF) || errors.Is(err, ErrNotConnected)
}

// checkStatus checks the response status and returns an error if not OK.
// If allowNotFound is true, StatusNotFound is not treated as an error.
func checkStatus(status StatusCode, data []byte, allowNotFound bool) error {
	if status == StatusOK {
		return nil
	}
	if allowNotFound && status == StatusNotFound {
		return nil
	}
	return newServerError(status, data)
}

// IsNotFound returns true if the error is a not found error.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// IsConflict returns true if the error is a conflict error.
func IsConflict(err error) bool {
	return errors.Is(err, ErrConflict)
}

// NonRetryableError indicates a failure that should not be retried.
// Wrap or return this error in an action handler to signal the server
// that the task should fail permanently (retry=false).
type NonRetryableError struct {
	Err error
}

func (e *NonRetryableError) Error() string {
	return e.Err.Error()
}

func (e *NonRetryableError) Unwrap() error {
	return e.Err
}

// NewNonRetryableError wraps an error as non-retryable.
func NewNonRetryableError(err error) *NonRetryableError {
	return &NonRetryableError{Err: err}
}

// NewNonRetryableErrorf creates a non-retryable error with a formatted message.
func NewNonRetryableErrorf(format string, args ...interface{}) *NonRetryableError {
	return &NonRetryableError{Err: fmt.Errorf(format, args...)}
}

// IsNonRetryable returns true if the error is a NonRetryableError.
func IsNonRetryable(err error) bool {
	var nr *NonRetryableError
	return errors.As(err, &nr)
}

// ActionResult represents a named outcome from an action handler.
// Use this to route workflows based on business outcomes.
type ActionResult struct {
	// Outcome is the named outcome string (maps to workflow transition keys).
	Outcome string
	// Data is the result payload bytes.
	Data []byte
}

// IsBadRequest returns true if the error is a bad request error.
func IsBadRequest(err error) bool {
	return errors.Is(err, ErrBadRequest)
}

// IsUnauthorized returns true if the error is an unauthorized error.
func IsUnauthorized(err error) bool {
	return errors.Is(err, ErrUnauthorized)
}

// IsOverloaded returns true if the error is an overloaded error.
func IsOverloaded(err error) bool {
	return errors.Is(err, ErrOverloaded)
}

// IsUnavailable returns true if the error is an unavailable error: no leader,
// or the shard isn't taking writes. Retryable, like overloaded.
func IsUnavailable(err error) bool {
	return errors.Is(err, ErrUnavailable)
}

// IsInternal returns true if the error is an internal server error. Not
// retryable: the server also uses it for a write that committed but wasn't
// applied, so resending it could apply it twice.
func IsInternal(err error) bool {
	return errors.Is(err, ErrInternal)
}

// TableMismatchError is returned when the server was built from another
// protocol version or op table than this SDK, so neither can read the
// other. Upgrade the SDK and the server together.
type TableMismatchError struct {
	ServerVersion uint8
	ServerTable   uint64
}

func (e *TableMismatchError) Error() string {
	if e.ServerVersion != Version {
		return fmt.Sprintf("flo: server protocol %d, client protocol %d: upgrade the client", e.ServerVersion, Version)
	}
	return fmt.Sprintf("flo: server table 0x%016x, client table 0x%016x: upgrade the client", e.ServerTable, TableHash)
}

func (e *TableMismatchError) Is(target error) bool { return target == ErrTableMismatch }
