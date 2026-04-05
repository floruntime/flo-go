package flo

import (
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

	// ErrUnsupportedVersion indicates an unsupported protocol version.
	ErrUnsupportedVersion = errors.New("flo: unsupported protocol version")

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
)

// ServerError represents an error returned by the Flo server.
type ServerError struct {
	Status  StatusCode
	Message string
}

// Error implements the error interface.
func (e *ServerError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("flo: server error (%s): %s", e.Status, e.Message)
	}
	return fmt.Sprintf("flo: server error: %s", e.Status)
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

	// ErrInternal indicates an internal server error.
	ErrInternal = &ServerError{Status: StatusInternalError}
)

// newServerError creates a ServerError from a status code and optional data.
func newServerError(status StatusCode, data []byte) error {
	msg := ""
	if len(data) > 0 {
		msg = string(data)
	}
	return &ServerError{
		Status:  status,
		Message: msg,
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

// IsInternal returns true if the error is an internal server error.
func IsInternal(err error) bool {
	return errors.Is(err, ErrInternal)
}
