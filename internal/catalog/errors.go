package catalog

import (
	"errors"
	"fmt"
)

type ErrorCode string

const (
	ErrorInvalidArgument    ErrorCode = "invalid_argument"
	ErrorNotFound           ErrorCode = "not_found"
	ErrorUnsupported        ErrorCode = "unsupported"
	ErrorInvariantViolation ErrorCode = "invariant_violation"
	ErrorConflict           ErrorCode = "conflict"
	ErrorCancelled          ErrorCode = "cancelled"
	ErrorOperationFailed    ErrorCode = "operation_failed"
)

// Error is the backend-neutral catalog error boundary.
type Error struct {
	Code      ErrorCode
	Operation string
	Invariant string
	Message   string
	Cause     error
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	message := e.Message
	if message == "" && e.Cause != nil {
		message = e.Cause.Error()
	}
	if e.Operation == "" {
		return message
	}
	if message == "" {
		return "catalog " + e.Operation
	}
	return fmt.Sprintf("catalog %s: %s", e.Operation, message)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func NewError(code ErrorCode, operation, invariant, message string, cause error) *Error {
	return &Error{Code: code, Operation: operation, Invariant: invariant, Message: message, Cause: cause}
}

func CodeOf(err error) (ErrorCode, bool) {
	var catalogErr *Error
	if !errors.As(err, &catalogErr) || catalogErr == nil {
		return "", false
	}
	return catalogErr.Code, true
}

func IsCode(err error, code ErrorCode) bool {
	actual, ok := CodeOf(err)
	return ok && actual == code
}

var deferredCause = errors.New("catalog operation not implemented")

// ErrNotImplemented is retained for error-taxonomy compatibility. No active
// catalog operation returns it after Phase 9 completed all four planning APIs.
var ErrNotImplemented error = NewError(
	ErrorUnsupported,
	"deferred planning operation",
	"catalog_planning_api_must_be_implemented",
	deferredCause.Error(),
	deferredCause,
)

// IsDeferred recognizes only the transitional sentinel, not all unsupported errors.
func IsDeferred(err error) bool { return errors.Is(err, ErrNotImplemented) }
