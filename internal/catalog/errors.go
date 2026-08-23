package catalog

import (
	"context"
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
	cause     error
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	message := e.Message
	if message == "" && e.cause != nil {
		message = e.cause.Error()
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
	return e.cause
}

func NewError(code ErrorCode, operation, invariant, message string, cause error) *Error {
	if !validErrorCode(code) {
		code = ErrorOperationFailed
	}
	return &Error{Code: code, Operation: operation, Invariant: invariant, Message: message, cause: cause}
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

// translateServiceError completes the backend-neutral error boundary for
// aggregate Service methods whose private query helpers intentionally retain
// contextual database errors. It must be called exactly once at the public
// method boundary.
func translateServiceError(operation, message string, err error) error {
	if err == nil {
		return nil
	}
	var catalogErr *Error
	if errors.As(err, &catalogErr) && catalogErr != nil {
		return catalogErr
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return NewError(ErrorCancelled, operation, "", message, err)
	}
	return NewError(ErrorOperationFailed, operation, "", message, err)
}

func validErrorCode(code ErrorCode) bool {
	switch code {
	case ErrorInvalidArgument,
		ErrorNotFound,
		ErrorUnsupported,
		ErrorInvariantViolation,
		ErrorConflict,
		ErrorCancelled,
		ErrorOperationFailed:
		return true
	default:
		return false
	}
}
