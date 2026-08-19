package engine

import (
	"context"
	"errors"

	"github.com/franchoy/coldkeep/internal/catalog"
	"github.com/franchoy/coldkeep/internal/invariants"
)

// ErrorCode is the renderer- and backend-neutral classification of an engine
// operation failure. Human messages and public CLI exit behavior remain caller
// compatibility concerns.
type ErrorCode string

const (
	ErrorInvalidArgument    ErrorCode = "invalid_argument"
	ErrorNotFound           ErrorCode = "not_found"
	ErrorUnsupported        ErrorCode = "unsupported"
	ErrorInvariantViolation ErrorCode = "invariant_violation"
	ErrorVerificationFailed ErrorCode = "verification_failed"
	ErrorRecoveryFailed     ErrorCode = "recovery_failed"
	ErrorConflict           ErrorCode = "conflict"
	ErrorCancelled          ErrorCode = "cancelled"
	ErrorOperationFailed    ErrorCode = "operation_failed"
)

// Error is the stable typed engine failure. Its exported state is deliberately
// string-only and backend-neutral; cause remains private but is available to
// errors.Is/errors.As through Unwrap.
type Error struct {
	Code          ErrorCode
	Operation     string
	Message       string
	InvariantCode string
	cause         error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return e.Message
	}
	if e.cause != nil {
		return e.cause.Error()
	}
	return string(e.Code)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// NewError creates one typed error without changing the supplied human
// message. Invalid or empty codes fail closed to operation_failed.
func NewError(code ErrorCode, operation, message, invariantCode string, cause error) *Error {
	if !validErrorCode(code) {
		code = ErrorOperationFailed
	}
	if message == "" && cause != nil {
		message = cause.Error()
	}
	return &Error{
		Code:          code,
		Operation:     operation,
		Message:       message,
		InvariantCode: invariantCode,
		cause:         cause,
	}
}

// TranslateError applies deterministic engine-wide classification while
// preserving the original error message and chain. Existing engine Errors are
// returned unchanged.
func TranslateError(operation string, err error) error {
	if err == nil {
		return nil
	}
	var typed *Error
	if errors.As(err, &typed) {
		return err
	}

	code := ErrorOperationFailed
	invariantCode := ""
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		code = ErrorCancelled
	default:
		var catalogErr *catalog.Error
		if errors.As(err, &catalogErr) && catalogErr != nil {
			code = engineCodeFromCatalog(catalogErr.Code)
			invariantCode = catalogErr.Invariant
		} else if value, ok := invariants.Code(err); ok {
			code = ErrorInvariantViolation
			invariantCode = value
		}
	}
	return NewError(code, operation, err.Error(), invariantCode, err)
}

func engineCodeFromCatalog(code catalog.ErrorCode) ErrorCode {
	switch code {
	case catalog.ErrorInvalidArgument:
		return ErrorInvalidArgument
	case catalog.ErrorNotFound:
		return ErrorNotFound
	case catalog.ErrorUnsupported:
		return ErrorUnsupported
	case catalog.ErrorInvariantViolation:
		return ErrorInvariantViolation
	case catalog.ErrorConflict:
		return ErrorConflict
	case catalog.ErrorCancelled:
		return ErrorCancelled
	default:
		return ErrorOperationFailed
	}
}

// TranslateErrorAs applies a caller-selected semantic classification. Context
// cancellation and invariant errors still use their universal classifications.
func TranslateErrorAs(operation string, code ErrorCode, err error) error {
	if err == nil {
		return nil
	}
	var typed *Error
	if errors.As(err, &typed) {
		return err
	}
	if universal := CodeOf(err); universal == ErrorCancelled || universal == ErrorInvariantViolation {
		return TranslateError(operation, err)
	}
	invariantCode := ""
	if value, ok := invariants.Code(err); ok {
		invariantCode = value
	}
	return NewError(code, operation, err.Error(), invariantCode, err)
}

// CodeOf returns the typed or universally derivable classification. An empty
// result means the error has not crossed a typed engine boundary yet.
func CodeOf(err error) ErrorCode {
	if err == nil {
		return ""
	}
	var typed *Error
	if errors.As(err, &typed) {
		return typed.Code
	}
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return ErrorCancelled
	}
	if _, ok := invariants.Code(err); ok {
		return ErrorInvariantViolation
	}
	return ""
}

// IsCode reports whether err has the requested stable engine classification.
func IsCode(err error, code ErrorCode) bool {
	return code != "" && CodeOf(err) == code
}

// IsUnsupported reports the stable typed unsupported classification.
func IsUnsupported(err error) bool {
	return IsCode(err, ErrorUnsupported)
}

func validErrorCode(code ErrorCode) bool {
	switch code {
	case ErrorInvalidArgument,
		ErrorNotFound,
		ErrorUnsupported,
		ErrorInvariantViolation,
		ErrorVerificationFailed,
		ErrorRecoveryFailed,
		ErrorConflict,
		ErrorCancelled,
		ErrorOperationFailed:
		return true
	default:
		return false
	}
}
