package invariants

import "errors"

const (
	CodePhysicalGraphIntegrity        = "PHYSICAL_GRAPH_INTEGRITY"
	CodePhysicalGraphOrphan           = "PHYSICAL_GRAPH_ORPHAN"
	CodePhysicalGraphRefCountMismatch = "PHYSICAL_GRAPH_REFCOUNT_MISMATCH"
	CodePhysicalGraphNegativeRefCount = "PHYSICAL_GRAPH_NEGATIVE_REFCOUNT"
	CodeGCRefusedIntegrity            = "GC_REFUSED_INTEGRITY"
	CodeRepairRefusedOrphanRows       = "REPAIR_REFUSED_ORPHAN_ROWS"
)

// Error provides stable machine-readable classification for invariant failures
// while preserving human-readable messages.
type Error struct {
	Code    string
	Message string
	Err     error
}

func (e *Error) Error() string {
	if e.Message != "" {
		return e.Message
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return ""
}

func (e *Error) Unwrap() error {
	return e.Err
}

func New(code, message string, err error) error {
	return &Error{Code: code, Message: message, Err: err}
}

func Code(err error) (string, bool) {
	if err == nil {
		return "", false
	}

	var invErr *Error
	if !errors.As(err, &invErr) {
		return "", false
	}
	if invErr.Code == "" {
		return "", false
	}

	return invErr.Code, true
}

func RecommendedActionForCode(code string) string {
	switch code {
	case CodePhysicalGraphIntegrity,
		CodePhysicalGraphOrphan,
		CodePhysicalGraphRefCountMismatch,
		CodePhysicalGraphNegativeRefCount,
		CodeGCRefusedIntegrity:
		return "coldkeep repair ref-counts; then rerun coldkeep verify"
	case CodeRepairRefusedOrphanRows:
		return "investigate orphan physical_file rows; repair ref-counts is blocked until orphans are resolved"
	default:
		return ""
	}
}

func RecommendedActionForError(err error) string {
	code, ok := Code(err)
	if !ok {
		return ""
	}
	return RecommendedActionForCode(code)
}
