package observability

import "errors"

var (
	ErrUnsupportedEntity = errors.New("unsupported observability entity")
	ErrNotFound          = errors.New("observability target not found")
	ErrInvalidTarget     = errors.New("invalid observability target")
)
