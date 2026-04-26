package observability

import (
	"errors"
	"fmt"
)

var (
	ErrUnsupportedInspectTarget  = errors.New("unsupported inspect target")
	ErrUnsupportedSimulateTarget = errors.New("unsupported simulate target")
)

type EntityNotFoundError struct {
	EntityType EntityType
	EntityID   string
}

func (e EntityNotFoundError) Error() string {
	return fmt.Sprintf("%s %q not found", e.EntityType, e.EntityID)
}
