package catalog_test

import (
	"errors"
	"testing"

	"github.com/franchoy/coldkeep/internal/catalog"
)

func TestCatalogErrorCauseIsPrivateButPreservesErrorsIs(t *testing.T) {
	cause := errors.New("backend unavailable")
	err := catalog.NewError(catalog.ErrorOperationFailed, "load metadata", "", "metadata load failed", cause)
	if !errors.Is(err, cause) {
		t.Fatalf("catalog error lost private cause: %v", err)
	}
}

func TestCatalogErrorRejectsUnknownCodeFailClosed(t *testing.T) {
	err := catalog.NewError(catalog.ErrorCode("invented"), "load metadata", "", "metadata load failed", nil)
	if err.Code != catalog.ErrorOperationFailed {
		t.Fatalf("unknown code should fail closed: %+v", err)
	}
}
