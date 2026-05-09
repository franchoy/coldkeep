package storage

import (
	"context"

	repositorycaps "github.com/franchoy/coldkeep/internal/repository/capabilities"
)

// RepositoryCapabilities is the internal capability surface used by storage
// and upcoming engine extraction integration points.
type RepositoryCapabilities = repositorycaps.RepositoryCapabilities

// GetRepositoryCapabilities centralizes repository capability introspection for
// internal callers. Errors degrade to safe defaults so internal call sites can
// remain simple during extraction refactors.
func GetRepositoryCapabilities(repo *Repository) RepositoryCapabilities {
	caps, err := GetRepositoryCapabilitiesWithError(repo)
	if err != nil {
		return repositorycaps.DefaultRepositoryCapabilities()
	}
	return caps
}

// GetRepositoryCapabilitiesWithError is the strict variant for internal call
// sites that want introspection failures surfaced explicitly.
func GetRepositoryCapabilitiesWithError(repo *Repository) (RepositoryCapabilities, error) {
	if repo == nil || repo.DB() == nil {
		return repositorycaps.DefaultRepositoryCapabilities(), nil
	}
	return repositorycaps.Derive(context.Background(), repo.DB())
}
