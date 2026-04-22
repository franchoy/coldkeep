package chunk

import (
	"fmt"

	"github.com/franchoy/coldkeep/internal/chunk/simplecdc"
)

type Registry struct {
	byVersion map[Version]Chunker
	defaultV  Version
}

// NewRegistry constructs a Registry with the given default version and chunkers.
// It returns an error if:
//   - any chunker version is duplicated
//   - defaultV is not present among the provided chunkers
func NewRegistry(defaultV Version, chunkers ...Chunker) (*Registry, error) {
	byVersion := make(map[Version]Chunker, len(chunkers))
	for _, c := range chunkers {
		v := c.Version()
		if _, exists := byVersion[v]; exists {
			return nil, fmt.Errorf("duplicate chunker registration for version %q", v)
		}
		byVersion[v] = c
	}
	if _, ok := byVersion[defaultV]; !ok {
		return nil, fmt.Errorf("default version %q not present in registered chunkers", defaultV)
	}
	return &Registry{byVersion: byVersion, defaultV: defaultV}, nil
}

// Get returns the Chunker registered for version, and whether it was found.
func (r *Registry) Get(version Version) (Chunker, bool) {
	c, ok := r.byVersion[version]
	return c, ok
}

// MustGet returns the Chunker for version or panics if not registered.
func (r *Registry) MustGet(version Version) Chunker {
	c, ok := r.Get(version)
	if !ok {
		panic(fmt.Sprintf("chunk: chunker for version %q not registered", version))
	}
	return c
}

// Default returns the default Chunker for this registry.
func (r *Registry) Default() Chunker {
	return r.MustGet(r.defaultV)
}

// DefaultVersion returns the version identifier of the default chunker.
func (r *Registry) DefaultVersion() Version {
	return r.defaultV
}

// DefaultRegistry returns the package-level default registry.
func DefaultRegistry() *Registry {
	return defaultRegistry
}

// DefaultChunker returns the default Chunker from the package-level registry.
func DefaultChunker() Chunker {
	return defaultRegistry.Default()
}

var defaultRegistry = func() *Registry {
	r, err := NewRegistry(DefaultChunkerVersion, simplecdc.New())
	if err != nil {
		panic(fmt.Sprintf("chunk: failed to build default registry: %v", err))
	}
	return r
}()
