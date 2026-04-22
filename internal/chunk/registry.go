package chunk

import (
	"fmt"
)

type Registry struct {
	chunkers       map[Version]Chunker
	defaultVersion Version
}

func NewRegistry(defaultVersion Version) *Registry {
	return &Registry{
		chunkers:       make(map[Version]Chunker),
		defaultVersion: defaultVersion,
	}
}

func (r *Registry) Register(chunker Chunker) error {
	if chunker == nil {
		return fmt.Errorf("chunker cannot be nil")
	}
	version := Version(chunker.Version())
	if version == "" {
		return fmt.Errorf("chunker version cannot be empty")
	}
	if _, exists := r.chunkers[version]; exists {
		return fmt.Errorf("chunker already registered for version %q", version)
	}
	r.chunkers[version] = chunker
	return nil
}

func (r *Registry) MustRegister(chunker Chunker) {
	if err := r.Register(chunker); err != nil {
		panic(err)
	}
}

func (r *Registry) Get(version Version) (Chunker, error) {
	chunker, ok := r.chunkers[version]
	if !ok {
		return nil, fmt.Errorf("unknown chunker version %q", version)
	}
	return chunker, nil
}

func (r *Registry) Default() (Chunker, error) {
	return r.Get(r.defaultVersion)
}

func DefaultRegistry() *Registry {
	return defaultRegistry
}

func DefaultChunker() Chunker {
	return defaultChunker
}

func ChunkerByVersion(version Version) (Chunker, error) {
	return defaultRegistry.Get(version)
}

func buildDefaultRegistry() *Registry {
	registry := NewRegistry(DefaultChunkerVersion)
	registry.MustRegister(newSimpleRollingChunker())
	return registry
}

var defaultRegistry = buildDefaultRegistry()

var defaultChunker = func() Chunker {
	chunker, err := defaultRegistry.Default()
	if err != nil {
		panic(err)
	}
	return chunker
}()
