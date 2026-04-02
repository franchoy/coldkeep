package container

// ContainerWriter is the storage-context ownership boundary: callers can always
// finalize and release writer-owned resources, while append-oriented behavior is
// discovered through optional interfaces in internal/storage.
type ContainerWriter interface {
	FinalizeContainer() error
}
