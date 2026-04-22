package shared

// Info is the chunk metadata shape shared by chunker implementations.
type Info struct {
	Hash string
	Size int64
	// optional (future-safe)
	Offset int64
}

// Result is one emitted chunk and its metadata.
type Result struct {
	Info Info
	Data []byte
}
