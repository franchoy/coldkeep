package chunk

type Info struct {
	Hash string
	Size int64
	// optional (future-safe)
	Offset int64
}
