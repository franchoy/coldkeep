package chunk

type Version string

const (
	VersionSimpleRolling Version = "v1-simple-rolling"
	DefaultVersion       Version = VersionSimpleRolling
)
