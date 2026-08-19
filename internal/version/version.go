package version

import "fmt"

const (
	Major = 1
	Minor = 13
	Patch = 11
)

func String() string {
	return fmt.Sprintf("%d.%d.%d", Major, Minor, Patch)
}
