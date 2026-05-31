package version

import "fmt"

const (
	Major = 1
	Minor = 11
	Patch = 0
)

func String() string {
	return fmt.Sprintf("%d.%d.%d", Major, Minor, Patch)
}
