package version

import "fmt"

const (
	Major = 1
	Minor = 10
	Patch = 13
)

func String() string {
	return fmt.Sprintf("%d.%d.%d", Major, Minor, Patch)
}
