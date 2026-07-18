package version

import "fmt"

const (
	Major = 1
	Minor = 13
	Patch = 9
)

func String() string {
	return fmt.Sprintf("%d.%d.%d", Major, Minor, Patch)
}
