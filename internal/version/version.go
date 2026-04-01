package version

import "fmt"

const (
	Major = 0
	Minor = 9
	Patch = 0
)

func String() string {
	return fmt.Sprintf("%d.%d.%d", Major, Minor, Patch)
}
