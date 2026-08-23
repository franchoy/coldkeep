//go:build !linux && !darwin && !windows

package secureinstall

import "fmt"

func beginPlatform(Request) (nativePending, error) {
	return nil, fmt.Errorf("%w on this platform", ErrAtomicNoReplaceUnsupported)
}
