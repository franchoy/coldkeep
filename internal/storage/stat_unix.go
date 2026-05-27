//go:build !windows

package storage

import (
	"os"
	"syscall"
)

// extractUIDGID returns the Unix UID and GID from fileInfo if available.
func extractUIDGID(fileInfo os.FileInfo) (uid, gid int64, ok bool) {
	if fileInfo == nil {
		return 0, 0, false
	}
	if stat, statOK := fileInfo.Sys().(*syscall.Stat_t); statOK {
		return int64(stat.Uid), int64(stat.Gid), true
	}
	return 0, 0, false
}
