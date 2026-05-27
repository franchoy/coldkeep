//go:build windows

package storage

import "os"

// extractUIDGID returns no UID/GID on Windows; POSIX ownership is not available.
func extractUIDGID(_ os.FileInfo) (uid, gid int64, ok bool) {
	return 0, 0, false
}
