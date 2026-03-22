package storage

import "time"

var logicalFileWaitingtime = 100 * time.Millisecond

var chunkWaitingtime = 100 * time.Millisecond

var containerLockRetryAttempts = 3

var containerLockRetryWait = 25 * time.Millisecond
