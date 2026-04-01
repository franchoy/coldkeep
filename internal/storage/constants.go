package storage

import "time"

var logicalFileWaitingtime = 100 * time.Millisecond

var chunkWaitingtime = 100 * time.Millisecond

var maxClaimPollingWait = 2 * time.Second
var maxClaimWaitDuration = 2 * time.Minute

func claimPollingBackoff(base time.Duration, attempt int) time.Duration {
	if base <= 0 {
		base = 100 * time.Millisecond
	}
	if attempt <= 0 {
		if base > maxClaimPollingWait {
			return maxClaimPollingWait
		}
		return base
	}

	wait := base
	for i := 0; i < attempt; i++ {
		if wait >= maxClaimPollingWait/2 {
			return maxClaimPollingWait
		}
		wait *= 2
	}

	if wait > maxClaimPollingWait {
		return maxClaimPollingWait
	}

	return wait
}
