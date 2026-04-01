package storage

import (
	"time"

	"github.com/franchoy/coldkeep/internal/utils_env"
)

var logicalFileWaitingtime = loadLogicalFileWaitingTime()

var chunkWaitingtime = loadChunkWaitingTime()

var maxClaimPollingWait = loadMaxClaimPollingWait()
var maxClaimWaitDuration = loadMaxClaimWaitDuration()

func loadLogicalFileWaitingTime() time.Duration {
	const defaultWait = 100 * time.Millisecond
	valueMs := utils_env.GetenvOrDefaultInt64("COLDKEEP_LOGICAL_FILE_WAIT_MS", int64(defaultWait/time.Millisecond))
	if valueMs <= 0 {
		return defaultWait
	}
	return time.Duration(valueMs) * time.Millisecond
}

func loadChunkWaitingTime() time.Duration {
	const defaultWait = 100 * time.Millisecond
	valueMs := utils_env.GetenvOrDefaultInt64("COLDKEEP_CHUNK_WAIT_MS", int64(defaultWait/time.Millisecond))
	if valueMs <= 0 {
		return defaultWait
	}
	return time.Duration(valueMs) * time.Millisecond
}

func loadMaxClaimPollingWait() time.Duration {
	const defaultPollingWait = 2 * time.Second
	valueMs := utils_env.GetenvOrDefaultInt64("COLDKEEP_MAX_CLAIM_POLL_WAIT_MS", int64(defaultPollingWait/time.Millisecond))
	if valueMs <= 0 {
		return defaultPollingWait
	}
	return time.Duration(valueMs) * time.Millisecond
}

func loadMaxClaimWaitDuration() time.Duration {
	const defaultWait = 2 * time.Minute
	valueMs := utils_env.GetenvOrDefaultInt64("COLDKEEP_MAX_CLAIM_WAIT_MS", int64(defaultWait/time.Millisecond))
	if valueMs <= 0 {
		return defaultWait
	}
	wait := time.Duration(valueMs) * time.Millisecond
	if wait < maxClaimPollingWait {
		return maxClaimPollingWait
	}
	return wait
}

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
