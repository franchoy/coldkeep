package container

import (
	"github.com/franchoy/coldkeep/internal/utils_env"
)

var ContainersDir = utils_env.GetenvOrDefault("COLDKEEP_STORAGE_DIR", "./storage/containers")

var containerMaxSize = utils_env.GetenvOrDefaultInt64("COLDKEEP_CONTAINER_MAX_SIZE_MB", 64) * 1024 * 1024 //MB

// GetContainerMaxSize returns the current container max size
func GetContainerMaxSize() int64 {
	return containerMaxSize
}

// SetContainerMaxSize sets the container max size (for testing)
func SetContainerMaxSize(size int64) {
	containerMaxSize = size
}
