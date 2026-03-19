package utils_env

import (
	"fmt"
	"os"
)

func GetenvOrDefault(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}

func GetenvOrDefaultInt64(key string, fallback int64) int64 {
	if val, ok := os.LookupEnv(key); ok {
		var result int64
		if _, err := fmt.Sscanf(val, "%d", &result); err == nil {
			return result
		}
	}
	return fallback
}
