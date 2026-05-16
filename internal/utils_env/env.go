package utils_env

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func GetenvOrDefault(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}

func GetenvOrDefaultInt64(key string, fallback int64) int64 {
	if val, ok := os.LookupEnv(key); ok {
		if result, err := ParseRequiredInt64(key, val); err == nil {
			return result
		}
	}
	return fallback
}

func ParseRequiredInt64(settingName, raw string) (int64, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0, fmt.Errorf("%s must not be empty", settingName)
	}
	if strings.ContainsRune(value, '\x00') {
		return 0, fmt.Errorf("%s must not contain NUL byte", settingName)
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s has invalid integer value %q", settingName, raw)
	}
	return parsed, nil
}
