package utils_env

import "testing"

func TestGetenvOrDefault(t *testing.T) {
	const key = "COLDKEEP_TEST_GETENV_OR_DEFAULT"
	t.Setenv(key, "configured")
	if got := GetenvOrDefault(key, "fallback"); got != "configured" {
		t.Fatalf("GetenvOrDefault configured mismatch: got=%q", got)
	}

	const missing = "COLDKEEP_TEST_GETENV_OR_DEFAULT_MISSING"
	if got := GetenvOrDefault(missing, "fallback"); got != "fallback" {
		t.Fatalf("GetenvOrDefault fallback mismatch: got=%q", got)
	}
}

func TestGetenvOrDefaultInt64(t *testing.T) {
	const key = "COLDKEEP_TEST_GETENV_OR_DEFAULT_INT64"
	t.Setenv(key, "123")
	if got := GetenvOrDefaultInt64(key, 7); got != 123 {
		t.Fatalf("GetenvOrDefaultInt64 configured mismatch: got=%d", got)
	}

	t.Setenv(key, "not-an-int")
	if got := GetenvOrDefaultInt64(key, 7); got != 7 {
		t.Fatalf("GetenvOrDefaultInt64 invalid fallback mismatch: got=%d", got)
	}
}
