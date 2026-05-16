package utils_env

import "strings"
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

	t.Setenv(key, "123bad")
	if got := GetenvOrDefaultInt64(key, 7); got != 7 {
		t.Fatalf("GetenvOrDefaultInt64 partial-int fallback mismatch: got=%d", got)
	}

	t.Setenv(key, "  55  ")
	if got := GetenvOrDefaultInt64(key, 7); got != 55 {
		t.Fatalf("GetenvOrDefaultInt64 trimmed parse mismatch: got=%d", got)
	}
}

func TestParseRequiredInt64(t *testing.T) {
	got, err := ParseRequiredInt64("COLDKEEP_TEST_INT", "42")
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}

	_, err = ParseRequiredInt64("COLDKEEP_TEST_INT", "")
	if err == nil || !strings.Contains(err.Error(), "COLDKEEP_TEST_INT") {
		t.Fatalf("expected setting-name error for empty value, got: %v", err)
	}

	_, err = ParseRequiredInt64("COLDKEEP_TEST_INT", "12x")
	if err == nil || !strings.Contains(err.Error(), "invalid integer value") {
		t.Fatalf("expected invalid integer error, got: %v", err)
	}

	_, err = ParseRequiredInt64("COLDKEEP_TEST_INT", "1\x00")
	if err == nil || !strings.Contains(err.Error(), "must not contain NUL") {
		t.Fatalf("expected NUL rejection error, got: %v", err)
	}
}
