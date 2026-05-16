package utils_env

import (
	"strings"
	"testing"
)

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
	assertParseRequiredInt64OK(t, "COLDKEEP_TEST_INT", "42", 42)

	assertParseRequiredInt64ErrorContains(t, "COLDKEEP_TEST_INT", "", "COLDKEEP_TEST_INT")
	assertParseRequiredInt64ErrorContains(t, "COLDKEEP_TEST_INT", "12x", "invalid integer value")
	assertParseRequiredInt64ErrorContains(t, "COLDKEEP_TEST_INT", "1\x00", "must not contain NUL")
}

func assertParseRequiredInt64OK(t *testing.T, name string, raw string, want int64) {
	t.Helper()

	got, err := ParseRequiredInt64(name, raw)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if got != want {
		t.Fatalf("expected %d, got %d", want, got)
	}
}

func assertParseRequiredInt64ErrorContains(t *testing.T, name string, raw string, wantSubstr string) {
	t.Helper()

	_, err := ParseRequiredInt64(name, raw)
	if err == nil || !strings.Contains(err.Error(), wantSubstr) {
		t.Fatalf("expected error containing %q, got: %v", wantSubstr, err)
	}
}
