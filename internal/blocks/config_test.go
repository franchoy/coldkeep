package blocks

import (
	"os"
	"strings"
	"testing"
)

func TestLoadDefaultCodec_DefaultIsAESGCM(t *testing.T) {
	prev, hadPrev := os.LookupEnv("COLDKEEP_CODEC")
	if err := os.Unsetenv("COLDKEEP_CODEC"); err != nil {
		t.Fatalf("unset COLDKEEP_CODEC: %v", err)
	}
	t.Cleanup(func() {
		if hadPrev {
			_ = os.Setenv("COLDKEEP_CODEC", prev)
		} else {
			_ = os.Unsetenv("COLDKEEP_CODEC")
		}
	})

	codec, err := LoadDefaultCodec()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if codec != CodecAESGCM {
		t.Fatalf("unexpected default codec: %s", codec)
	}
}

func TestLoadDefaultCodec_EnvOverride(t *testing.T) {
	t.Setenv("COLDKEEP_CODEC", "plain")

	codec, err := LoadDefaultCodec()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if codec != CodecPlain {
		t.Fatalf("unexpected codec from env: %s", codec)
	}
}

func TestLoadDefaultCodec_InvalidEnv(t *testing.T) {
	t.Setenv("COLDKEEP_CODEC", "invalid")

	_, err := LoadDefaultCodec()
	if err == nil || !strings.Contains(err.Error(), "COLDKEEP_CODEC") || !strings.Contains(err.Error(), "unsupported codec") {
		t.Fatalf("expected unsupported codec error, got: %v", err)
	}
}

func TestLoadDefaultCodec_WhitespaceEnvFailsDeterministically(t *testing.T) {
	t.Setenv("COLDKEEP_CODEC", "   ")

	_, err := LoadDefaultCodec()
	if err == nil || !strings.Contains(err.Error(), "COLDKEEP_CODEC") || !strings.Contains(err.Error(), "must not be empty") {
		t.Fatalf("expected whitespace env error with setting name, got: %v", err)
	}
}

func TestParseCodecAcceptsPlainAndAESGCM(t *testing.T) {
	plainCodec, err := ParseCodec("plain")
	if err != nil {
		t.Fatalf("parse plain codec: %v", err)
	}
	if plainCodec != CodecPlain {
		t.Fatalf("expected plain codec, got %s", plainCodec)
	}

	aesCodec, err := ParseCodec("aes-gcm")
	if err != nil {
		t.Fatalf("parse aes-gcm codec: %v", err)
	}
	if aesCodec != CodecAESGCM {
		t.Fatalf("expected aes-gcm codec, got %s", aesCodec)
	}
}

func TestParseCodecRejectsCaseMismatchedValue(t *testing.T) {
	_, err := ParseCodec("AES-GCM")
	if err == nil || !strings.Contains(err.Error(), "unsupported codec") {
		t.Fatalf("expected unsupported codec contract, got: %v", err)
	}
}
