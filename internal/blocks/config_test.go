package blocks

import (
	"strings"
	"testing"
)

func TestLoadDefaultCodec_DefaultIsAESGCM(t *testing.T) {
	t.Setenv("COLDKEEP_CODEC", "")

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
	if err == nil || !strings.Contains(err.Error(), "unsupported codec") {
		t.Fatalf("expected unsupported codec error, got: %v", err)
	}
}
