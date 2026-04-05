package blocks

import (
	"strings"
	"testing"
)

func TestGetBlockTransformerFailsOnUnknownCodec(t *testing.T) {
	_, err := GetBlockTransformer("codec-unknown")
	if err == nil || !strings.Contains(err.Error(), "unknown codec") {
		t.Fatalf("expected unknown-codec error contract, got: %v", err)
	}
}

func TestGetBlockTransformerFailsForAESGCMWhenKeyNotSet(t *testing.T) {
	t.Setenv("COLDKEEP_KEY", "")

	_, err := GetBlockTransformer(CodecAESGCM)
	if err == nil || !strings.Contains(err.Error(), "aes-gcm requires COLDKEEP_KEY") {
		t.Fatalf("expected aes-gcm key-required error contract, got: %v", err)
	}
}

func TestGetBlockTransformerReturnsPlainTransformerForPlainCodec(t *testing.T) {
	transformer, err := GetBlockTransformer(CodecPlain)
	if err != nil {
		t.Fatalf("expected plain transformer, got error: %v", err)
	}
	if _, ok := transformer.(*PlainTransformer); !ok {
		t.Fatalf("expected *PlainTransformer, got %T", transformer)
	}
}

func TestGetBlockTransformerReturnsAESGCMTransformerWhenKeyIsSet(t *testing.T) {
	t.Setenv("COLDKEEP_KEY", strings.Repeat("ab", 32)) // 32 bytes = 64 hex chars

	transformer, err := GetBlockTransformer(CodecAESGCM)
	if err != nil {
		t.Fatalf("expected aes-gcm transformer, got error: %v", err)
	}
	if _, ok := transformer.(*AESGCMTransformer); !ok {
		t.Fatalf("expected *AESGCMTransformer, got %T", transformer)
	}
}
