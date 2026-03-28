package storage

import (
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
)

func TestStoreFileWithDBAndCodec_AESGCMMissingKeyFailsEarly(t *testing.T) {
	t.Setenv("COLDKEEP_KEY", "")

	err := StoreFileWithDBAndCodec(nil, "does-not-matter.txt", blocks.CodecAESGCM)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "encryption key required for aes-gcm") {
		t.Fatalf("unexpected error: %v", err)
	}
}
