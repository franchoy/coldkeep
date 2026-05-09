package capabilities

import "testing"

func TestRepositoryCapabilitiesSupportHelpers(t *testing.T) {
	caps := RepositoryCapabilities{
		SupportedCompression:   []string{"none", "zstd"},
		SupportedEncryption:    []string{"none", "aes-gcm"},
		SupportedPacking:       []string{"legacy-single", "packed-multi"},
		SupportsCompressedHash: true,
		SupportsPhysicalHash:   false,
	}

	if !caps.SupportsCompression("ZSTD") {
		t.Fatalf("expected zstd compression support")
	}
	if caps.SupportsCompression("gzip") {
		t.Fatalf("expected gzip compression to be unsupported")
	}

	if !caps.SupportsEncryption("plain") {
		t.Fatalf("expected plain to normalize to none encryption support")
	}
	if !caps.SupportsEncryption("aes-gcm") {
		t.Fatalf("expected aes-gcm encryption support")
	}
	if caps.SupportsEncryption("chacha20") {
		t.Fatalf("expected chacha20 encryption to be unsupported")
	}

	if !caps.SupportsPacking("packed-multi") {
		t.Fatalf("expected packed-multi packing support")
	}
	if caps.SupportsPacking("future-packed") {
		t.Fatalf("expected future-packed to be unsupported")
	}

	if !caps.SupportsHashLayer("logical") {
		t.Fatalf("expected logical hash layer support")
	}
	if !caps.SupportsHashLayer("compressed") {
		t.Fatalf("expected compressed hash layer support")
	}
	if caps.SupportsHashLayer("physical") {
		t.Fatalf("expected physical hash layer to be unsupported")
	}
	if caps.SupportsHashLayer("unknown") {
		t.Fatalf("expected unknown hash layer to be unsupported")
	}
}

func TestDefaultRepositoryCapabilitiesBaseline(t *testing.T) {
	caps := DefaultRepositoryCapabilities()

	if caps.DefaultCompression != CompressionNone {
		t.Fatalf("expected default compression %q, got %q", CompressionNone, caps.DefaultCompression)
	}
	if caps.DefaultCompressionLevel != 3 {
		t.Fatalf("expected default compression level 3, got %d", caps.DefaultCompressionLevel)
	}
	if caps.DefaultEncryption != EncryptionNone {
		t.Fatalf("expected default encryption %q, got %q", EncryptionNone, caps.DefaultEncryption)
	}
	if caps.DefaultPacking != PackingPackedMulti {
		t.Fatalf("expected default packing %q, got %q", PackingPackedMulti, caps.DefaultPacking)
	}
	if !caps.ReadPathMetadataDriven {
		t.Fatalf("expected metadata-driven read path to be true")
	}
}
