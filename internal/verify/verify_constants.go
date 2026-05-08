package verify

type VerifyLevel int

const (
	VerifyFast VerifyLevel = iota
	VerifyStandard
	VerifyFull
	VerifyDeep
)

// BlockVerifyStage identifies one layer in the staged block payload
// verification pipeline introduced in Step 1.6.
//
// Three layers are defined, from outermost to innermost:
//
// StagePhysicalPayload   — raw bytes on disk (physical layer)
// StageCompressedPayload — decompressed but still-encrypted bytes (transform layer)
// StageLogicalPayload    — decrypted plaintext block bytes (logical layer)
//
// Only StageLogicalPayload is active today. The other two stages are
// explicit no-ops reserved as future insertion points for compressed-hash
// and physical-hash verification once those transforms are activated (Phase 3+).
type BlockVerifyStage int

const (
	// StagePhysicalPayload verifies the raw stored bytes on disk.
	// Future: compare sha256(storedBytes) against storage_blocks.physical_hash.
	// Current: no-op (physical_hash column exists but is NULL for all rows).
	StagePhysicalPayload BlockVerifyStage = iota

	// StageCompressedPayload verifies the compressed-but-not-yet-encrypted payload.
	// Future: compare sha256(compressedBytes) against storage_blocks.compressed_hash.
	// Current: no-op (compressed_hash column exists but is NULL; compression disabled).
	StageCompressedPayload

	// StageLogicalPayload verifies the decrypted plaintext encoded block bytes.
	// This is the currently active hash check: sha256(plaintextEncoded) == block_hash.
	StageLogicalPayload
)
