package verify

type VerifyLevel int

const (
	VerifyFast VerifyLevel = iota
	VerifyStandard
	VerifyFull
	VerifyDeep
)

type VerifyStage string

const (
	VerifyStagePhysicalPayload VerifyStage = "physical_payload"
	VerifyStageDecrypt         VerifyStage = "decrypt"
	VerifyStageCompressedHash  VerifyStage = "compressed_hash"
	VerifyStageDecompress      VerifyStage = "decompress"
	VerifyStageLogicalHash     VerifyStage = "logical_hash"
	VerifyStageBlockDecode     VerifyStage = "block_decode"
	VerifyStageChunkRefs       VerifyStage = "chunk_refs"
	VerifyStageSnapshots       VerifyStage = "snapshots"
)
