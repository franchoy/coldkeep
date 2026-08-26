package blocks

import (
	"fmt"
	"os"
	"strings"

	"github.com/franchoy/coldkeep/internal/utils_env"
)

const (
	// DefaultPackedBlockTargetSizeBytes is the locked v1 packed-block target.
	DefaultPackedBlockTargetSizeBytes int64 = 1 << 20
	defaultPackedBlockTargetSizeMB    int64 = 1
)

// PackedBlockTargetWarning classifies Store-compatible fallback warnings.
type PackedBlockTargetWarning uint8

const (
	PackedBlockTargetWarningNone PackedBlockTargetWarning = iota
	PackedBlockTargetWarningInvalid
	PackedBlockTargetWarningUnsupported
	PackedBlockTargetWarningOverflow
)

// PackedBlockTargetResolution is the pure effective target and warning data
// shared by Store and Stats.
type PackedBlockTargetResolution struct {
	Bytes       int64
	Megabytes   int64
	Warning     PackedBlockTargetWarning
	Environment string
}

// ResolvePackedBlockTarget preserves the v1 Store environment precedence,
// parsing, supported-size set, and fallback semantics without producing logs.
func ResolvePackedBlockTarget() PackedBlockTargetResolution {
	const newEnvironment = "COLDKEEP_BLOCK_TARGET_SIZE_MB"
	const legacyEnvironment = "COLDKEEP_PACKED_BLOCK_SIZE_MIB"

	environment := legacyEnvironment
	if _, ok := os.LookupEnv(newEnvironment); ok {
		environment = newEnvironment
	}
	megabytes := utils_env.GetenvOrDefaultInt64(environment, defaultPackedBlockTargetSizeMB)
	resolution := PackedBlockTargetResolution{
		Bytes:       DefaultPackedBlockTargetSizeBytes,
		Megabytes:   megabytes,
		Environment: environment,
	}

	if megabytes <= 0 {
		resolution.Warning = PackedBlockTargetWarningInvalid
		return resolution
	}
	if megabytes != 1 && megabytes != 2 && megabytes != 3 {
		resolution.Warning = PackedBlockTargetWarningUnsupported
		return resolution
	}
	if megabytes > (1<<63-1)/(1<<20) {
		resolution.Warning = PackedBlockTargetWarningOverflow
		return resolution
	}
	resolution.Bytes = megabytes << 20
	return resolution
}

func ParseCodec(value string) (Codec, error) {
	switch Codec(value) {
	case CodecPlain:
		return CodecPlain, nil
	case CodecAESGCM:
		return CodecAESGCM, nil
	default:
		return "", fmt.Errorf("unsupported codec: %s", value)
	}
}

// LoadDefaultCodec resolves codec from env with a secure default.
// Precedence: env (COLDKEEP_CODEC) -> default (aes-gcm).
func LoadDefaultCodec() (Codec, error) {
	const envCodec = "COLDKEEP_CODEC"
	raw, isSet := os.LookupEnv(envCodec)
	if !isSet {
		return CodecAESGCM, nil
	}
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", fmt.Errorf("%s must not be empty", envCodec)
	}
	codec, err := ParseCodec(value)
	if err != nil {
		return "", fmt.Errorf("invalid %s value %q: %w", envCodec, raw, err)
	}
	return codec, nil
}
