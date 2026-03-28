package blocks

import (
	"fmt"

	"github.com/franchoy/coldkeep/internal/utils_env"
)

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

// default to AES-GCM if not set
func LoadDefaultCodec() (Codec, error) {
	value := utils_env.GetenvOrDefault("COLDKEEP_CODEC", string(CodecPlain))
	return ParseCodec(value)
}
