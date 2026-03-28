package blocks

import (
	"fmt"
	"strings"

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

// LoadDefaultCodec resolves codec from env with a secure default.
// Precedence: env (COLDKEEP_CODEC) -> default (aes-gcm).
func LoadDefaultCodec() (Codec, error) {
	value := utils_env.GetenvOrDefault("COLDKEEP_CODEC", string(CodecAESGCM))
	if strings.TrimSpace(value) == "" {
		value = string(CodecAESGCM)
	}
	return ParseCodec(value)
}
