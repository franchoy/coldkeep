package blocks

import (
	"fmt"
	"os"
	"strings"
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
