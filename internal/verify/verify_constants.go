package verify

type VerifyLevel int

const (
	VerifyFast VerifyLevel = iota
	VerifyStandard
	VerifyFull
	VerifyDeep
)

func VerifyLevelString(v VerifyLevel) string {
	switch v {
	case VerifyFast:
		return "fast"
	case VerifyStandard:
		return "standard"
	case VerifyFull:
		return "full"
	case VerifyDeep:
		return "deep"
	default:
		return "unknown"
	}
}
