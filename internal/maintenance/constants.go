package maintenance

const gcAdvisoryLockID = 847362

type VerifyLevel int

const (
	VerifyStandard VerifyLevel = iota
	VerifyFull
	VerifyDeep
)

func VerifyLevelString(v VerifyLevel) string {
	switch v {
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

const maxErrorsToPrint = 50
