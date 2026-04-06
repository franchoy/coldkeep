package batch

import "fmt"

// LoadOptions defines supported target input channels.
type LoadOptions struct {
	Args      []string
	InputFile string
	Patterns  []string
}

// LoadRawTargets collects raw targets from supported sources.
// Step 1 scope supports positional args only.
func LoadRawTargets(opts LoadOptions) ([]RawTarget, error) {
	if opts.InputFile != "" {
		return nil, fmt.Errorf("--input is not supported yet")
	}
	if len(opts.Patterns) > 0 {
		return nil, fmt.Errorf("--pattern is not supported yet")
	}

	raw := make([]RawTarget, 0, len(opts.Args))
	for _, arg := range opts.Args {
		raw = append(raw, RawTarget{Value: arg, Source: TargetFromArgs})
	}
	return raw, nil
}
