package batch

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// LoadOptions defines supported target input channels.
type LoadOptions struct {
	Args      []string
	InputFile string
	Patterns  []string
}

// LoadRawTargets collects raw targets from supported sources.
// Step 2 supports positional args and --input text files.
func LoadRawTargets(opts LoadOptions) ([]RawTarget, error) {
	if len(opts.Patterns) > 0 {
		return nil, fmt.Errorf("--pattern is not supported yet")
	}

	raw := make([]RawTarget, 0, len(opts.Args))
	for _, arg := range opts.Args {
		raw = append(raw, RawTarget{Value: arg, Source: TargetFromArgs})
	}

	if opts.InputFile == "" {
		return raw, nil
	}

	file, err := os.Open(opts.InputFile)
	if err != nil {
		return nil, fmt.Errorf("open input file %q: %w", opts.InputFile, err)
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		raw = append(raw, RawTarget{Value: line, Source: TargetFromInput})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read input file %q: %w", opts.InputFile, err)
	}
	return raw, nil
}
