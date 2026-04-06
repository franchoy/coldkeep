package batch

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// LoadRawTargets collects targets from positional args and optional input file.
func LoadRawTargets(args []string, inputFile string) ([]RawTarget, error) {
	raw := make([]RawTarget, 0, len(args))
	for _, arg := range args {
		raw = append(raw, RawTarget{Value: arg, Source: "args"})
	}

	if inputFile == "" {
		return raw, nil
	}

	file, err := os.Open(inputFile)
	if err != nil {
		return nil, fmt.Errorf("open input file %q: %w", inputFile, err)
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		raw = append(raw, RawTarget{Value: line, Source: "input"})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read input file %q: %w", inputFile, err)
	}
	return raw, nil
}
