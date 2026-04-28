package render

import (
	"encoding/json"
	"sort"
	"time"

	"github.com/franchoy/coldkeep/internal/observability"
)

const cliJSONSchemaVersion = "v1.6"

type jsonEnvelope struct {
	GeneratedAtUTC string                             `json:"generated_at_utc"`
	Type           string                             `json:"type"`
	Data           map[string]any                     `json:"data"`
	Warnings       []observability.ObservationWarning `json:"warnings"`
	Meta           jsonEnvelopeMeta                   `json:"meta"`
}

type jsonEnvelopeMeta struct {
	Version string `json:"version"`
	Exact   bool   `json:"exact"`
}

func normalizeGeneratedAt(ts time.Time) string {
	if ts.IsZero() {
		return time.Time{}.UTC().Format(time.RFC3339)
	}
	return ts.UTC().Format(time.RFC3339)
}

func normalizeWarnings(in []observability.ObservationWarning) []observability.ObservationWarning {
	if len(in) == 0 {
		return []observability.ObservationWarning{}
	}
	out := append([]observability.ObservationWarning(nil), in...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Code == out[j].Code {
			return out[i].Message < out[j].Message
		}
		return out[i].Code < out[j].Code
	})
	return out
}

func toObjectMap(v any) (map[string]any, error) {
	encoded, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	var out map[string]any
	if err := json.Unmarshal(encoded, &out); err != nil {
		return nil, err
	}
	if out == nil {
		out = map[string]any{}
	}
	return out, nil
}
