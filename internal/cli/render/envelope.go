package render

import (
	"encoding/json"
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
		ts = time.Now().UTC()
	}
	return ts.UTC().Format(time.RFC3339)
}

func normalizeWarnings(in []observability.ObservationWarning) []observability.ObservationWarning {
	if len(in) == 0 {
		return []observability.ObservationWarning{}
	}
	return append([]observability.ObservationWarning(nil), in...)
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
