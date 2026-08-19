package render

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func TestToObjectMapPreservesExactJSONNumbers(t *testing.T) {
	t.Parallel()

	input := map[string]any{
		"numbers": map[string]int64{
			"zero":                 0,
			"one":                  1,
			"negative_one":         -1,
			"max_int32":            2147483647,
			"above_uint32":         4294967296,
			"max_safe_integer":     9007199254740991,
			"two_to_53":            9007199254740992,
			"beyond_safe_integer":  9007199254740993,
			"negative_beyond_safe": -9007199254740993,
			"max_int64":            math.MaxInt64,
		},
		"nested": map[string]any{
			"id": int64(9007199254740993),
		},
		"items": []any{
			map[string]any{"offset": int64(math.MaxInt64)},
		},
		"float_half":    0.5,
		"float_quarter": 1.25,
		"ordinary":      int64(42),
		"null":          nil,
		"boolean":       true,
		"string":        "unchanged",
	}

	converted, err := toObjectMap(input)
	if err != nil {
		t.Fatalf("toObjectMap: %v", err)
	}

	numbers, ok := converted["numbers"].(map[string]any)
	if !ok {
		t.Fatalf("numbers type=%T want map[string]any", converted["numbers"])
	}
	wantNumbers := map[string]string{
		"zero":                 "0",
		"one":                  "1",
		"negative_one":         "-1",
		"max_int32":            "2147483647",
		"above_uint32":         "4294967296",
		"max_safe_integer":     "9007199254740991",
		"two_to_53":            "9007199254740992",
		"beyond_safe_integer":  "9007199254740993",
		"negative_beyond_safe": "-9007199254740993",
		"max_int64":            "9223372036854775807",
	}
	for field, want := range wantNumbers {
		assertExactJSONNumber(t, numbers[field], want)
	}

	nested, ok := converted["nested"].(map[string]any)
	if !ok {
		t.Fatalf("nested type=%T want map[string]any", converted["nested"])
	}
	assertExactJSONNumber(t, nested["id"], "9007199254740993")

	items, ok := converted["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("items=%v (%T) want one-element array", converted["items"], converted["items"])
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("items[0] type=%T want map[string]any", items[0])
	}
	assertExactJSONNumber(t, item["offset"], "9223372036854775807")

	assertExactJSONNumber(t, converted["float_half"], "0.5")
	assertExactJSONNumber(t, converted["float_quarter"], "1.25")
	assertExactJSONNumber(t, converted["ordinary"], "42")
	if converted["null"] != nil {
		t.Fatalf("null=%v want nil", converted["null"])
	}
	if got, ok := converted["boolean"].(bool); !ok || !got {
		t.Fatalf("boolean=%v (%T) want true", converted["boolean"], converted["boolean"])
	}
	if got, ok := converted["string"].(string); !ok || got != "unchanged" {
		t.Fatalf("string=%v (%T) want unchanged", converted["string"], converted["string"])
	}

	remarshaled, err := json.Marshal(converted)
	if err != nil {
		t.Fatalf("remarshal converted object: %v", err)
	}
	for _, token := range []string{
		`"beyond_safe_integer":9007199254740993`,
		`"negative_beyond_safe":-9007199254740993`,
		`"max_int64":9223372036854775807`,
		`"id":9007199254740993`,
		`"offset":9223372036854775807`,
	} {
		if !strings.Contains(string(remarshaled), token) {
			t.Fatalf("remarshaled JSON missing exact token %q: %s", token, remarshaled)
		}
	}
}

func assertExactJSONNumber(t *testing.T, value any, want string) {
	t.Helper()

	number, ok := value.(json.Number)
	if !ok {
		t.Fatalf("value=%v type=%T want json.Number", value, value)
	}
	if got := number.String(); got != want {
		t.Fatalf("number=%q want %q", got, want)
	}
}
