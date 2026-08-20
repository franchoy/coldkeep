package render

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/observability"
)

func TestRenderInspectHumanChunkExampleLayout(t *testing.T) {
	r := &InspectResult{
		EntityType: observability.EntityChunk,
		EntityID:   "123",
		Summary: map[string]any{
			"size_bytes":      int64(4096),
			"chunker_version": "v2-fastcdc",
			"container_id":    int64(2),
		},
		Relations: []observability.Relation{
			{
				Type:       "referenced_by",
				Direction:  observability.RelationIncoming,
				TargetType: observability.EntityLogicalFile,
				TargetID:   "45",
			},
		},
	}

	var buf bytes.Buffer
	if err := (HumanRenderer{}).RenderInspect(&buf, r); err != nil {
		t.Fatalf("RenderInspectHuman: %v", err)
	}
	out := buf.String()

	for _, want := range []string{
		"Inspect chunk 123",
		"Summary",
		"size:",
		"4.0 KiB",
		"chunker_version:",
		"v2-fastcdc",
		"container:",
		"2",
		"Referenced by",
		"relation: logical file 45",
		"45",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestRenderInspectHumanLogicalFileExampleLayout(t *testing.T) {
	r := &InspectResult{
		EntityType: observability.EntityLogicalFile,
		EntityID:   "45",
		Summary: map[string]any{
			"original_name":        "photo.jpg",
			"chunk_count":          int64(12),
			"chunker_version":      "v2-fastcdc",
			"avg_chunk_size_bytes": 2048.0,
		},
		Relations: []observability.Relation{
			{Type: "references", Direction: observability.RelationOutgoing, TargetType: observability.EntityChunk, TargetID: "123"},
			{Type: "references", Direction: observability.RelationOutgoing, TargetType: observability.EntityChunk, TargetID: "124"},
			{Type: "referenced_by", Direction: observability.RelationIncoming, TargetType: observability.EntitySnapshot, TargetID: "10"},
			{Type: "referenced_by", Direction: observability.RelationIncoming, TargetType: observability.EntitySnapshot, TargetID: "11"},
		},
	}

	var buf bytes.Buffer
	if err := (HumanRenderer{}).RenderInspect(&buf, r); err != nil {
		t.Fatalf("RenderInspectHuman: %v", err)
	}
	out := buf.String()

	for _, want := range []string{
		"Inspect logical file 45",
		"Summary",
		"name:",
		"photo.jpg",
		"chunks:",
		"12",
		"chunker_version:",
		"v2-fastcdc",
		"References",
		"relation: chunk 123",
		"123",
		"124",
		"Referenced by",
		"relation: snapshot 10",
		"10",
		"11",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestRenderInspectJSONIsDeterministic(t *testing.T) {
	r := &InspectResult{
		EntityType: observability.EntityChunk,
		EntityID:   "123",
		Relations: []observability.Relation{
			{Type: "referenced_by", Direction: observability.RelationIncoming, TargetType: observability.EntitySnapshot, TargetID: "11"},
			{Type: "referenced_by", Direction: observability.RelationIncoming, TargetType: observability.EntitySnapshot, TargetID: "10"},
		},
		Warnings: []observability.ObservationWarning{
			{Code: "B", Message: "b"},
			{Code: "A", Message: "a"},
		},
	}

	var first bytes.Buffer
	if err := (JSONRenderer{}).RenderInspect(&first, r); err != nil {
		t.Fatalf("RenderInspectJSON first: %v", err)
	}
	var second bytes.Buffer
	if err := (JSONRenderer{}).RenderInspect(&second, r); err != nil {
		t.Fatalf("RenderInspectJSON second: %v", err)
	}

	if first.String() != second.String() {
		t.Fatalf("expected deterministic inspect JSON output\nfirst:\n%s\nsecond:\n%s", first.String(), second.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(first.Bytes(), &payload); err != nil {
		t.Fatalf("decode inspect json: %v", err)
	}
	data := payload["data"].(map[string]any)
	relations := data["relations"].([]any)
	firstRel := relations[0].(map[string]any)
	if got, _ := firstRel["target_id"].(string); got != "10" {
		t.Fatalf("expected sorted relations by target_id, got %v", got)
	}
}

func TestToInt64UnsignedBoundaries(t *testing.T) {
	tests := []struct {
		name   string
		value  any
		want   int64
		wantOK bool
	}{
		{name: "normal uint", value: uint(42), want: 42, wantOK: true},
		{name: "maximum int64 as uint64", value: uint64(math.MaxInt64), want: math.MaxInt64, wantOK: true},
		{name: "one above maximum int64 as uint64", value: uint64(math.MaxInt64) + 1, wantOK: false},
		{name: "maximum uint64", value: uint64(math.MaxUint64), wantOK: false},
	}
	if strconv.IntSize == 64 {
		tests = append(tests, struct {
			name   string
			value  any
			want   int64
			wantOK bool
		}{name: "one above maximum int64 as uint", value: uint(uint64(math.MaxInt64) + 1), wantOK: false})
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := toInt64(tc.value)
			if got != tc.want || ok != tc.wantOK {
				t.Fatalf("toInt64(%T(%v)) = (%d, %t), want (%d, %t)", tc.value, tc.value, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

func TestToInt64FloatBoundaries(t *testing.T) {
	const (
		minInt64Float = -9223372036854775808.0
		maxInt64Float = 9223372036854775808.0
	)
	largestBelowMax := math.Nextafter(maxInt64Float, 0)
	tests := []struct {
		name   string
		value  any
		want   int64
		wantOK bool
	}{
		{name: "float64 fractional positive truncates", value: float64(42.75), want: 42, wantOK: true},
		{name: "float64 fractional negative truncates", value: float64(-42.75), want: -42, wantOK: true},
		{name: "float64 minimum inclusive", value: float64(minInt64Float), want: math.MinInt64, wantOK: true},
		{name: "float64 largest representable below maximum", value: largestBelowMax, want: math.MaxInt64 - 1023, wantOK: true},
		{name: "float64 maximum exclusive", value: float64(maxInt64Float), wantOK: false},
		{name: "float64 below minimum", value: math.Nextafter(minInt64Float, math.Inf(-1)), wantOK: false},
		{name: "float64 positive infinity", value: math.Inf(1), wantOK: false},
		{name: "float64 negative infinity", value: math.Inf(-1), wantOK: false},
		{name: "float64 NaN", value: math.NaN(), wantOK: false},
		{name: "float32 fractional positive truncates", value: float32(42.75), want: 42, wantOK: true},
		{name: "float32 fractional negative truncates", value: float32(-42.75), want: -42, wantOK: true},
		{name: "float32 minimum inclusive", value: float32(minInt64Float), want: math.MinInt64, wantOK: true},
		{name: "float32 below minimum", value: math.Nextafter32(float32(minInt64Float), float32(math.Inf(-1))), wantOK: false},
		{name: "float32 maximum exclusive", value: float32(maxInt64Float), wantOK: false},
		{name: "float32 positive infinity", value: float32(math.Inf(1)), wantOK: false},
		{name: "float32 NaN", value: float32(math.NaN()), wantOK: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := toInt64(tc.value)
			if got != tc.want || ok != tc.wantOK {
				t.Fatalf("toInt64(%T(%v)) = (%d, %t), want (%d, %t)", tc.value, tc.value, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

func TestSummaryValueStringFloatSafetyAndCompatibility(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value any
		want  string
	}{
		{name: "fractional human summary retains truncation", key: "compression_factor", value: 2.75, want: "2"},
		{name: "fractional byte value retains truncation", key: "size_bytes", value: 2048.75, want: "2.0 KiB"},
		{name: "positive infinity is not narrowed", key: "compression_factor", value: math.Inf(1), want: "+Inf"},
		{name: "negative infinity is not narrowed", key: "compression_factor", value: math.Inf(-1), want: "-Inf"},
		{name: "NaN is not narrowed", key: "compression_factor", value: math.NaN(), want: "NaN"},
		{name: "exclusive upper bound is not narrowed", key: "compression_factor", value: float64(9223372036854775808.0), want: "9223372036854775808.00"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := summaryValueString(tc.key, tc.value); got != tc.want {
				t.Fatalf("summaryValueString(%q, %T(%v)) = %q, want %q", tc.key, tc.value, tc.value, got, tc.want)
			}
		})
	}
}

func TestSummaryValueStringOversizedUnsignedDoesNotNarrow(t *testing.T) {
	value := uint64(math.MaxInt64) + 1
	if got, want := summaryValueString("chunk_count", value), strconv.FormatUint(value, 10); got != want {
		t.Fatalf("summaryValueString() = %q, want exact unsigned fallback %q", got, want)
	}
	if got := summaryValueString("chunk_count", uint64(math.MaxUint64)); strings.HasPrefix(got, "-") {
		t.Fatalf("maximum uint64 rendered as negative value: %q", got)
	}
}
