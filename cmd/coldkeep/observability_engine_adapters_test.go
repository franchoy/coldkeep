package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
)

func TestEngineValueToAnyPreservesIntegerBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		integer string
		want    any
	}{
		{name: "zero", integer: "0", want: int64(0)},
		{name: "maximum int64", integer: "9223372036854775807", want: int64(9223372036854775807)},
		{name: "one above maximum int64", integer: "9223372036854775808", want: json.Number("9223372036854775808")},
		{name: "maximum uint64", integer: "18446744073709551615", want: json.Number("18446744073709551615")},
		{name: "one above maximum uint64", integer: "18446744073709551616", want: json.Number("18446744073709551616")},
		{name: "minimum int64", integer: "-9223372036854775808", want: int64(-9223372036854775808)},
		{name: "one below minimum int64", integer: "-9223372036854775809", want: json.Number("-9223372036854775809")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertEngineIntegerProjection(t, tc.integer, tc.want)
		})
	}
}

func assertEngineIntegerProjection(t *testing.T, integer string, want any) {
	t.Helper()
	value, err := engineValueToAny(engine.Value{Kind: engine.ValueInteger, Integer: integer})
	if err != nil {
		t.Fatalf("engineValueToAny: %v", err)
	}
	if !reflect.DeepEqual(value, want) {
		t.Fatalf("engineValueToAny type/value = %T(%v), want %T(%v)", value, value, want, want)
	}
	if got := fmt.Sprint(value); got != integer {
		t.Fatalf("human integer token changed: got=%q want=%q", got, integer)
	}

	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if got := string(encoded); got != integer {
		t.Fatalf("JSON integer token changed: got=%s want=%s", got, integer)
	}

	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatalf("decode JSON integer token: %v", err)
	}
	number, ok := decoded.(json.Number)
	if !ok || string(number) != integer {
		t.Fatalf("decoded JSON value = %T(%v), want json.Number(%q)", decoded, decoded, integer)
	}
}

func TestEngineValueToAnyPreservesNestedIntegerBoundaries(t *testing.T) {
	value, err := engineValueToAny(engine.Value{
		Kind: engine.ValueObject,
		Object: map[string]engine.Value{
			"array": {
				Kind: engine.ValueArray,
				Array: []engine.Value{
					{Kind: engine.ValueInteger, Integer: "0"},
					{Kind: engine.ValueInteger, Integer: "9223372036854775808"},
					{Kind: engine.ValueInteger, Integer: "18446744073709551615"},
					{Kind: engine.ValueInteger, Integer: "18446744073709551616"},
					{
						Kind: engine.ValueObject,
						Object: map[string]engine.Value{
							"below": {Kind: engine.ValueInteger, Integer: "-9223372036854775809"},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("engineValueToAny: %v", err)
	}
	want := map[string]any{
		"array": []any{
			int64(0),
			json.Number("9223372036854775808"),
			json.Number("18446744073709551615"),
			json.Number("18446744073709551616"),
			map[string]any{"below": json.Number("-9223372036854775809")},
		},
	}
	if !reflect.DeepEqual(value, want) {
		t.Fatalf("nested projection = %#v, want %#v", value, want)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if got, wantJSON := string(encoded), `{"array":[0,9223372036854775808,18446744073709551615,18446744073709551616,{"below":-9223372036854775809}]}`; got != wantJSON {
		t.Fatalf("nested JSON integer tokens changed: got=%s want=%s", got, wantJSON)
	}
}

func TestStatsEngineCompatibilityProjectionPreservesFields(t *testing.T) {
	projected := statsResultFromEngine(engine.StatsResult{
		Logical: engine.StatsLogical{TotalFiles: 3, TotalSizeBytes: 99},
		Chunks: engine.StatsChunks{
			CountsByVersion: map[string]int64{"v2-fastcdc": 2},
			ChunkerVersions: []engine.StatsVersion{{Version: "v2-fastcdc", Chunks: 2, Bytes: 99}},
		},
		Warnings: []engine.OperationWarning{{Code: "warning", Message: "detail"}},
	})
	if projected.Logical.TotalFiles != 3 || projected.Logical.TotalSizeBytes != 99 {
		t.Fatalf("logical projection changed: %+v", projected.Logical)
	}
	if projected.Chunks.CountsByVersion["v2-fastcdc"] != 2 || len(projected.Chunks.ChunkerVersions) != 1 {
		t.Fatalf("chunk projection changed: %+v", projected.Chunks)
	}
	if len(projected.Warnings) != 1 || projected.Warnings[0].Code != "warning" {
		t.Fatalf("warning projection changed: %+v", projected.Warnings)
	}
}
