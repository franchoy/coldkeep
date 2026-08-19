package engine

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"

	"github.com/franchoy/coldkeep/internal/observability"
)

func TestValueFromAnyPreservesExactNumericKinds(t *testing.T) {
	tests := []struct {
		name  string
		input any
		want  Value
	}{
		{name: "signed", input: int64(math.MaxInt64), want: Value{Kind: ValueInteger, Integer: "9223372036854775807"}},
		{name: "unsigned", input: uint64(math.MaxUint64), want: Value{Kind: ValueInteger, Integer: "18446744073709551615"}},
		{name: "decimal", input: 1.25, want: Value{Kind: ValueDecimal, Decimal: "1.25"}},
		{name: "json integer", input: json.Number("9007199254740993"), want: Value{Kind: ValueInteger, Integer: "9007199254740993"}},
		{name: "json decimal", input: json.Number("1.125"), want: Value{Kind: ValueDecimal, Decimal: "1.125"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := valueFromAny(tc.input)
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("valueFromAny(%T): got (%+v, %v), want %+v", tc.input, got, err, tc.want)
			}
		})
	}
}

func TestValueFromAnyPreservesNestedObjectsAndArrays(t *testing.T) {
	got, err := valueFromAny(map[string]any{
		"enabled": true,
		"items":   []any{"alpha", int64(7), nil},
		"counts":  map[string]int64{"chunks": 3},
	})
	if err != nil {
		t.Fatalf("valueFromAny: %v", err)
	}
	if got.Kind != ValueObject || got.Object["enabled"].Kind != ValueBoolean {
		t.Fatalf("object conversion mismatch: %+v", got)
	}
	items := got.Object["items"]
	if items.Kind != ValueArray || len(items.Array) != 3 || items.Array[2].Kind != ValueNull {
		t.Fatalf("array conversion mismatch: %+v", items)
	}
	if got.Object["counts"].Object["chunks"].Integer != "3" {
		t.Fatalf("nested integer mismatch: %+v", got)
	}
}

func TestValueFromAnyRejectsNonFiniteAndUnsupportedValues(t *testing.T) {
	for _, input := range []any{math.NaN(), math.Inf(1), make(chan int)} {
		if _, err := valueFromAny(input); err == nil {
			t.Fatalf("expected %T to be rejected", input)
		}
	}
}

func TestTraceCollectorPreservesOrderAndNeutralMetadata(t *testing.T) {
	collector := &observabilityTraceCollector{}
	collector.Event(observability.TraceEvent{Step: "first", Metadata: map[string]any{"count": int64(9007199254740993)}})
	collector.Event(observability.TraceEvent{Step: "second", Metadata: map[string]any{"ok": true}})
	if collector.err != nil {
		t.Fatalf("collector error: %v", collector.err)
	}
	if len(collector.events) != 2 || collector.events[0].Step != "first" || collector.events[1].Step != "second" {
		t.Fatalf("trace order changed: %+v", collector.events)
	}
	if got := collector.events[0].Metadata["count"]; got.Kind != ValueInteger || got.Integer != "9007199254740993" {
		t.Fatalf("trace integer changed: %+v", got)
	}
}
