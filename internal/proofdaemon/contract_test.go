package proofdaemon

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestProofDTOContract(t *testing.T) {
	raw, e := os.ReadFile("../../docs/release/v2.0/proof-openapi.json")
	if e != nil {
		t.Fatal(e)
	}
	var doc struct {
		Components struct {
			Schemas map[string]struct {
				Required   []string `json:"required"`
				Properties map[string]struct {
					Type string `json:"type"`
				} `json:"properties"`
			} `json:"schemas"`
		} `json:"components"`
	}
	if e = json.Unmarshal(raw, &doc); e != nil {
		t.Fatal(e)
	}
	for _, v := range []any{Problem{}, BootstrapRequest{}, SessionResult{}, HistoryEntry{}, HistoryResult{}, RestoreRequest{}, RestoreResult{}, Capabilities{}, Status{}} {
		typ := reflect.TypeOf(v)
		schema, ok := doc.Components.Schemas[typ.Name()]
		if !ok || len(schema.Properties) != typ.NumField() || len(schema.Required) != typ.NumField() {
			t.Fatalf("missing/extra DTO fields for %s", typ.Name())
		}
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			name := f.Tag.Get("json")
			property, ok := schema.Properties[name]
			if !ok {
				t.Fatalf("missing %s.%s", typ.Name(), name)
			}
			kind := map[reflect.Kind]string{reflect.String: "string", reflect.Bool: "boolean", reflect.Int: "integer", reflect.Slice: "array"}[f.Type.Kind()]
			if kind != property.Type {
				t.Fatalf("type drift %s.%s", typ.Name(), name)
			}
		}
	}
	t.Log("PROOF_EVENT go_dto_openapi_agreement")
}
