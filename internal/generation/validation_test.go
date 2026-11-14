package generation

import "testing"

// Escape scanning must preserve literal backslash-u and reject incomplete pairs
// before encoding/json can replace them. JSON syntax is a separate admission.
func TestGenerationUnicodeBoundaries(t *testing.T) {
	cases := []struct {
		raw   string
		valid bool
	}{
		{`"\ud800\udc00"`, true}, {`"\udbff\udfff"`, true},
		{`"\\ud800"`, true}, {`"\u0000"`, true}, {`"�"`, true},
		{`"\ud800"`, false}, {`"\ud800\u0041"`, false},
		{`"\udc00"`, false}, {`"\udfff"`, false}, {`"\u123"`, false},
		{`"\ud800\u"`, false}, {`"\uxxxx"`, false}, {"\"\xff\"", false},
	}
	for _, c := range cases {
		if losslessUnicode([]byte(c.raw)) != c.valid {
			t.Fatalf("Unicode admission: %q", c.raw)
		}
	}
}

// Refactored predicates must retain nil short circuits and absent/empty scope.
func TestGenerationValidationShortCircuit(t *testing.T) {
	empty := ""
	id := readIDs("entry")
	cases := []struct {
		a, b  *string
		equal bool
	}{
		{nil, nil, true}, {nil, &empty, false}, {&empty, nil, false},
		{&empty, &empty, true}, {&id, &empty, false}, {&id, &id, true},
	}
	for _, c := range cases {
		if sameParent(c.a, c.b) != c.equal {
			t.Fatal("parent scope collapsed")
		}
	}
	assertMissingSelectionRefused(t)
	ended := Decimal(1)
	if validGenerationTimes(GenerationDTO{EndedNS: &ended}) {
		t.Fatal("end without start")
	}
}

func assertMissingSelectionRefused(t *testing.T) {
	t.Helper()
	for _, query := range []string{QueryEntry, QuerySelector} {
		r := Response{Kind: ModelRecords, Query: query, RepositoryID: readIDs("repo"), SourceID: readIDs("source"), GenerationID: readIDs("generation")}
		if r.Validate() != ErrInvalid {
			t.Fatal("missing selection admitted")
		}
	}
	p := Policy{Version: 1, SelectionVersion: 1, Intent: "capture", Scope: "source-root"}
	if _, err := p.Canonical(); err != ErrInvalid {
		t.Fatal("nil rules admitted")
	}
	p.Rules = []Rule{}
	if _, err := p.Canonical(); err != nil {
		t.Fatal("empty rules refused", err)
	}
}
