package recoveryexport

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// These parser regressions do not require PostgreSQL or payload fixtures. The
// manifest passes metadata admission; no catalog.dump is supplied, so admitted
// JSON stops at the first missing member. checkGeneration returns its decoded
// Manifest even on error, allowing an over-limit test to prove that the typed
// decode never populated its slices without memory benchmarks or large inputs.
func parserManifest() Manifest {
	return Manifest{
		Version: packageVersion, Generation: strings.Repeat("a", 32),
		ApplicationVersion: "2.0.0-dev.0", CatalogVersion: 17,
		Selection: "full-catalog", CutTime: "2026-11-08T00:00:00Z",
		Snapshot: "00000003-0000001B-1", LegacyRootHash: strings.Repeat("b", 64),
		Reader: "coldkeep v1.13.19", KeyCustody: "unknown",
		ReaderCustody: "unknown", Independence: "unknown",
		Database: DatabaseRequirements{
			ServerVersion: 160000, Encoding: "UTF8", Collate: "C", CType: "C",
			LocaleProvider: "c", DumpVersion: "16.0", RestoreVersion: "16.0",
			RestorePolicy: "trusted operator; fresh database; --no-owner --no-acl --exit-on-error",
		},
		CatalogRows: 1, Tables: 19,
		Members: []Member{{Name: "catalog.dump", Size: 8, SHA256: strings.Repeat("c", 64)}},
	}
}

func parserManifestJSON(t *testing.T, m Manifest) []byte {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func checkParserManifest(t *testing.T, ctx context.Context, b []byte, members int) (Manifest, error) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	})
	limits := DefaultExportLimits()
	limits.Members = members
	m, _, err := checkGeneration(ctx, root, limits, false)
	return m, err
}

func TestRecoveryJSONPredecodeLimits(t *testing.T) {
	for _, name := range []string{"members", "excluded"} {
		t.Run(name, func(t *testing.T) {
			input := parserManifest()
			if name == "members" {
				input.Members = append(input.Members, Member{})
			} else {
				// About 60 KB, below the manifest byte cap; the fixed
				// directory-entry ceiling is independent of Members.
				input.Excluded = make([]string, DefaultLimits().DirectoryEntries+1)
			}
			m, err := checkParserManifest(t, context.Background(), parserManifestJSON(t, input), 1)
			if !errors.Is(err, ErrLimit) || m.Members != nil || m.Excluded != nil {
				t.Fatalf("limit must refuse before typed decode: error=%v members=%d excluded=%d", err, len(m.Members), len(m.Excluded))
			}
		})
	}
}

func TestRecoveryJSONIndependentLimits(t *testing.T) {
	for _, count := range []int{2, DefaultLimits().DirectoryEntries} {
		input := parserManifest()
		input.Excluded = make([]string, count)
		m, err := checkParserManifest(t, context.Background(), parserManifestJSON(t, input), 1)
		if !errors.Is(err, ErrPackage) || errors.Is(err, ErrLimit) || len(m.Members) != 1 || len(m.Excluded) != count {
			t.Fatalf("Members=1 must still admit %d exclusions before missing catalog refusal: error=%v members=%d excluded=%d", count, err, len(m.Members), len(m.Excluded))
		}
	}
}

func TestRecoveryJSONFieldAliases(t *testing.T) {
	for _, key := range []string{"MEMBERS", "member\u017f"} {
		t.Run(key, func(t *testing.T) {
			input := parserManifest()
			input.Members = append(input.Members, Member{})
			b := strings.Replace(string(parserManifestJSON(t, input)), `"members":`, `"`+key+`":`, 1)
			m, err := checkParserManifest(t, context.Background(), []byte(b), 1)
			if !errors.Is(err, ErrLimit) || m.Members != nil {
				t.Fatalf("field alias bypassed pre-decode cap: error=%v members=%d", err, len(m.Members))
			}
		})
	}
	t.Run("excluded-uppercase", func(t *testing.T) {
		input := parserManifest()
		input.Excluded = make([]string, DefaultLimits().DirectoryEntries+1)
		b := strings.Replace(string(parserManifestJSON(t, input)), `"uncataloged_excluded":`, `"UNCATALOGED_EXCLUDED":`, 1)
		m, err := checkParserManifest(t, context.Background(), []byte(b), 1)
		if !errors.Is(err, ErrLimit) || m.Excluded != nil {
			t.Fatalf("excluded alias bypassed pre-decode cap: error=%v excluded=%d", err, len(m.Excluded))
		}
	})
	for _, key := range []string{"MEMBERS", "member\u017f"} {
		t.Run("duplicate-"+key, func(t *testing.T) {
			b := strings.TrimSuffix(string(parserManifestJSON(t, parserManifest())), "}") + `,"` + key + `":[]}`
			m, err := checkParserManifest(t, context.Background(), []byte(b), 1)
			if !errors.Is(err, ErrPackage) || m.Version != 0 || m.Members != nil {
				t.Fatalf("case-fold duplicate reached typed decode: error=%v version=%d members=%d", err, m.Version, len(m.Members))
			}
		})
	}
	t.Run("nested-duplicate", func(t *testing.T) {
		b := strings.Replace(string(parserManifestJSON(t, parserManifest())), `"name":"catalog.dump"`, `"name":"catalog.dump","NAME":"catalog.dump"`, 1)
		m, err := checkParserManifest(t, context.Background(), []byte(b), 1)
		if !errors.Is(err, ErrPackage) || m.Version != 0 || m.Members != nil {
			t.Fatalf("nested duplicate reached typed decode: error=%v version=%d members=%d", err, m.Version, len(m.Members))
		}
	})
}

func TestRecoveryJSONStrictness(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		limit     bool
	}{
		{"duplicate-key", `{"package_version":1,"package_version":1}`, false},
		{"depth", strings.Repeat("[", 14) + "0" + strings.Repeat("]", 14), true},
		{"long-string", `{"reader_requirement":"` + strings.Repeat("x", 4097) + `"}`, true},
		{"long-key", `{"package_version":1,"` + strings.Repeat("x", 129) + `":0}`, false},
		{"trailing-value", string(parserManifestJSON(t, parserManifest())) + ` {}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, err := checkParserManifest(t, context.Background(), []byte(tc.raw), 1)
			if !errors.Is(err, ErrPackage) || tc.limit && !errors.Is(err, ErrLimit) || m.Version != 0 {
				t.Fatalf("strict preflight failed: error=%v version=%d", err, m.Version)
			}
		})
	}
	t.Run("unknown-field", func(t *testing.T) {
		_, err := checkParserManifest(t, context.Background(), []byte(`{"unknown":0}`), 1)
		if !errors.Is(err, ErrPackage) || !strings.Contains(err.Error(), `unknown field "unknown"`) {
			t.Fatalf("unknown-field refusal lost: %v", err)
		}
	})
	for _, raw := range []string{`{"members":{}}`, `{"members":[1]}`, `{"uncataloged_excluded":[1]}`} {
		var typeError *json.UnmarshalTypeError
		_, err := checkParserManifest(t, context.Background(), []byte(raw), 1)
		if !errors.Is(err, ErrPackage) || !errors.As(err, &typeError) {
			t.Fatalf("wrong-type refusal lost for %s: %v", raw, err)
		}
	}
}

type parserCancelContext struct {
	context.Context
	cancel context.CancelFunc
	checks int
}

func (c *parserCancelContext) Err() error {
	c.checks++
	if c.checks == 4 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestRecoveryJSONCancellation(t *testing.T) {
	for _, during := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		parseContext := ctx
		if during {
			// Deterministic cancellation while walking tokens, without a
			// timeout, a goroutine race, or a large input.
			parseContext = &parserCancelContext{Context: ctx, cancel: cancel}
		} else {
			cancel()
		}
		m, err := checkParserManifest(t, parseContext, parserManifestJSON(t, parserManifest()), 1)
		cancel()
		if !errors.Is(err, context.Canceled) || m.Version != 0 || m.Members != nil {
			t.Fatalf("cancellation must stop preflight (during=%v): error=%v version=%d members=%d", during, err, m.Version, len(m.Members))
		}
	}
}
