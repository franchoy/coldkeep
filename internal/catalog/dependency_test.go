package catalog_test

import (
	"bytes"
	"encoding/json"
	"os/exec"
	"testing"
)

// goListPkg is the subset of go list -json output we need for dependency
// direction tests.
type goListPkg struct {
	ImportPath string
	Imports    []string
	Deps       []string
}

// TestCatalogDependencyDirection enforces the dependency direction rules for
// internal/catalog. Mirrors the pattern in internal/engine/dependency_guard_test.go.
//
// Rules:
//  1. internal/catalog must not import internal/engine (transitively).
//  2. internal/catalog must not import cmd/coldkeep (transitively).
//  3. internal/catalog must not import CLI renderer packages (transitively).
func TestCatalogDependencyDirection(t *testing.T) {
	out, err := exec.Command("go", "list", "-json", "./...").Output()
	if err != nil {
		t.Fatalf("go list -json: %v", err)
	}

	pkgs := decodeGoListOutput(t, out)
	catalogPkg := findPkg(pkgs, "github.com/franchoy/coldkeep/internal/catalog")
	if catalogPkg == nil {
		t.Fatal("internal/catalog package not found in go list output")
	}

	forbidden := []struct {
		prefix string
		label  string
	}{
		{"github.com/franchoy/coldkeep/internal/engine", "internal/engine"},
		{"github.com/franchoy/coldkeep/cmd/coldkeep", "cmd/coldkeep"},
		{"github.com/spf13/cobra", "cobra (CLI renderer)"},
		{"github.com/franchoy/coldkeep/internal/cli", "internal/cli"},
	}

	for _, dep := range catalogPkg.Deps {
		for _, f := range forbidden {
			if dep == f.prefix || len(dep) > len(f.prefix) && dep[:len(f.prefix)+1] == f.prefix+"/" {
				t.Errorf("internal/catalog must not depend on %s (found %q in deps)", f.label, dep)
			}
		}
	}
}

func decodeGoListOutput(t *testing.T, data []byte) []goListPkg {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(data))
	var pkgs []goListPkg
	for dec.More() {
		var p goListPkg
		if err := dec.Decode(&p); err != nil {
			t.Fatalf("decode go list output: %v", err)
		}
		pkgs = append(pkgs, p)
	}
	return pkgs
}

func findPkg(pkgs []goListPkg, importPath string) *goListPkg {
	for i := range pkgs {
		if pkgs[i].ImportPath == importPath {
			return &pkgs[i]
		}
	}
	return nil
}
