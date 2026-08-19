package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

// TestProductionCLIUsesOnlyApplicationAndEngineExecutionBoundaries is the
// executable thin-CLI contract. Compatibility projection and explicitly
// isolated benchmark/simulation tooling have exact, reviewable exceptions.
func TestProductionCLIUsesOnlyApplicationAndEngineExecutionBoundaries(t *testing.T) {
	_, current, _, _ := runtime.Caller(0)
	dir := filepath.Dir(current)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	allowed := map[string]bool{
		"application_session.go:openApplicationSession:application.Open":                     true,
		"init.go:initCommand:storage.IsRegisteredCompressionCodec":                           true,
		"main.go:resolveTraceOptions:observability.NewJSONTraceSink":                         true,
		"main.go:newSimulationObservabilityService:observability.NewService":                 true,
		"main.go:captureBenchmarkDiagnosticFinalState:db.BuildPostgresConnStringFromEnv":     true,
		"main.go:createTemporaryBenchmarkDatabase:db.BuildPostgresConnStringFromEnv":         true,
		"main.go:captureBenchmarkState:db.BuildPostgresConnStringFromEnv":                    true,
		"main.go:runSimulateCommand:storage.ParseStorageContext":                             true,
		"main.go:runSimulateCommand:storage.StoreFileWithStorageContext":                     true,
		"main.go:runSimulateCommand:storage.StoreFileWithStorageContextAndCodec":             true,
		"main.go:runSimulateCommand:storage.StoreFolderWithStorageContextAndOptions":         true,
		"main.go:runSimulateCommand:storage.StoreFolderWithStorageContextAndCodecAndOptions": true,
		"main.go:parseSnapshotQuery:snapshot.NormalizeSnapshotPath":                          true,
		"main.go:parseSnapshotRestoreExactPathSelectors:snapshot.NormalizeSnapshotPath":      true,
		"main.go:parseSnapshotRestorePrefixSelectors:snapshot.NormalizeSnapshotPath":         true,
	}
	used := make(map[string]bool)
	var violations []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		inspectCalls := func(owner string, body ast.Node) {
			ast.Inspect(body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				pkg, ok := selector.X.(*ast.Ident)
				if !ok || !isGuardedCLIPackage(pkg.Name) {
					return true
				}
				key := name + ":" + owner + ":" + pkg.Name + "." + selector.Sel.Name
				if allowed[key] {
					used[key] = true
					return true
				}
				if isProjectionOnlyCall(name, pkg.Name, selector.Sel.Name) {
					return true
				}
				violations = append(violations, key)
				return true
			})
		}
		for _, declaration := range file.Decls {
			switch declaration := declaration.(type) {
			case *ast.FuncDecl:
				if declaration.Body != nil {
					inspectCalls(declaration.Name.Name, declaration.Body)
				}
			case *ast.GenDecl:
				for _, spec := range declaration.Specs {
					values, ok := spec.(*ast.ValueSpec)
					if !ok {
						continue
					}
					for i, value := range values.Values {
						literal, ok := value.(*ast.FuncLit)
						if !ok {
							continue
						}
						owner := "<function literal>"
						if i < len(values.Names) {
							owner = values.Names[i].Name
						}
						inspectCalls(owner, literal.Body)
					}
				}
			}
		}
	}
	for exception := range allowed {
		if !used[exception] {
			violations = append(violations, "stale allowlist entry: "+exception)
		}
	}
	sort.Strings(violations)
	if len(violations) > 0 {
		t.Fatalf("production CLI bypasses application/engine boundary:\n  %s", strings.Join(violations, "\n  "))
	}
}

func isGuardedCLIPackage(name string) bool {
	switch name {
	case "application", "catalog", "db", "maintenance", "observability", "recovery", "snapshot", "storage":
		return true
	default:
		return false
	}
}

func isProjectionOnlyCall(filename, packageName, selector string) bool {
	if filename == "observability_engine_adapters.go" && packageName == "observability" {
		return true
	}
	if filename == "stored_path_engine_adapters.go" && packageName == "storage" {
		return true
	}
	if filename != "main.go" || packageName != "snapshot" {
		return false
	}
	switch selector {
	case "Snapshot", "SnapshotLineageStatus", "DiffType":
		return true
	default:
		return false
	}
}
