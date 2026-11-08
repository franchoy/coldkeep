package recoveryexport

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// Runtime before/after and event checks cannot detect a mutation-capable
// constructor that happens to make no changes on one fixture. Keep this narrow
// production seam free of bootstrap/repair/pin and ambient-key constructors.
func TestPassiveCallBoundary(t *testing.T) {
	forbidden := map[string]bool{}
	for _, name := range strings.Fields("ConnectDB EnsureSchema RunMigrations ParseStorageContext OpenLocalStorage LoadDefaultStorageContext GetBlockTransformer VerifyStoredBlock NewLocalWriter NewLocalWriterWithDirAndDB Exec ExecContext Create CreateTemp Mkdir MkdirAll Remove RemoveAll Rename OpenFile WriteFile") {
		forbidden[name] = true
	}
	entries, e := os.ReadDir(".")
	must(t, e)
	for _, entry := range entries {
		name := entry.Name()
		if name != "inventory.go" && name != "catalog.go" && name != "payload.go" && name != "catalog_session.go" {
			continue
		}
		f, e := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		must(t, e)
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if ok && forbidden[sel.Sel.Name] {
				t.Errorf("%s: mutation-capable or ambient constructor %s", name, sel.Sel.Name)
			}
			return true
		})
	}
}

// Destination writes are allowed in Export; source runtime/bootstrap/preparation
// and SQL writes remain forbidden outside the explicit fresh-target importer.
func TestRecoverySourceCallBoundary(t *testing.T) {
	forbidden := map[string]bool{}
	for _, name := range strings.Fields("PrepareExisting PrepareControlNamespace ConnectDB EnsureSchema RunMigrations ParseStorageContext OpenLocalStorage LoadDefaultStorageContext NewLocalWriter NewLocalWriterWithDirAndDB Exec ExecContext") {
		forbidden[name] = true
	}
	entries, e := os.ReadDir(".")
	must(t, e)
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, e := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		must(t, e)
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			// The only preparation allowance is in the fresh-target importer.
			// No source/export file gains SQL, bootstrap or preparation capability.
			if ok && name == "import_linux.go" && sel.Sel.Name == "PrepareExisting" {
				return true
			}
			if ok && forbidden[sel.Sel.Name] {
				t.Errorf("%s: source mutation capability %s", name, sel.Sel.Name)
			}
			return true
		})
	}
}
