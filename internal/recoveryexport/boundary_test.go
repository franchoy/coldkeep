package recoveryexport

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
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
		if name != "inventory.go" && name != "catalog.go" && name != "payload.go" && name != "catalog_session.go" && name != "generation_read_linux.go" {
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
// and SQL writes remain forbidden outside explicitly owned maintenance/import
// seams. New schema20 allowances are bound to both file and function names.
func TestRecoverySourceCallBoundary(t *testing.T) {
	explicitWrites := map[string]map[string]bool{
		"upgrade_sql_linux.go":    {"commitUpgrade": true},
		"generation_records.go":   {"insertGenerationPolicy": true, "insertGenerationRecords": true},
		"reader_session_linux.go": {"openRecoverySession": true},
	}
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
		for _, declaration := range f.Decls {
			function, isFunction := declaration.(*ast.FuncDecl)
			allowed := isFunction && explicitWrites[name][function.Name.Name]
			ast.Inspect(declaration, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if isFunction && allowedReadIndexExec(name, function.Name.Name, call) {
					return true
				}
				// Preserve the prior explicit adoption/binding allowance. C1 adds
				// only its transactional upgrade, insert-only model fixture seam,
				// and validated owned-reader session marker.
				if ok && (name == "adoption_linux.go" || name == "binding_sql_linux.go" || allowed) && sel.Sel.Name == "ExecContext" {
					return true
				}
				// The only preparation allowance is in the fresh-target importer.
				// No browsing/export function gains SQL, bootstrap or preparation capability.
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
}

// C2 admits only its owned index transaction's two fixed statements. The read
// adapter, other functions, bootstrap calls and arbitrary SQL retain the fence.
func allowedReadIndexExec(file, function string, call *ast.CallExpr) bool {
	if file != "generation_index_linux.go" || function != "commitReadIndex" || len(call.Args) != 2 {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "ExecContext" {
		return false
	}
	receiver, ok := sel.X.(*ast.Ident)
	if !ok || receiver.Name != "tx" {
		return false
	}
	if id, ok := call.Args[1].(*ast.Ident); ok {
		return id.Name == "generationReadIndexDDL"
	}
	literal, ok := call.Args[1].(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return false
	}
	sql, err := strconv.Unquote(literal.Value)
	return err == nil && sql == "LOCK TABLE public.capture_generation IN SHARE MODE"
}

func TestRecoveryReadWriteBoundary(t *testing.T) {
	for _, c := range []struct {
		name, file, function, statement string
		allowed                         bool
	}{
		{"ddl", "generation_index_linux.go", "commitReadIndex", "tx.ExecContext(ctx,generationReadIndexDDL)", true},
		{"lock", "generation_index_linux.go", "commitReadIndex", "tx.ExecContext(ctx,`LOCK TABLE public.capture_generation IN SHARE MODE`)", true},
		{"wrong-file", "generation_read_linux.go", "commitReadIndex", "tx.ExecContext(ctx,generationReadIndexDDL)", false},
		{"wrong-function", "generation_index_linux.go", "readIndexState", "tx.ExecContext(ctx,generationReadIndexDDL)", false},
		{"row-write", "generation_index_linux.go", "commitReadIndex", "tx.ExecContext(ctx,`UPDATE capture_generation SET outcome='failed'`)", false},
		{"dynamic-query", "generation_index_linux.go", "commitReadIndex", "tx.ExecContext(ctx,query)", false},
		{"extra-argument", "generation_index_linux.go", "commitReadIndex", "tx.ExecContext(ctx,generationReadIndexDDL,argument)", false},
		{"bootstrap", "generation_index_linux.go", "commitReadIndex", "tx.PrepareExisting(ctx,generationReadIndexDDL)", false},
		{"wrong-receiver", "generation_index_linux.go", "commitReadIndex", "db.ExecContext(ctx,generationReadIndexDDL)", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			file, err := parser.ParseFile(token.NewFileSet(), c.file, "package fixture; func "+c.function+"(){"+c.statement+"}", 0)
			must(t, err)
			calls := 0
			ast.Inspect(file, func(node ast.Node) bool {
				if call, ok := node.(*ast.CallExpr); ok {
					calls++
					if allowedReadIndexExec(c.file, c.function, call) != c.allowed {
						t.Fatal("wrong maintenance allowance")
					}
				}
				return true
			})
			if calls != 1 {
				t.Fatal("counterexample did not contain one call")
			}
		})
	}
}
