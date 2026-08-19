package engine_test

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

type goListPackage struct {
	ImportPath string
	Imports    []string
	Deps       []string
}

// TestEngineDependencyDirection enforces the v1.11 engine facade boundary:
//
//   - Rule 1: internal/engine must not import cmd/coldkeep (transitively).
//   - Rule 2: non-engine internal/* packages except the application composition
//     boundary must not directly import internal/engine.
//   - Rule 3: cmd/coldkeep may import internal/engine (allowed; not tested here).
//
// This guard prevents architecture drift as more CLI commands are routed through
// the engine over time.
func TestEngineDependencyDirection(t *testing.T) {
	const module = "github.com/franchoy/coldkeep"
	const enginePkg = module + "/internal/engine"
	const catalogPkg = module + "/internal/catalog"
	const cliPkg = module + "/cmd/coldkeep"
	const applicationPkg = module + "/internal/application"

	// Locate the module root so ./... covers all packages, not just internal/engine.
	modOut, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		t.Fatalf("go env GOMOD: %v", err)
	}
	moduleRoot := filepath.Dir(strings.TrimSpace(string(modOut)))

	cmd := exec.Command("go", "list", "-json", "./...")
	cmd.Dir = moduleRoot
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list -json ./...: %v", err)
	}

	dec := json.NewDecoder(strings.NewReader(string(out)))
	for dec.More() {
		var pkg goListPackage
		if err := dec.Decode(&pkg); err != nil {
			t.Fatalf("decode go list output: %v", err)
		}
		checkEngineNotDependsOnCLI(t, pkg, enginePkg, cliPkg)
		checkDomainNotImportsEngine(t, pkg, module, enginePkg, applicationPkg)
		checkCLINotImportsCatalog(t, pkg, cliPkg, catalogPkg)
	}
}

// checkEngineNotDependsOnCLI enforces Rule 1: engine must not depend on CLI,
// even transitively.
func checkEngineNotDependsOnCLI(t *testing.T, pkg goListPackage, enginePkg, cliPkg string) {
	t.Helper()
	if pkg.ImportPath != enginePkg {
		return
	}
	for _, dep := range pkg.Deps {
		if dep == cliPkg || strings.HasPrefix(dep, cliPkg+"/") {
			t.Errorf("Rule 1 violation: engine must not depend on CLI:\n\t%s -> %s",
				pkg.ImportPath, dep)
		}
	}
}

// checkDomainNotImportsEngine enforces Rule 2: non-engine internal/* packages
// must not directly import internal/engine.
func checkDomainNotImportsEngine(t *testing.T, pkg goListPackage, module, enginePkg, applicationPkg string) {
	t.Helper()
	if !strings.HasPrefix(pkg.ImportPath, module+"/internal/") ||
		strings.HasPrefix(pkg.ImportPath, enginePkg) || pkg.ImportPath == applicationPkg {
		return
	}
	for _, imp := range pkg.Imports {
		if imp == enginePkg || strings.HasPrefix(imp, enginePkg+"/") {
			t.Errorf("Rule 2 violation: domain/internal package must not import engine facade:\n\t%s -> %s",
				pkg.ImportPath, imp)
		}
	}
}

// checkCLINotImportsCatalog enforces the CLI-side coupling rule for phased
// migration: cmd/coldkeep must not import internal/catalog directly.
func checkCLINotImportsCatalog(t *testing.T, pkg goListPackage, cliPkg, catalogPkg string) {
	t.Helper()
	if pkg.ImportPath != cliPkg {
		return
	}
	for _, imp := range pkg.Imports {
		if imp == catalogPkg || strings.HasPrefix(imp, catalogPkg+"/") {
			t.Errorf("Rule 4 violation: CLI must not import catalog directly:\n\t%s -> %s",
				pkg.ImportPath, imp)
		}
	}
}
