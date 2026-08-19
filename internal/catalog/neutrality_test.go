package catalog_test

import (
	"reflect"
	"testing"

	"github.com/franchoy/coldkeep/internal/catalog"
)

// TestCatalogContractGraphsAreNeutral walks every exported request/result
// graph. It rejects interface/any fields and dependencies on database, storage,
// domain, CLI, renderer, and stream implementations.
func TestCatalogContractGraphsAreNeutral(t *testing.T) {
	for _, root := range allCatalogContractRoots() {
		seen := make(map[reflect.Type]bool)
		checkCatalogContractType(t, reflect.TypeOf(root), reflect.TypeOf(root).Name(), seen)
	}
}

func allCatalogContractRoots() []any {
	return []any{
		catalog.Error{},
		catalog.LogicalFileRef{}, catalog.PhysicalFileRef{},
		catalog.CurrentFileRef{}, catalog.CurrentFilePage{}, catalog.CurrentFileSearch{},
		catalog.RepositoryConfigurationRef{}, catalog.SetRepositoryConfigurationResult{},
		catalog.SnapshotRef{}, catalog.SnapshotFilter{},
		catalog.SnapshotGraphNode{}, catalog.SnapshotGraph{}, catalog.ReachabilityRoots{},
		catalog.ContainerPlacementRef{}, catalog.LegacyChunkPlacement{},
		catalog.PackedChunkPlacement{}, catalog.ChunkPlacementRef{},
		catalog.RestorePlanInput{}, catalog.RestoreLogicalFileRef{},
		catalog.RestoreSourceRef{}, catalog.RestorePlanMetadata{},
		catalog.GCPlanInput{}, catalog.GCReachabilityRoot{}, catalog.GCPlanMetadata{},
	}
}

func TestCatalogOperationSetAndNeutralityCoverageAreComplete(t *testing.T) {
	wantMethods := []string{
		"FindLogicalFile",
		"FindPhysicalFilesForLogicalFile",
		"FindSnapshot",
		"GetRepositoryConfiguration",
		"ListCurrentFiles",
		"ListSnapshots",
		"LoadChunkPlacements",
		"LoadGCPlanMetadata",
		"LoadReachabilityRoots",
		"LoadRestorePlanMetadata",
		"LoadSnapshotGraph",
		"SearchCurrentFiles",
		"SetRepositoryConfiguration",
	}
	covered := make(map[reflect.Type]bool)
	for _, root := range allCatalogContractRoots() {
		covered[reflect.TypeOf(root)] = true
	}

	interfaceType := reflect.TypeOf((*catalog.Catalog)(nil)).Elem()
	if interfaceType.NumMethod() != len(wantMethods) {
		t.Fatalf("catalog operation count changed: got=%d want=%d", interfaceType.NumMethod(), len(wantMethods))
	}
	for i, want := range wantMethods {
		method := interfaceType.Method(i)
		if method.Name != want {
			t.Errorf("catalog operation %d changed: got=%q want=%q", i, method.Name, want)
		}
		for arg := 0; arg < method.Type.NumIn(); arg++ {
			assertCatalogMethodContractCovered(t, method.Name, method.Type.In(arg), covered)
		}
		for result := 0; result < method.Type.NumOut(); result++ {
			assertCatalogMethodContractCovered(t, method.Name, method.Type.Out(result), covered)
		}
	}
}

func assertCatalogMethodContractCovered(t *testing.T, method string, typ reflect.Type, covered map[reflect.Type]bool) {
	t.Helper()
	for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array {
		typ = typ.Elem()
	}
	if typ.PkgPath() != "github.com/franchoy/coldkeep/internal/catalog" || typ.Kind() != reflect.Struct {
		return
	}
	if !covered[typ] {
		t.Errorf("Catalog.%s contract %s is absent from the neutrality walk", method, typ)
	}
}

func checkCatalogContractType(t *testing.T, typ reflect.Type, path string, seen map[reflect.Type]bool) {
	t.Helper()
	for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array || typ.Kind() == reflect.Map {
		if typ.Kind() == reflect.Map {
			checkCatalogContractType(t, typ.Key(), path+".<key>", seen)
		}
		typ = typ.Elem()
	}
	if seen[typ] {
		return
	}
	seen[typ] = true

	if typ.Kind() == reflect.Interface {
		t.Errorf("%s exposes interface/any type %v", path, typ)
		return
	}
	if typ.Kind() == reflect.Func || typ.Kind() == reflect.Chan || typ.Kind() == reflect.UnsafePointer {
		t.Errorf("%s exposes executable or unsafe type %v", path, typ)
		return
	}
	if pkg := typ.PkgPath(); pkg != "" && pkg != "time" && pkg != "github.com/franchoy/coldkeep/internal/catalog" {
		t.Errorf("%s exposes non-neutral package type %v from %q", path, typ, pkg)
		return
	}
	if typ.Kind() != reflect.Struct || (typ.PkgPath() != "" && typ.PkgPath() != "github.com/franchoy/coldkeep/internal/catalog") {
		return
	}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}
		checkCatalogContractType(t, field.Type, path+"."+field.Name, seen)
	}
}
