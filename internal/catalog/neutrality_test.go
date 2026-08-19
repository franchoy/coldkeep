package catalog_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/catalog"
)

// TestCatalogContractGraphsAreNeutral walks every exported request/result
// graph. It rejects interface/any fields and dependencies on database, storage,
// domain, CLI, renderer, and stream implementations.
func TestCatalogContractGraphsAreNeutral(t *testing.T) {
	roots := []any{
		catalog.LogicalFileRef{}, catalog.PhysicalFileRef{},
		catalog.SnapshotRef{}, catalog.SnapshotFilter{},
		catalog.SnapshotGraph{}, catalog.ReachabilityRoots{},
		catalog.ContainerPlacementRef{}, catalog.LegacyChunkPlacement{},
		catalog.PackedChunkPlacement{}, catalog.ChunkPlacementRef{},
		catalog.RestorePlanInput{}, catalog.RestoreLogicalFileRef{},
		catalog.RestoreSourceRef{}, catalog.RestorePlanMetadata{},
		catalog.GCPlanInput{}, catalog.GCReachabilityRoot{}, catalog.GCPlanMetadata{},
	}

	forbiddenPackages := []string{
		"database/sql", "internal/storage", "internal/domain", "internal/db",
		"cmd/coldkeep", "internal/cli", "github.com/spf13/cobra", "io",
	}
	for _, root := range roots {
		seen := make(map[reflect.Type]bool)
		checkCatalogContractType(t, reflect.TypeOf(root), reflect.TypeOf(root).Name(), forbiddenPackages, seen)
	}
}

func checkCatalogContractType(t *testing.T, typ reflect.Type, path string, forbiddenPackages []string, seen map[reflect.Type]bool) {
	t.Helper()
	for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array || typ.Kind() == reflect.Map {
		if typ.Kind() == reflect.Map {
			checkCatalogContractType(t, typ.Key(), path+".<key>", forbiddenPackages, seen)
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
	for _, forbidden := range forbiddenPackages {
		if typ.PkgPath() == forbidden || strings.Contains(typ.PkgPath(), forbidden) {
			t.Errorf("%s exposes forbidden package type %v", path, typ)
			return
		}
	}
	if typ.Kind() != reflect.Struct || (typ.PkgPath() != "" && typ.PkgPath() != "github.com/franchoy/coldkeep/internal/catalog") {
		return
	}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}
		checkCatalogContractType(t, field.Type, path+"."+field.Name, forbiddenPackages, seen)
	}
}
