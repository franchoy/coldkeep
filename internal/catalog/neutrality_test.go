package catalog_test

import (
	"database/sql"
	"io"
	"reflect"
	"testing"

	"github.com/franchoy/coldkeep/internal/catalog"
)

// TestCatalogExportedTypesAreNeutral verifies that exported catalog contract
// types do not expose any backend-specific or renderer-specific types in their
// fields.
//
// Forbidden field types: *sql.DB, *sql.Tx, sql.Rows, sql.Row, io.Writer,
// io.Reader, and any cobra/CLI renderer type.
func TestCatalogExportedTypesAreNeutral(t *testing.T) {
	types := []any{
		catalog.LogicalFileRef{},
		catalog.PhysicalFileRef{},
		catalog.SnapshotRef{},
		catalog.SnapshotFilter{},
		catalog.SnapshotGraph{},
		catalog.ReachabilityRoots{},
		catalog.ChunkPlacementRef{},
		catalog.RestorePlanInput{},
		catalog.RestorePlanMetadata{},
		catalog.GCPlanInput{},
		catalog.GCPlanMetadata{},
	}

	forbiddenTypes := []reflect.Type{
		reflect.TypeOf(&sql.DB{}),
		reflect.TypeOf(&sql.Tx{}),
		reflect.TypeOf(sql.Rows{}),
		reflect.TypeOf(sql.Row{}),
		reflect.TypeOf((*io.Writer)(nil)).Elem(),
		reflect.TypeOf((*io.Reader)(nil)).Elem(),
	}
	forbiddenNames := []string{
		"*sql.DB", "*sql.Tx", "sql.Rows", "sql.Row",
		"sql.NullTime", // internal scan type must not leak into exported types
	}

	for _, v := range types {
		rt := reflect.TypeOf(v)
		checkTypeNeutral(t, rt, forbiddenTypes, forbiddenNames, rt.Name())
	}
}

func checkTypeNeutral(t *testing.T, rt reflect.Type, forbiddenTypes []reflect.Type, forbiddenNames []string, path string) {
	t.Helper()
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	if rt.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		if !field.IsExported() {
			continue
		}
		ft := field.Type
		fieldPath := path + "." + field.Name

		for _, forbidden := range forbiddenTypes {
			if ft == forbidden || (forbidden.Kind() == reflect.Interface && ft.Implements(forbidden)) {
				t.Errorf("field %s has forbidden type %v", fieldPath, forbidden)
			}
		}
		for _, name := range forbiddenNames {
			if ft.String() == name {
				t.Errorf("field %s has forbidden type %q", fieldPath, name)
			}
		}
		// Recurse into nested structs defined in the same package.
		if ft.Kind() == reflect.Struct || (ft.Kind() == reflect.Ptr && ft.Elem().Kind() == reflect.Struct) {
			checkTypeNeutral(t, ft, forbiddenTypes, forbiddenNames, fieldPath)
		}
	}
}
