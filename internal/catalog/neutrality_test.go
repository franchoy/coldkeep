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
	rt = dereferenceCatalogType(rt)
	if !isStructType(rt) {
		return
	}

	for _, field := range exportedStructFields(rt) {
		checkFieldNeutrality(t, field, forbiddenTypes, forbiddenNames, path)
	}
}

func checkFieldNeutrality(
	t *testing.T,
	field reflect.StructField,
	forbiddenTypes []reflect.Type,
	forbiddenNames []string,
	parentPath string,
) {
	t.Helper()
	fieldPath := parentPath + "." + field.Name
	fieldType := field.Type

	checkForbiddenFieldTypes(t, fieldPath, fieldType, forbiddenTypes)
	checkForbiddenFieldNames(t, fieldPath, fieldType, forbiddenNames)
	recurseIntoNestedStruct(t, fieldPath, fieldType, forbiddenTypes, forbiddenNames)
}

func dereferenceCatalogType(rt reflect.Type) reflect.Type {
	if rt.Kind() == reflect.Ptr {
		return rt.Elem()
	}
	return rt
}

func isStructType(rt reflect.Type) bool {
	return rt.Kind() == reflect.Struct
}

func exportedStructFields(rt reflect.Type) []reflect.StructField {
	fields := make([]reflect.StructField, 0, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		if field.IsExported() {
			fields = append(fields, field)
		}
	}
	return fields
}

func checkForbiddenFieldTypes(t *testing.T, fieldPath string, fieldType reflect.Type, forbiddenTypes []reflect.Type) {
	t.Helper()
	for _, forbidden := range forbiddenTypes {
		if isForbiddenFieldType(fieldType, forbidden) {
			t.Errorf("field %s has forbidden type %v", fieldPath, forbidden)
		}
	}
}

func isForbiddenFieldType(fieldType, forbidden reflect.Type) bool {
	return fieldType == forbidden || implementsForbiddenInterface(fieldType, forbidden)
}

func implementsForbiddenInterface(fieldType, forbidden reflect.Type) bool {
	return forbidden.Kind() == reflect.Interface && fieldType.Implements(forbidden)
}

func checkForbiddenFieldNames(t *testing.T, fieldPath string, fieldType reflect.Type, forbiddenNames []string) {
	t.Helper()
	for _, name := range forbiddenNames {
		if fieldType.String() == name {
			t.Errorf("field %s has forbidden type %q", fieldPath, name)
		}
	}
}

func recurseIntoNestedStruct(
	t *testing.T,
	fieldPath string,
	fieldType reflect.Type,
	forbiddenTypes []reflect.Type,
	forbiddenNames []string,
) {
	t.Helper()
	if nestedStructType(fieldType) == nil {
		return
	}
	checkTypeNeutral(t, fieldType, forbiddenTypes, forbiddenNames, fieldPath)
}

func nestedStructType(fieldType reflect.Type) reflect.Type {
	fieldType = dereferenceCatalogType(fieldType)
	if isStructType(fieldType) {
		return fieldType
	}
	return nil
}
