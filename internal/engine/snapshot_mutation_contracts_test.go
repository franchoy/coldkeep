package engine_test

import (
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/engine"
)

func TestSnapshotMutationCreateContractShape(t *testing.T) {
	assertExactStructFields(t, reflect.TypeOf(engine.SnapshotCreateRequest{}), []fieldExpectation{
		{"ID", reflect.TypeOf("")},
		{"Label", reflect.TypeOf("")},
		{"ParentID", reflect.TypeOf("")},
		{"Paths", reflect.TypeOf([]string{})},
	})
	assertStructOmitsFields(t, reflect.TypeOf(engine.SnapshotCreateRequest{}), []string{
		"Type", "DB", "Storage", "Writer", "Duration", "PerfSpans",
	})

	assertExactStructFields(t, reflect.TypeOf(engine.SnapshotCreateResult{}), []fieldExpectation{
		{"SnapshotID", reflect.TypeOf("")},
		{"Type", reflect.TypeOf(engine.SnapshotType(""))},
		{"PathsCount", reflect.TypeOf(int(0))},
		{"FilesInserted", reflect.TypeOf(int(0))},
		{"Label", reflect.TypeOf("")},
		{"ParentID", reflect.TypeOf("")},
	})
	assertStructOmitsFields(t, reflect.TypeOf(engine.SnapshotCreateResult{}), []string{
		"Warnings", "Duration", "PerfSpans", "OutputRoot", "TypeName",
	})
}

func TestSnapshotMutationDeleteContractShape(t *testing.T) {
	assertExactStructFields(t, reflect.TypeOf(engine.SnapshotDeleteParent{}), []fieldExpectation{
		{"ID", reflect.TypeOf("")},
		{"State", reflect.TypeOf(engine.SnapshotDeleteParentState(""))},
	})

	assertExactStructFields(t, reflect.TypeOf(engine.SnapshotDeletePreviewResult{}), []fieldExpectation{
		{"Parent", reflect.TypeOf(engine.SnapshotDeleteParent{})},
		{"Children", reflect.TypeOf([]string{})},
		{"TotalFiles", reflect.TypeOf(int64(0))},
		{"UniqueFiles", reflect.TypeOf(int64(0))},
		{"SharedFiles", reflect.TypeOf(int64(0))},
	})

	assertExactStructFields(t, reflect.TypeOf(engine.SnapshotDeleteRequest{}), []fieldExpectation{
		{"SnapshotID", reflect.TypeOf("")},
		{"Mode", reflect.TypeOf(engine.SnapshotDeleteMode(""))},
	})
	assertStructOmitsFields(t, reflect.TypeOf(engine.SnapshotDeleteRequest{}), []string{
		"Force", "DryRun", "Output", "JSON", "Duration",
	})

	assertExactStructFields(t, reflect.TypeOf(engine.SnapshotDeleteResult{}), []fieldExpectation{
		{"SnapshotID", reflect.TypeOf("")},
		{"Mode", reflect.TypeOf(engine.SnapshotDeleteMode(""))},
		{"Deleted", reflect.TypeOf(true)},
		{"Preview", reflect.TypeOf((*engine.SnapshotDeletePreviewResult)(nil))},
	})
	assertStructOmitsFields(t, reflect.TypeOf(engine.SnapshotDeleteResult{}), []string{
		"DryRun", "ParentID", "ParentMissing", "Children", "TotalFiles",
		"UniqueFiles", "SharedFiles", "Warnings",
	})
}

func TestSnapshotMutationRestoreContractShape(t *testing.T) {
	assertExactStructFields(t, reflect.TypeOf(engine.SnapshotRestoreSelection{}), []fieldExpectation{
		{"ExactPaths", reflect.TypeOf([]string{})},
		{"Prefixes", reflect.TypeOf([]string{})},
		{"Pattern", reflect.TypeOf("")},
		{"Regex", reflect.TypeOf("")},
		{"MinSize", reflect.TypeOf((*int64)(nil))},
		{"MaxSize", reflect.TypeOf((*int64)(nil))},
		{"ModifiedAfter", reflect.TypeOf((*time.Time)(nil))},
		{"ModifiedBefore", reflect.TypeOf((*time.Time)(nil))},
	})
	assertStructOmitsFields(t, reflect.TypeOf(engine.SnapshotRestoreSelection{}), []string{
		"Path", "Prefix", "Limit",
	})

	assertExactStructFields(t, reflect.TypeOf(engine.SnapshotRestoreDestination{}), []fieldExpectation{
		{"Mode", reflect.TypeOf(engine.SnapshotRestoreDestinationMode(""))},
		{"Path", reflect.TypeOf("")},
	})

	assertExactStructFields(t, reflect.TypeOf(engine.SnapshotRestoreWarning{}), []fieldExpectation{
		{"Code", reflect.TypeOf(engine.SnapshotRestoreWarningCode(""))},
		{"Path", reflect.TypeOf("")},
		{"Operation", reflect.TypeOf("")},
		{"Detail", reflect.TypeOf("")},
	})

	assertExactStructFields(t, reflect.TypeOf(engine.SnapshotRestoreRequest{}), []fieldExpectation{
		{"SnapshotID", reflect.TypeOf("")},
		{"Paths", reflect.TypeOf([]string{})},
		{"Selection", reflect.TypeOf(engine.SnapshotRestoreSelection{})},
		{"Destination", reflect.TypeOf(engine.SnapshotRestoreDestination{})},
		{"Overwrite", reflect.TypeOf(true)},
		{"Metadata", reflect.TypeOf(engine.SnapshotRestoreMetadataMode(""))},
	})
	assertStructOmitsFields(t, reflect.TypeOf(engine.SnapshotRestoreRequest{}), []string{
		"DestinationMode", "DestinationRoot", "DestinationPath",
		"Strict", "NoMetadata", "Query", "Type", "DB", "StorageContext",
	})
}

func TestSnapshotMutationRestoreResultContractShape(t *testing.T) {
	assertExactStructFields(t, reflect.TypeOf(engine.SnapshotRestoreResult{}), []fieldExpectation{
		{"SnapshotID", reflect.TypeOf("")},
		{"DestinationMode", reflect.TypeOf(engine.SnapshotRestoreDestinationMode(""))},
		{"RequestedPathsCount", reflect.TypeOf(int(0))},
		{"RestoredFiles", reflect.TypeOf(int64(0))},
		{"OutputTarget", reflect.TypeOf("")},
		{"OutputPaths", reflect.TypeOf([]string{})},
		{"Warnings", reflect.TypeOf([]engine.SnapshotRestoreWarning{})},
	})
	assertStructOmitsFields(t, reflect.TypeOf(engine.SnapshotRestoreResult{}), []string{
		"Type", "OutputRoot", "Duration", "PerfSpans", "Writer",
	})
}

func TestSnapshotMutationEnumValues(t *testing.T) {
	if engine.SnapshotTypeFull != "full" || engine.SnapshotTypePartial != "partial" {
		t.Fatalf("snapshot type enum drifted: full=%q partial=%q", engine.SnapshotTypeFull, engine.SnapshotTypePartial)
	}
	if engine.SnapshotDeleteModePreview != "preview" || engine.SnapshotDeleteModeExecute != "execute" {
		t.Fatalf("snapshot delete mode enum drifted: preview=%q execute=%q", engine.SnapshotDeleteModePreview, engine.SnapshotDeleteModeExecute)
	}
	if engine.SnapshotDeleteParentNone != "none" || engine.SnapshotDeleteParentPresent != "present" || engine.SnapshotDeleteParentMissing != "missing" {
		t.Fatalf("snapshot delete parent-state enum drifted: none=%q present=%q missing=%q",
			engine.SnapshotDeleteParentNone, engine.SnapshotDeleteParentPresent, engine.SnapshotDeleteParentMissing)
	}
	if engine.SnapshotRestoreDestinationOriginal != "original" ||
		engine.SnapshotRestoreDestinationPrefix != "prefix" ||
		engine.SnapshotRestoreDestinationOverride != "override" {
		t.Fatalf("snapshot restore destination enum drifted: original=%q prefix=%q override=%q",
			engine.SnapshotRestoreDestinationOriginal, engine.SnapshotRestoreDestinationPrefix, engine.SnapshotRestoreDestinationOverride)
	}
	if engine.SnapshotRestoreMetadataBestEffort != "" ||
		engine.SnapshotRestoreMetadataStrict != "strict" ||
		engine.SnapshotRestoreMetadataNone != "none" {
		t.Fatalf("snapshot restore metadata enum drifted: best-effort=%q strict=%q none=%q",
			engine.SnapshotRestoreMetadataBestEffort, engine.SnapshotRestoreMetadataStrict, engine.SnapshotRestoreMetadataNone)
	}
	if engine.SnapshotRestoreWarningMetadata != "metadata_apply_failed" {
		t.Fatalf("snapshot restore warning code drifted: %q", engine.SnapshotRestoreWarningMetadata)
	}
}

func TestSnapshotMutationPointerAndSliceShapes(t *testing.T) {
	selection := reflect.TypeOf(engine.SnapshotRestoreSelection{})
	assertFieldType(t, selection, "ExactPaths", reflect.TypeOf([]string{}))
	assertFieldType(t, selection, "Prefixes", reflect.TypeOf([]string{}))
	assertFieldType(t, selection, "MinSize", reflect.TypeOf((*int64)(nil)))
	assertFieldType(t, selection, "MaxSize", reflect.TypeOf((*int64)(nil)))
	assertFieldType(t, selection, "ModifiedAfter", reflect.TypeOf((*time.Time)(nil)))
	assertFieldType(t, selection, "ModifiedBefore", reflect.TypeOf((*time.Time)(nil)))

	deleteResult := reflect.TypeOf(engine.SnapshotDeleteResult{})
	assertFieldType(t, deleteResult, "Preview", reflect.TypeOf((*engine.SnapshotDeletePreviewResult)(nil)))

	restoreResult := reflect.TypeOf(engine.SnapshotRestoreResult{})
	assertFieldType(t, restoreResult, "OutputPaths", reflect.TypeOf([]string{}))
	assertFieldType(t, restoreResult, "Warnings", reflect.TypeOf([]engine.SnapshotRestoreWarning{}))
	assertFieldType(t, restoreResult, "RestoredFiles", reflect.TypeOf(int64(0)))

	deletePreview := reflect.TypeOf(engine.SnapshotDeletePreviewResult{})
	assertFieldType(t, deletePreview, "TotalFiles", reflect.TypeOf(int64(0)))
	assertFieldType(t, deletePreview, "UniqueFiles", reflect.TypeOf(int64(0)))
	assertFieldType(t, deletePreview, "SharedFiles", reflect.TypeOf(int64(0)))
}

func TestSnapshotMutationRepresentability(t *testing.T) {
	assertSnapshotCreateRepresentability(t)
	assertSnapshotDeleteRepresentability(t)
	assertSnapshotRestoreRepresentability(t)
}

func TestSnapshotMutationContractsDoNotExposeImplementationDependencies(t *testing.T) {
	disallowed := []reflect.Type{
		reflect.TypeOf((*sql.DB)(nil)),
		reflect.TypeOf((*sql.Tx)(nil)),
		reflect.TypeOf(sql.NullString{}),
	}
	typesToCheck := []reflect.Type{
		reflect.TypeOf(engine.SnapshotCreateRequest{}),
		reflect.TypeOf(engine.SnapshotCreateResult{}),
		reflect.TypeOf(engine.SnapshotDeleteParent{}),
		reflect.TypeOf(engine.SnapshotDeletePreviewResult{}),
		reflect.TypeOf(engine.SnapshotDeleteRequest{}),
		reflect.TypeOf(engine.SnapshotDeleteResult{}),
		reflect.TypeOf(engine.SnapshotRestoreSelection{}),
		reflect.TypeOf(engine.SnapshotRestoreDestination{}),
		reflect.TypeOf(engine.SnapshotRestoreWarning{}),
		reflect.TypeOf(engine.SnapshotRestoreRequest{}),
		reflect.TypeOf(engine.SnapshotRestoreResult{}),
	}
	for _, rt := range typesToCheck {
		for i := 0; i < rt.NumField(); i++ {
			fieldType := rt.Field(i).Type
			for _, forbidden := range disallowed {
				if fieldType == forbidden {
					t.Fatalf("%s field %s exposes forbidden type %v", rt.Name(), rt.Field(i).Name, forbidden)
				}
			}
		}
	}
}

func TestDefaultEngineDoesNotConsumeInactiveSnapshotMutationRequestTypes(t *testing.T) {
	defaultEngine := reflect.TypeOf((*engine.DefaultEngine)(nil))
	for i := 0; i < defaultEngine.NumMethod(); i++ {
		method := defaultEngine.Method(i)
		for j := 0; j < method.Type.NumIn(); j++ {
			arg := method.Type.In(j)
			if arg == reflect.TypeOf(engine.SnapshotRestoreRequest{}) {
				t.Fatalf("DefaultEngine method %s unexpectedly consumes snapshot mutation request type %v", method.Name, arg)
			}
		}
	}
}

func assertSnapshotCreateRepresentability(t *testing.T) {
	t.Helper()

	createFull := engine.SnapshotCreateRequest{}
	createPartial := engine.SnapshotCreateRequest{Paths: []string{"docs/a.txt"}}
	if len(createFull.Paths) != 0 || len(createPartial.Paths) != 1 {
		t.Fatal("snapshot create request shape no longer represents full and partial snapshots")
	}

	createResult := engine.SnapshotCreateResult{
		SnapshotID:    "snap-abc",
		Type:          engine.SnapshotTypePartial,
		PathsCount:    1,
		FilesInserted: 3,
		Label:         "label",
		ParentID:      "parent",
	}
	if createResult.SnapshotID == "" || createResult.Type == "" {
		t.Fatal("snapshot create result shape not representable")
	}
}

func assertSnapshotDeleteRepresentability(t *testing.T) {
	t.Helper()

	preview := engine.SnapshotDeleteResult{
		SnapshotID: "s1",
		Mode:       engine.SnapshotDeleteModePreview,
		Preview: &engine.SnapshotDeletePreviewResult{
			Parent:      engine.SnapshotDeleteParent{ID: "p0", State: engine.SnapshotDeleteParentPresent},
			Children:    []string{"c1"},
			TotalFiles:  5,
			UniqueFiles: 2,
			SharedFiles: 3,
		},
	}
	execute := engine.SnapshotDeleteResult{
		SnapshotID: "s1",
		Mode:       engine.SnapshotDeleteModeExecute,
		Deleted:    true,
	}
	if preview.Preview == nil || execute.Preview != nil || !execute.Deleted {
		t.Fatal("snapshot delete result shape not representable")
	}
}

func assertSnapshotRestoreRepresentability(t *testing.T) {
	t.Helper()

	after := time.Unix(0, 0)
	before := time.Unix(100, 0)
	restoreReq := engine.SnapshotRestoreRequest{
		SnapshotID: "s1",
		Paths:      []string{"docs/a.txt"},
		Selection: engine.SnapshotRestoreSelection{
			ExactPaths:     []string{"docs/a.txt", "docs/a.txt"},
			Prefixes:       []string{"docs/", "docs/sub/"},
			Pattern:        "*.txt",
			Regex:          "^docs/",
			MinSize:        int64Ptr(1),
			MaxSize:        int64Ptr(10),
			ModifiedAfter:  &after,
			ModifiedBefore: &before,
		},
		Destination: engine.SnapshotRestoreDestination{
			Mode: engine.SnapshotRestoreDestinationOriginal,
			Path: "/tmp/out",
		},
		Metadata: engine.SnapshotRestoreMetadataBestEffort,
	}
	if len(restoreReq.Selection.ExactPaths) != 2 || len(restoreReq.Selection.Prefixes) != 2 {
		t.Fatal("snapshot restore request no longer preserves repeated selectors")
	}

	restoreResult := engine.SnapshotRestoreResult{
		SnapshotID:          "s1",
		DestinationMode:     engine.SnapshotRestoreDestinationOverride,
		RequestedPathsCount: 1,
		RestoredFiles:       1,
		OutputTarget:        "/tmp/out.txt",
		OutputPaths:         []string{"/tmp/out.txt"},
		Warnings: []engine.SnapshotRestoreWarning{
			{Code: engine.SnapshotRestoreWarningMetadata, Path: "/tmp/out.txt", Operation: "chmod", Detail: "failed"},
		},
	}
	if restoreResult.DestinationMode == "" || len(restoreResult.OutputPaths) != 1 {
		t.Fatal("snapshot restore result shape not representable")
	}
}

type fieldExpectation struct {
	name string
	typ  reflect.Type
}

func assertExactStructFields(t *testing.T, rt reflect.Type, want []fieldExpectation) {
	t.Helper()
	if rt.NumField() != len(want) {
		t.Fatalf("%s field count mismatch: got %d want %d", rt.Name(), rt.NumField(), len(want))
	}
	for i, exp := range want {
		field := rt.Field(i)
		if field.Name != exp.name {
			t.Fatalf("%s field %d name mismatch: got %s want %s", rt.Name(), i, field.Name, exp.name)
		}
		if field.Type != exp.typ {
			t.Fatalf("%s.%s type mismatch: got %v want %v", rt.Name(), field.Name, field.Type, exp.typ)
		}
	}
}

func assertStructOmitsFields(t *testing.T, rt reflect.Type, forbidden []string) {
	t.Helper()
	for _, name := range forbidden {
		if _, ok := rt.FieldByName(name); ok {
			t.Fatalf("%s unexpectedly exposes field %s", rt.Name(), name)
		}
	}
}

func assertFieldType(t *testing.T, rt reflect.Type, fieldName string, want reflect.Type) {
	t.Helper()
	field, ok := rt.FieldByName(fieldName)
	if !ok {
		t.Fatalf("%s missing field %s", rt.Name(), fieldName)
	}
	if field.Type != want {
		t.Fatalf("%s.%s type mismatch: got %v want %v", rt.Name(), fieldName, field.Type, want)
	}
}

func int64Ptr(v int64) *int64 {
	return &v
}
