package engine_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/internal/testutil/backendtest"
)

func TestEngineMutationStoreRemoveAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		fixture := newMutationBackendFixture(t, backend)
		payloadA := []byte("phase9 deterministic payload A")
		payloadB := []byte("phase9 replacement payload B")

		storedA := fixture.store(t, "docs/a.txt", payloadA)
		storedB := fixture.store(t, "docs/b.txt", payloadA)
		storedC := fixture.store(t, "docs/c.txt", payloadA)
		for _, result := range []engine.StoreResult{storedA, storedB, storedC} {
			assertStoreResultShape(t, result)
		}
		if storedA.AlreadyStored || !storedB.AlreadyStored || !storedC.AlreadyStored {
			t.Fatalf("unexpected Store dedup flags: A=%+v B=%+v C=%+v", storedA, storedB, storedC)
		}
		if storedA.LogicalFileID != storedB.LogicalFileID ||
			storedA.LogicalFileID != storedC.LogicalFileID ||
			storedA.FileHash != storedB.FileHash ||
			storedA.FileHash != storedC.FileHash {
			t.Fatalf("identical content did not share logical identity: A=%+v B=%+v C=%+v", storedA, storedB, storedC)
		}

		if err := os.WriteFile(
			filepath.Join(fixture.inputRoot, "docs", "b.txt"),
			payloadB,
			0o600,
		); err != nil {
			t.Fatalf("rewrite replacement source: %v", err)
		}
		replacement := fixture.storeExisting(t, "docs/b.txt")
		assertStoreResultShape(t, replacement)
		if replacement.AlreadyStored || replacement.LogicalFileID == storedA.LogicalFileID ||
			replacement.FileHash == storedA.FileHash {
			t.Fatalf("replacement did not retarget the physical path: %+v", replacement)
		}
		empty := fixture.store(t, "empty.txt", nil)
		assertStoreResultShape(t, empty)
		if empty.AlreadyStored {
			t.Fatalf("first empty-file Store unexpectedly deduplicated: %+v", empty)
		}
		fixture.finalize(t)

		if got := mutationBackendInt64(t, backend.DB,
			`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, storedA.LogicalFileID); got == 0 {
			t.Fatal("non-empty Store produced no recipe entries")
		}
		storedAChunkHash := mutationBackendString(t, backend.DB, `
			SELECT c.chunk_hash
			FROM file_chunk fc
			JOIN chunk c ON c.id = fc.chunk_id
			WHERE fc.logical_file_id = $1
			ORDER BY fc.chunk_order
			LIMIT 1`, storedA.LogicalFileID)
		if got := mutationBackendInt64(t, backend.DB,
			`SELECT live_ref_count FROM chunk WHERE chunk_hash = $1`, storedAChunkHash); got != 1 {
			t.Fatalf("deduplicated content live_ref_count: got %d want 1", got)
		}
		if got := mutationBackendInt64(t, backend.DB,
			`SELECT ref_count FROM logical_file WHERE id = $1`, storedA.LogicalFileID); got != 2 {
			t.Fatalf("replacement-adjusted logical ref_count: got %d want 2", got)
		}
		if got := mutationBackendString(t, backend.DB, `
			SELECT lf.file_hash
			FROM physical_file pf
			JOIN logical_file lf ON lf.id = pf.logical_file_id
			WHERE pf.path = $1`, "docs/b.txt"); got != replacement.FileHash {
			t.Fatalf("replacement mapping hash: got %q want %q", got, replacement.FileHash)
		}
		if got := mutationBackendInt64(t, backend.DB,
			`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, empty.LogicalFileID); got != 0 {
			t.Fatalf("empty file recipe count: got %d want 0", got)
		}
		removePath := fixture.useAbsoluteStoredPath(t, "docs/c.txt")

		beforeDryRun := captureMutationRepositoryFingerprint(t, backend.DB, fixture.containerDir)
		dryRun, err := fixture.engine.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
			StoredPaths: []string{removePath, " " + removePath + " ", ""},
			DryRun:      true,
		})
		if err != nil {
			t.Fatalf("RemoveStoredPaths dry-run: %v", err)
		}
		if dryRun.ExecutionMode != engine.ExecutionModeSequential ||
			dryRun.Summary != (engine.BatchSummary{OK: 1, Failed: 1, Skipped: 1}) ||
			len(dryRun.Items) != 3 ||
			dryRun.Items[0].Status != engine.BatchItemPlanned ||
			dryRun.Items[1].Status != engine.BatchItemSkipped ||
			dryRun.Items[2].Status != engine.BatchItemFailed {
			t.Fatalf("unexpected stored-path dry-run result: %+v", dryRun)
		}
		assertMutationFingerprintEqual(
			t,
			beforeDryRun,
			captureMutationRepositoryFingerprint(t, backend.DB, fixture.containerDir),
		)

		unlinked, err := fixture.engine.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
			StoredPaths: []string{removePath},
		})
		if err != nil || len(unlinked.Items) != 1 ||
			unlinked.Items[0].Status != engine.BatchItemOK ||
			!unlinked.Items[0].MappingRemoved ||
			unlinked.Items[0].RemainingRefCount != 1 {
			t.Fatalf("RemoveStoredPaths live: got (%+v, %v)", unlinked, err)
		}
		beforeRemoveDryRun := captureMutationRepositoryFingerprint(t, backend.DB, fixture.containerDir)
		removeDryRun, err := fixture.engine.Remove(context.Background(), engine.RemoveRequest{
			FileIDs: []int64{storedA.LogicalFileID},
			DryRun:  true,
		})
		if err != nil || !removeDryRun.DryRun ||
			removeDryRun.ExecutionMode != engine.ExecutionModeSequential ||
			removeDryRun.Summary != (engine.BatchSummary{OK: 1}) ||
			len(removeDryRun.Items) != 1 ||
			removeDryRun.Items[0].Status != engine.BatchItemOK ||
			removeDryRun.Items[0].LogicalFileRemoved {
			t.Fatalf("Remove dry-run: got (%+v, %v)", removeDryRun, err)
		}
		assertMutationFingerprintEqual(
			t,
			beforeRemoveDryRun,
			captureMutationRepositoryFingerprint(t, backend.DB, fixture.containerDir),
		)
		containerBeforeRemove := mutationFileManifest(t, fixture.containerDir)
		removed, err := fixture.engine.Remove(context.Background(), engine.RemoveRequest{
			FileIDs: []int64{storedA.LogicalFileID},
		})
		if err != nil || len(removed.Items) != 1 ||
			removed.ExecutionMode != engine.ExecutionModeSequential ||
			removed.Summary != (engine.BatchSummary{OK: 1}) ||
			removed.Items[0].Status != engine.BatchItemOK ||
			!removed.Items[0].LogicalFileRemoved ||
			removed.Items[0].RemovedChunkAssociations == 0 {
			t.Fatalf("Remove by ID: got (%+v, %v)", removed, err)
		}
		if got := mutationBackendInt64(t, backend.DB,
			`SELECT COUNT(*) FROM logical_file WHERE id = $1`, storedA.LogicalFileID); got != 0 {
			t.Fatalf("removed logical file count: got %d want 0", got)
		}
		if got := mutationBackendInt64(t, backend.DB,
			`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, storedA.LogicalFileID); got != 0 {
			t.Fatalf("removed logical recipe rows remain: %d", got)
		}
		if got := mutationBackendInt64(t, backend.DB,
			`SELECT live_ref_count FROM chunk WHERE chunk_hash = $1`, storedAChunkHash); got != 0 {
			t.Fatalf("removed logical chunk live_ref_count: got %d want 0", got)
		}
		if got := mutationBackendInt64(t, backend.DB,
			`SELECT COUNT(*) FROM physical_file WHERE path IN ($1, $2)`, "docs/a.txt", removePath); got != 0 {
			t.Fatalf("removed mappings remain: %d", got)
		}
		if got := mutationBackendInt64(t, backend.DB,
			`SELECT COUNT(*) FROM physical_file WHERE path = $1`, "docs/b.txt"); got != 1 {
			t.Fatalf("replacement mapping was disturbed: %d", got)
		}
		if after := mutationFileManifest(t, fixture.containerDir); !reflect.DeepEqual(containerBeforeRemove, after) {
			t.Fatalf("Remove deleted or rewrote payload containers:\nbefore=%v\nafter=%v", containerBeforeRemove, after)
		}
	})
}

func TestEngineMutationSnapshotLifecycleAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		fixture := newMutationBackendFixture(t, backend)
		fixture.store(t, "docs/a.txt", []byte("snapshot lifecycle A"))
		fixture.store(t, "docs/sub/b.txt", []byte("snapshot lifecycle B"))
		fixture.store(t, "img/c.txt", []byte("snapshot lifecycle C"))
		fixture.finalize(t)
		eng := fixture.readEngine(t)

		root, err := eng.SnapshotCreate(context.Background(), engine.SnapshotCreateRequest{
			ID: "phase9-root", Label: "root",
		})
		if err != nil || root.SnapshotID != "phase9-root" ||
			root.Type != engine.SnapshotTypeFull || root.FilesInserted != 3 ||
			root.PathsCount != 0 || root.Label != "root" {
			t.Fatalf("SnapshotCreate root: got (%+v, %v)", root, err)
		}
		child, err := eng.SnapshotCreate(context.Background(), engine.SnapshotCreateRequest{
			ID: "phase9-child", Label: "child", ParentID: "phase9-root",
		})
		if err != nil || child.SnapshotID != "phase9-child" ||
			child.Type != engine.SnapshotTypeFull || child.FilesInserted != 3 ||
			child.ParentID != "phase9-root" {
			t.Fatalf("SnapshotCreate child: got (%+v, %v)", child, err)
		}
		partial, err := eng.SnapshotCreate(context.Background(), engine.SnapshotCreateRequest{
			ID: "phase9-partial", Label: "partial",
			Paths: []string{"docs/", "docs/a.txt", "docs/"},
		})
		if err != nil || partial.SnapshotID != "phase9-partial" ||
			partial.Type != engine.SnapshotTypePartial ||
			partial.PathsCount != 3 || partial.FilesInserted != 2 {
			t.Fatalf("SnapshotCreate partial: got (%+v, %v)", partial, err)
		}
		if got := mutationBackendRows(t, backend.DB, `
			SELECT sp.path
			FROM snapshot_file sf
			JOIN snapshot_path sp ON sp.id = sf.path_id
			WHERE sf.snapshot_id = $1
			ORDER BY sp.path`, "phase9-partial"); !reflect.DeepEqual(got, []string{"docs/a.txt", "docs/sub/b.txt"}) {
			t.Fatalf("partial membership: got %v", got)
		}

		beforePreview := captureMutationRepositoryFingerprint(t, backend.DB, fixture.containerDir)
		preview, err := eng.SnapshotDelete(context.Background(), engine.SnapshotDeleteRequest{
			SnapshotID: "phase9-root",
			Mode:       engine.SnapshotDeleteModePreview,
		})
		if err != nil || preview.Deleted || preview.Preview == nil ||
			preview.Mode != engine.SnapshotDeleteModePreview ||
			preview.Preview.Parent != (engine.SnapshotDeleteParent{State: engine.SnapshotDeleteParentNone}) ||
			!reflect.DeepEqual(preview.Preview.Children, []string{"phase9-child"}) ||
			preview.Preview.TotalFiles != 3 ||
			preview.Preview.UniqueFiles != 0 ||
			preview.Preview.SharedFiles != 3 {
			t.Fatalf("SnapshotDelete preview: got (%+v, %v)", preview, err)
		}
		assertMutationFingerprintEqual(
			t,
			beforePreview,
			captureMutationRepositoryFingerprint(t, backend.DB, fixture.containerDir),
		)

		contentBeforeDelete := mutationBackendRows(t, backend.DB, `
			SELECT lf.file_hash, lf.ref_count, c.chunk_hash, c.live_ref_count
			FROM logical_file lf
			LEFT JOIN file_chunk fc ON fc.logical_file_id = lf.id
			LEFT JOIN chunk c ON c.id = fc.chunk_id
			ORDER BY lf.file_hash, fc.chunk_order`)
		containerBeforeDelete := mutationFileManifest(t, fixture.containerDir)
		deleted, err := eng.SnapshotDelete(context.Background(), engine.SnapshotDeleteRequest{
			SnapshotID: "phase9-root",
			Mode:       engine.SnapshotDeleteModeExecute,
		})
		if err != nil || deleted != (engine.SnapshotDeleteResult{
			SnapshotID: "phase9-root",
			Mode:       engine.SnapshotDeleteModeExecute,
			Deleted:    true,
		}) {
			t.Fatalf("SnapshotDelete execute: got (%+v, %v)", deleted, err)
		}
		if got := mutationBackendInt64(t, backend.DB,
			`SELECT COUNT(*) FROM snapshot WHERE id = $1`, "phase9-root"); got != 0 {
			t.Fatalf("deleted snapshot remains: %d", got)
		}
		if got := mutationBackendInt64(t, backend.DB,
			`SELECT COUNT(*) FROM snapshot_file WHERE snapshot_id = $1`, "phase9-root"); got != 0 {
			t.Fatalf("deleted snapshot membership remains: %d", got)
		}
		var parentID any
		if err := backend.DB.QueryRowContext(context.Background(),
			`SELECT parent_id FROM snapshot WHERE id = $1`, "phase9-child").Scan(&parentID); err != nil {
			t.Fatalf("query child parent after delete: %v", err)
		}
		if parentID != nil {
			t.Fatalf("child parent was not cleared: %v", parentID)
		}
		contentAfterDelete := mutationBackendRows(t, backend.DB, `
			SELECT lf.file_hash, lf.ref_count, c.chunk_hash, c.live_ref_count
			FROM logical_file lf
			LEFT JOIN file_chunk fc ON fc.logical_file_id = lf.id
			LEFT JOIN chunk c ON c.id = fc.chunk_id
			ORDER BY lf.file_hash, fc.chunk_order`)
		if !reflect.DeepEqual(contentBeforeDelete, contentAfterDelete) {
			t.Fatalf("snapshot delete changed content graph:\nbefore=%v\nafter=%v", contentBeforeDelete, contentAfterDelete)
		}
		if after := mutationFileManifest(t, fixture.containerDir); !reflect.DeepEqual(containerBeforeDelete, after) {
			t.Fatalf("snapshot delete changed containers:\nbefore=%v\nafter=%v", containerBeforeDelete, after)
		}
	})
}

func TestEngineMutationRestoreAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		fixture := newMutationBackendFixture(t, backend)
		payloadA := []byte("phase9 restore payload A")
		payloadB := []byte("phase9 restore payload B")
		storedA := fixture.store(t, "docs/a.txt", payloadA)
		storedB := fixture.store(t, "docs/b.txt", payloadB)
		fixture.finalize(t)
		eng := fixture.readEngine(t)
		if _, err := eng.SnapshotCreate(context.Background(), engine.SnapshotCreateRequest{
			ID: "phase9-restore-snapshot",
		}); err != nil {
			t.Fatalf("create restore snapshot: %v", err)
		}
		storedPathTarget := fixture.useAbsoluteStoredPath(t, "docs/b.txt")
		before := captureMutationRepositoryFingerprint(t, backend.DB, fixture.containerDir)

		byIDRoot := filepath.Join(t.TempDir(), "by-id")
		byID, err := eng.Restore(context.Background(), engine.RestoreRequest{
			FileIDs: []int64{storedA.LogicalFileID}, DestinationRoot: byIDRoot,
			Overwrite: true,
		})
		wantByIDPath := filepath.Join(byIDRoot, "a.txt")
		if err != nil || byID.ExecutionMode != engine.ExecutionModeSequential ||
			byID.Summary != (engine.BatchSummary{OK: 1}) ||
			len(byID.Items) != 1 || byID.Items[0].Status != engine.BatchItemOK ||
			byID.Items[0].DestinationPath != wantByIDPath ||
			byID.Items[0].RestoredHash != storedA.FileHash {
			t.Fatalf("Restore by ID: got (%+v, %v)", byID, err)
		}
		assertMutationFile(t, wantByIDPath, payloadA)
		assertMutationFileManifest(t, byIDRoot, []string{
			mutationManifestEntry("a.txt", payloadA),
		})

		storedPathRoot := filepath.Join(t.TempDir(), "stored-path")
		storedPath, err := eng.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
			StoredPath: storedPathTarget, DestinationMode: engine.RestoreDestinationPrefix,
			DestinationRoot: storedPathRoot, Overwrite: true, NoMetadata: true,
		})
		wantStoredPath := filepath.Join(
			storedPathRoot,
			strings.TrimPrefix(filepath.Clean(storedPathTarget), string(filepath.Separator)),
		)
		if err != nil || storedPath.StoredPath != storedPathTarget ||
			storedPath.FileID != storedB.LogicalFileID ||
			storedPath.DestinationMode != engine.RestoreDestinationPrefix ||
			storedPath.DestinationPath != wantStoredPath ||
			storedPath.RestoredHash != storedB.FileHash {
			t.Fatalf("RestoreStoredPath: got (%+v, %v)", storedPath, err)
		}
		assertMutationFile(t, wantStoredPath, payloadB)
		storedPathRelative, err := filepath.Rel(storedPathRoot, wantStoredPath)
		if err != nil {
			t.Fatalf("derive stored-path destination relative path: %v", err)
		}
		assertMutationFileManifest(t, storedPathRoot, []string{
			mutationManifestEntry(storedPathRelative, payloadB),
		})

		snapshotRoot := filepath.Join(t.TempDir(), "snapshot")
		snapshotResult, err := eng.SnapshotRestore(context.Background(), engine.SnapshotRestoreRequest{
			SnapshotID: "phase9-restore-snapshot",
			Selection: engine.SnapshotRestoreSelection{
				ExactPaths: []string{"docs/b.txt", "docs/a.txt", "docs/a.txt"},
				Prefixes:   []string{"docs/", "docs/"},
			},
			Destination: engine.SnapshotRestoreDestination{
				Mode: engine.SnapshotRestoreDestinationOriginal,
				Path: snapshotRoot,
			},
			Overwrite: true,
			Metadata:  engine.SnapshotRestoreMetadataNone,
		})
		wantSnapshotPaths := []string{
			filepath.Join(snapshotRoot, "docs", "a.txt"),
			filepath.Join(snapshotRoot, "docs", "b.txt"),
		}
		if err != nil || snapshotResult.SnapshotID != "phase9-restore-snapshot" ||
			snapshotResult.DestinationMode != engine.SnapshotRestoreDestinationOriginal ||
			snapshotResult.RequestedPathsCount != 0 ||
			snapshotResult.RestoredFiles != 2 ||
			snapshotResult.OutputTarget != snapshotRoot ||
			!reflect.DeepEqual(snapshotResult.OutputPaths, wantSnapshotPaths) ||
			len(snapshotResult.Warnings) != 0 {
			t.Fatalf("SnapshotRestore: got (%+v, %v)", snapshotResult, err)
		}
		assertMutationFile(t, wantSnapshotPaths[0], payloadA)
		assertMutationFile(t, wantSnapshotPaths[1], payloadB)
		assertMutationFileManifest(t, snapshotRoot, []string{
			mutationManifestEntry(filepath.Join("docs", "a.txt"), payloadA),
			mutationManifestEntry(filepath.Join("docs", "b.txt"), payloadB),
		})

		assertMutationFingerprintEqual(
			t,
			before,
			captureMutationRepositoryFingerprint(t, backend.DB, fixture.containerDir),
		)
		if pins := mutationBackendInt64(t, backend.DB,
			`SELECT COALESCE(SUM(pin_count), 0) FROM chunk`); pins != 0 {
			t.Fatalf("restore left chunk pins: %d", pins)
		}
	})
}

func TestEngineMutationErrorsAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		fixture := newMutationBackendFixture(t, backend)
		retained := fixture.store(t, "retained.txt", []byte("phase9 retained payload"))
		fixture.finalize(t)
		eng := fixture.readEngine(t)
		if _, err := eng.SnapshotCreate(context.Background(), engine.SnapshotCreateRequest{
			ID: "phase9-retained",
		}); err != nil {
			t.Fatalf("create retention snapshot: %v", err)
		}
		retainedPath := fixture.useAbsoluteStoredPath(t, "retained.txt")
		baseline := captureMutationRepositoryFingerprint(t, backend.DB, fixture.containerDir)

		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := fixture.engine.Store(cancelled, engine.StoreRequest{
			SourcePath: "retained.txt", Codec: "plain",
		}); !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-cancelled Store: %v", err)
		}
		cancelledOperations := []struct {
			name string
			call func() error
		}{
			{"Remove", func() error {
				_, err := eng.Remove(cancelled, engine.RemoveRequest{FileIDs: []int64{retained.LogicalFileID}})
				return err
			}},
			{"RemoveStoredPaths", func() error {
				_, err := eng.RemoveStoredPaths(cancelled, engine.RemoveStoredPathsRequest{StoredPaths: []string{retainedPath}})
				return err
			}},
			{"Restore", func() error {
				_, err := eng.Restore(cancelled, engine.RestoreRequest{
					FileIDs: []int64{retained.LogicalFileID}, DestinationRoot: t.TempDir(),
				})
				return err
			}},
			{"RestoreStoredPath", func() error {
				_, err := eng.RestoreStoredPath(cancelled, engine.RestoreStoredPathRequest{
					StoredPath: retainedPath, DestinationMode: engine.RestoreDestinationPrefix,
					DestinationRoot: t.TempDir(),
				})
				return err
			}},
			{"SnapshotCreate", func() error {
				_, err := eng.SnapshotCreate(cancelled, engine.SnapshotCreateRequest{ID: "phase9-cancelled"})
				return err
			}},
			{"SnapshotDelete", func() error {
				_, err := eng.SnapshotDelete(cancelled, engine.SnapshotDeleteRequest{
					SnapshotID: "phase9-retained", Mode: engine.SnapshotDeleteModePreview,
				})
				return err
			}},
			{"SnapshotRestore", func() error {
				_, err := eng.SnapshotRestore(cancelled, engine.SnapshotRestoreRequest{
					SnapshotID: "phase9-retained",
					Destination: engine.SnapshotRestoreDestination{
						Mode: engine.SnapshotRestoreDestinationOriginal,
						Path: t.TempDir(),
					},
				})
				return err
			}},
		}
		for _, operation := range cancelledOperations {
			t.Run("pre-cancelled "+operation.name, func(t *testing.T) {
				if err := operation.call(); !errors.Is(err, context.Canceled) {
					t.Fatalf("%s: expected context cancellation, got %v", operation.name, err)
				}
			})
		}
		if _, err := fixture.engine.Store(context.Background(), engine.StoreRequest{
			SourcePath: "retained.txt", Recursive: true,
		}); !errors.Is(err, engine.ErrNotImplemented) || !engine.IsUnsupported(err) {
			t.Fatalf("recursive Store classification: %v", err)
		}
		if _, err := fixture.engine.Store(context.Background(), engine.StoreRequest{
			SourcePath: " ",
		}); err == nil || engine.IsUnsupported(err) {
			t.Fatalf("blank-path Store classification: %v", err)
		}
		if _, err := fixture.engine.Store(context.Background(), engine.StoreRequest{
			SourcePath: "phase9-missing-source.txt", Codec: "plain",
		}); err == nil || engine.IsUnsupported(err) {
			t.Fatalf("missing-source Store classification: %v", err)
		}
		if _, err := fixture.engine.Store(context.Background(), engine.StoreRequest{
			SourcePath: "retained.txt", Codec: "invalid-codec",
		}); err == nil || engine.IsUnsupported(err) {
			t.Fatalf("invalid-codec Store classification: %v", err)
		}
		assertMutationFingerprintEqual(
			t,
			baseline,
			captureMutationRepositoryFingerprint(t, backend.DB, fixture.containerDir),
		)

		remove, err := eng.Remove(context.Background(), engine.RemoveRequest{
			FileIDs: []int64{retained.LogicalFileID},
		})
		if err != nil || len(remove.Items) != 1 ||
			remove.Items[0].Status != engine.BatchItemFailed ||
			remove.Items[0].InvariantCode != invariants.CodeSnapshotRetainedDeleteBlocked ||
			remove.Items[0].RecommendedAction == "" {
			t.Fatalf("snapshot-retained Remove: got (%+v, %v)", remove, err)
		}
		unlink, err := eng.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{
			StoredPaths: []string{retainedPath},
		})
		if err != nil || len(unlink.Items) != 1 ||
			unlink.Items[0].Status != engine.BatchItemFailed ||
			unlink.Items[0].InvariantCode != invariants.CodeSnapshotRetainedDeleteBlocked ||
			unlink.Items[0].RecommendedAction == "" {
			t.Fatalf("snapshot-retained RemoveStoredPaths: got (%+v, %v)", unlink, err)
		}
		assertMutationFingerprintEqual(
			t,
			baseline,
			captureMutationRepositoryFingerprint(t, backend.DB, fixture.containerDir),
		)

		for _, operation := range []struct {
			name string
			call func() error
			want string
		}{
			{
				name: "duplicate snapshot",
				call: func() error {
					_, err := eng.SnapshotCreate(context.Background(), engine.SnapshotCreateRequest{ID: "phase9-retained"})
					return err
				},
				want: "insert snapshot id=phase9-retained",
			},
			{
				name: "missing snapshot parent",
				call: func() error {
					_, err := eng.SnapshotCreate(context.Background(), engine.SnapshotCreateRequest{
						ID: "phase9-orphan", ParentID: "missing-parent",
					})
					return err
				},
				want: "parent snapshot",
			},
			{
				name: "missing snapshot create path",
				call: func() error {
					_, err := eng.SnapshotCreate(context.Background(), engine.SnapshotCreateRequest{
						ID: "phase9-missing-path", Paths: []string{"missing.txt"},
					})
					return err
				},
				want: "path not found",
			},
			{
				name: "missing snapshot delete",
				call: func() error {
					_, err := eng.SnapshotDelete(context.Background(), engine.SnapshotDeleteRequest{
						SnapshotID: "missing-snapshot", Mode: engine.SnapshotDeleteModeExecute,
					})
					return err
				},
				want: "not found",
			},
			{
				name: "invalid snapshot delete mode",
				call: func() error {
					_, err := eng.SnapshotDelete(context.Background(), engine.SnapshotDeleteRequest{
						SnapshotID: "phase9-retained", Mode: engine.SnapshotDeleteMode("invalid"),
					})
					return err
				},
				want: "unknown snapshot delete mode",
			},
			{
				name: "missing snapshot restore",
				call: func() error {
					_, err := eng.SnapshotRestore(context.Background(), engine.SnapshotRestoreRequest{
						SnapshotID: "missing-snapshot",
						Destination: engine.SnapshotRestoreDestination{
							Mode: engine.SnapshotRestoreDestinationOriginal,
							Path: filepath.Join(t.TempDir(), "missing-snapshot"),
						},
					})
					return err
				},
				want: "not found",
			},
			{
				name: "missing snapshot restore path",
				call: func() error {
					_, err := eng.SnapshotRestore(context.Background(), engine.SnapshotRestoreRequest{
						SnapshotID: "phase9-retained",
						Paths:      []string{"missing.txt"},
						Destination: engine.SnapshotRestoreDestination{
							Mode: engine.SnapshotRestoreDestinationOriginal,
							Path: filepath.Join(t.TempDir(), "missing-path"),
						},
					})
					return err
				},
				want: "path not found",
			},
			{
				name: "invalid snapshot restore regex",
				call: func() error {
					_, err := eng.SnapshotRestore(context.Background(), engine.SnapshotRestoreRequest{
						SnapshotID: "phase9-retained",
						Selection:  engine.SnapshotRestoreSelection{Regex: "("},
						Destination: engine.SnapshotRestoreDestination{
							Mode: engine.SnapshotRestoreDestinationOriginal,
							Path: filepath.Join(t.TempDir(), "invalid-regex"),
						},
					})
					return err
				},
				want: "invalid snapshot restore regex",
			},
		} {
			t.Run(operation.name, func(t *testing.T) {
				err := operation.call()
				if err == nil || !strings.Contains(err.Error(), operation.want) ||
					engine.IsUnsupported(err) {
					t.Fatalf("expected stable error containing %q, got %v", operation.want, err)
				}
			})
		}
		assertMutationFingerprintEqual(
			t,
			baseline,
			captureMutationRepositoryFingerprint(t, backend.DB, fixture.containerDir),
		)

		missingRestore, err := eng.Restore(context.Background(), engine.RestoreRequest{
			FileIDs: []int64{-1}, DestinationRoot: filepath.Join(t.TempDir(), "missing"),
		})
		if err != nil || len(missingRestore.Items) != 1 ||
			missingRestore.Items[0].Status != engine.BatchItemFailed ||
			missingRestore.Summary != (engine.BatchSummary{Failed: 1}) {
			t.Fatalf("missing Restore item: got (%+v, %v)", missingRestore, err)
		}
		failFastRestore, err := eng.Restore(context.Background(), engine.RestoreRequest{
			FileIDs: []int64{-1, retained.LogicalFileID}, DestinationRoot: filepath.Join(t.TempDir(), "fail-fast"),
			FailFast: true,
		})
		if err != nil || len(failFastRestore.Items) != 1 ||
			failFastRestore.Items[0].Status != engine.BatchItemFailed ||
			failFastRestore.Summary != (engine.BatchSummary{Failed: 1, Skipped: 1}) {
			t.Fatalf("fail-fast Restore: got (%+v, %v)", failFastRestore, err)
		}
		failFastRemove, err := eng.Remove(context.Background(), engine.RemoveRequest{
			FileIDs: []int64{-1, retained.LogicalFileID}, FailFast: true,
		})
		if err != nil || len(failFastRemove.Items) != 1 ||
			failFastRemove.Items[0].Status != engine.BatchItemFailed ||
			failFastRemove.Summary != (engine.BatchSummary{Failed: 1, Skipped: 1}) {
			t.Fatalf("fail-fast Remove: got (%+v, %v)", failFastRemove, err)
		}
		if result, err := eng.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
			StoredPath: "missing.txt", DestinationMode: engine.RestoreDestinationPrefix,
			DestinationRoot: filepath.Join(t.TempDir(), "missing-path"),
			NoMetadata:      true,
		}); err == nil || result != (engine.RestoreStoredPathResult{}) {
			t.Fatalf("missing RestoreStoredPath: got (%+v, %v)", result, err)
		}
		collisionRoot := filepath.Join(t.TempDir(), "collision")
		if err := os.MkdirAll(collisionRoot, 0o700); err != nil {
			t.Fatalf("create collision root: %v", err)
		}
		collisionPath := filepath.Join(collisionRoot, "retained.txt")
		collisionBytes := []byte("preserve collision")
		if err := os.WriteFile(collisionPath, collisionBytes, 0o600); err != nil {
			t.Fatalf("write collision fixture: %v", err)
		}
		collision, err := eng.Restore(context.Background(), engine.RestoreRequest{
			FileIDs: []int64{retained.LogicalFileID}, DestinationRoot: collisionRoot,
		})
		if err != nil || len(collision.Items) != 1 ||
			collision.Items[0].Status != engine.BatchItemFailed ||
			collision.Summary != (engine.BatchSummary{Failed: 1}) {
			t.Fatalf("Restore collision: got (%+v, %v)", collision, err)
		}
		assertMutationFile(t, collisionPath, collisionBytes)
		assertMutationFileManifest(t, collisionRoot, []string{
			mutationManifestEntry("retained.txt", collisionBytes),
		})

		snapshotCollisionRoot := filepath.Join(t.TempDir(), "snapshot-collision")
		if err := os.MkdirAll(snapshotCollisionRoot, 0o700); err != nil {
			t.Fatalf("create snapshot collision root: %v", err)
		}
		snapshotCollisionPath := filepath.Join(snapshotCollisionRoot, "retained.txt")
		snapshotCollisionBytes := []byte("preserve snapshot collision")
		if err := os.WriteFile(snapshotCollisionPath, snapshotCollisionBytes, 0o600); err != nil {
			t.Fatalf("write snapshot collision fixture: %v", err)
		}
		if result, err := eng.SnapshotRestore(context.Background(), engine.SnapshotRestoreRequest{
			SnapshotID: "phase9-retained",
			Destination: engine.SnapshotRestoreDestination{
				Mode: engine.SnapshotRestoreDestinationOriginal,
				Path: snapshotCollisionRoot,
			},
			Metadata: engine.SnapshotRestoreMetadataNone,
		}); err == nil || !reflect.DeepEqual(result, engine.SnapshotRestoreResult{}) {
			t.Fatalf("SnapshotRestore collision: got (%+v, %v)", result, err)
		}
		assertMutationFile(t, snapshotCollisionPath, snapshotCollisionBytes)
		assertMutationFileManifest(t, snapshotCollisionRoot, []string{
			mutationManifestEntry("retained.txt", snapshotCollisionBytes),
		})

		partialRoot := filepath.Join(t.TempDir(), "partial")
		partial, err := eng.Restore(context.Background(), engine.RestoreRequest{
			FileIDs: []int64{retained.LogicalFileID, -1}, DestinationRoot: partialRoot,
		})
		if err != nil || len(partial.Items) != 2 ||
			partial.Items[0].Status != engine.BatchItemOK ||
			partial.Items[1].Status != engine.BatchItemFailed ||
			partial.Summary != (engine.BatchSummary{OK: 1, Failed: 1}) {
			t.Fatalf("partial Restore batch: got (%+v, %v)", partial, err)
		}
		assertMutationFile(t, filepath.Join(partialRoot, "retained.txt"), []byte("phase9 retained payload"))
		assertMutationFileManifest(t, partialRoot, []string{
			mutationManifestEntry("retained.txt", []byte("phase9 retained payload")),
		})
		assertMutationFingerprintEqual(
			t,
			baseline,
			captureMutationRepositoryFingerprint(t, backend.DB, fixture.containerDir),
		)

		fixture.restartWriter(t)
		mismatch := fixture.store(t, "mismatch.txt", []byte("phase9 mismatch payload"))
		fixture.finalize(t)
		mismatchPath := fixture.useAbsoluteStoredPath(t, "mismatch.txt")
		if _, err := backend.DB.ExecContext(context.Background(),
			`UPDATE logical_file SET ref_count = $1 WHERE id = $2`,
			5, mismatch.LogicalFileID); err != nil {
			t.Fatalf("inject ref-count mismatch: %v", err)
		}
		mismatchBaseline := captureMutationRepositoryFingerprint(t, backend.DB, fixture.containerDir)
		mismatchResult, err := fixture.readEngine(t).RemoveStoredPaths(
			context.Background(),
			engine.RemoveStoredPathsRequest{StoredPaths: []string{mismatchPath}},
		)
		if err != nil || len(mismatchResult.Items) != 1 ||
			mismatchResult.Items[0].Status != engine.BatchItemFailed ||
			mismatchResult.Items[0].InvariantCode != invariants.CodePhysicalGraphRefCountMismatch {
			t.Fatalf("ref-count mismatch rollback: got (%+v, %v)", mismatchResult, err)
		}
		assertMutationFingerprintEqual(
			t,
			mismatchBaseline,
			captureMutationRepositoryFingerprint(t, backend.DB, fixture.containerDir),
		)
	})
}

func TestEngineGCDryRunAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		fixture := newMutationBackendFixture(t, backend)
		fixture.store(t, "live.txt", []byte("phase9 live GC payload"))
		fixture.finalize(t)
		eng := fixture.readEngine(t)
		if _, err := eng.SnapshotCreate(context.Background(), engine.SnapshotCreateRequest{
			ID: "phase9-gc-live",
		}); err != nil {
			t.Fatalf("create GC reachability snapshot: %v", err)
		}
		const deadFilename = "phase9-gc-dead.bin"
		seedMutationDeadContainer(
			t,
			backend.DB,
			fixture.containerDir,
			deadFilename,
			[]byte("phase9 fixed dead GC payload"),
		)
		backend.DB.SetMaxOpenConns(4)
		before := captureMutationRepositoryFingerprint(t, backend.DB, fixture.containerDir)

		first, err := eng.GarbageCollect(context.Background(), engine.GarbageCollectRequest{
			DryRun: true, Workers: 7,
		})
		if err != nil {
			t.Fatalf("GarbageCollect dry-run: %v", err)
		}
		if !first.DryRun || first.AffectedContainers != 1 ||
			!reflect.DeepEqual(first.ContainerFilenames, []string{deadFilename}) ||
			first.SnapshotRetainedContainers != 0 ||
			first.SnapshotRetainedLogicalFiles != 1 ||
			first.CurrentOnlyRetainedLogicalFiles != 0 ||
			first.SnapshotOnlyRetainedLogicalFiles != 0 ||
			first.SharedRetainedLogicalFiles != 1 ||
			first.BytesReclaimed != 0 || first.Warnings != nil {
			t.Fatalf("unexpected GC dry-run result: %+v", first)
		}
		assertMutationFingerprintEqual(
			t,
			before,
			captureMutationRepositoryFingerprint(t, backend.DB, fixture.containerDir),
		)
		second, err := eng.GarbageCollect(context.Background(), engine.GarbageCollectRequest{DryRun: true})
		if err != nil || !reflect.DeepEqual(first, second) {
			t.Fatalf("repeated GC dry-run: first=%+v second=%+v err=%v", first, second, err)
		}
		assertMutationFingerprintEqual(
			t,
			before,
			captureMutationRepositoryFingerprint(t, backend.DB, fixture.containerDir),
		)

		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := eng.GarbageCollect(cancelled, engine.GarbageCollectRequest{
			DryRun: true,
		}); !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-cancelled GC dry-run: %v", err)
		}
		assertMutationFingerprintEqual(
			t,
			before,
			captureMutationRepositoryFingerprint(t, backend.DB, fixture.containerDir),
		)
	})
}
