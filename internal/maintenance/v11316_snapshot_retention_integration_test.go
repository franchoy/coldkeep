package maintenance_test

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/internal/testutil/backendtest"
)

func TestV11316SnapshotOnlyRetentionCanBeCreatedThroughNormalRemove(t *testing.T) {
	originalContainerMax := container.GetContainerMaxSize()
	container.SetContainerMaxSize(2 * 1024 * 1024)
	t.Cleanup(func() { container.SetContainerMaxSize(originalContainerMax) })

	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		ctx := context.Background()
		captureRoot := filepath.Join(t.TempDir(), "capture")
		sourcePath := filepath.Join(captureRoot, "docs", "a.bin")
		unreachablePath := filepath.Join(captureRoot, "docs", "unreachable.bin")
		currentPath := filepath.Join(captureRoot, "docs", "current.bin")
		pinnedPath := filepath.Join(captureRoot, "docs", "pinned.bin")
		if err := os.MkdirAll(filepath.Dir(sourcePath), 0o700); err != nil {
			t.Fatalf("CK-V11316-001: create capture root: %v", err)
		}
		original := bytes.Repeat([]byte("CK-V11316-001 deterministic snapshot-only retention payload"), 40000)
		if err := os.WriteFile(sourcePath, original, 0o600); err != nil {
			t.Fatalf("CK-V11316-001: write source: %v", err)
		}
		unreachableBytes := bytes.Repeat([]byte("CK-V11316-008 deterministic unreachable control payload"), 80000)
		if err := os.WriteFile(unreachablePath, unreachableBytes, 0o600); err != nil {
			t.Fatalf("CK-V11316-008: write unreachable control: %v", err)
		}
		currentBytes := bytes.Repeat([]byte("CK-V11316-008 deterministic current-live control payload"), 1000)
		if err := os.WriteFile(currentPath, currentBytes, 0o600); err != nil {
			t.Fatalf("CK-V11316-008: write current-live control: %v", err)
		}
		pinnedBytes := bytes.Repeat([]byte("CK-V11316-008 deterministic pinned rootless control payload"), 80000)
		if err := os.WriteFile(pinnedPath, pinnedBytes, 0o600); err != nil {
			t.Fatalf("CK-V11316-008: write pinned control: %v", err)
		}

		containersDir := t.TempDir()
		writer := container.NewLocalWriterWithDirAndDB(
			containersDir,
			container.GetContainerMaxSize(),
			backend.DB,
		)
		storeContext := storage.StorageContext{
			DB:           backend.DB,
			Writer:       writer,
			ContainerDir: containersDir,
		}
		eng, err := engine.New(engine.Config{DB: backend.DB, ContainerDir: containersDir, StoreContext: &storeContext})
		if err != nil {
			t.Fatalf("CK-V11316-001: create Engine: %v", err)
		}

		codecName := os.Getenv("COLDKEEP_CODEC")
		if codecName == "" {
			codecName = string(blocks.CodecPlain)
		}
		codec, err := blocks.ParseCodec(codecName)
		if err != nil {
			t.Fatalf("CK-V11316-001: parse test codec %q: %v", codecName, err)
		}

		stored, err := storage.StoreFileWithStorageContextAndCodecResultContext(
			ctx,
			storeContext,
			sourcePath,
			codec,
		)
		if err != nil {
			t.Fatalf("CK-V11316-001: real Store: %v", err)
		}
		unreachable, err := storage.StoreFileWithStorageContextAndCodecResultContext(ctx, storeContext, unreachablePath, codec)
		if err != nil {
			t.Fatalf("CK-V11316-008: Store unreachable control: %v", err)
		}
		currentLive, err := storage.StoreFileWithStorageContextAndCodecResultContext(ctx, storeContext, currentPath, codec)
		if err != nil {
			t.Fatalf("CK-V11316-008: Store current-live control: %v", err)
		}
		pinnedRootless, err := storage.StoreFileWithStorageContextAndCodecResultContext(ctx, storeContext, pinnedPath, codec)
		if err != nil {
			t.Fatalf("CK-V11316-008: Store pinned control: %v", err)
		}
		unreachableRemove, err := eng.RemoveStoredPaths(ctx, engine.RemoveStoredPathsRequest{StoredPaths: []string{unreachable.Path}})
		if err != nil || len(unreachableRemove.Items) != 1 || unreachableRemove.Items[0].Status != engine.BatchItemOK {
			t.Fatalf("CK-V11316-008: unlink unreachable control: result=%+v err=%v", unreachableRemove, err)
		}
		pinnedRemove, err := eng.RemoveStoredPaths(ctx, engine.RemoveStoredPathsRequest{StoredPaths: []string{pinnedRootless.Path}})
		if err != nil || len(pinnedRemove.Items) != 1 || pinnedRemove.Items[0].Status != engine.BatchItemOK {
			t.Fatalf("CK-V11316-008: unlink pinned control: result=%+v err=%v", pinnedRemove, err)
		}

		snapshotIDs := []string{"v11316-phase6-remove-reproduction-a", "v11316-phase6-remove-reproduction-b"}
		for _, snapshotID := range snapshotIDs {
			created, err := eng.SnapshotCreate(ctx, engine.SnapshotCreateRequest{
				ID:            snapshotID,
				SelectionBase: captureRoot,
				Paths:         []string{"docs/a.bin"},
			})
			if err != nil {
				t.Fatalf("CK-V11316-001: real SnapshotCreate %s: %v", snapshotID, err)
			}
			if created.FilesInserted != 1 {
				t.Fatalf("CK-V11316-001: SnapshotCreate %s inserted %d files, want 1", snapshotID, created.FilesInserted)
			}
		}

		beforeCurrent := phase6Count(t, backend.DB,
			`SELECT COUNT(*) FROM physical_file WHERE logical_file_id = $1`, stored.FileID)
		beforeMembership := phase6Count(t, backend.DB,
			`SELECT COUNT(*) FROM snapshot_file WHERE logical_file_id = $1`, stored.FileID)
		pinCount := phase6Count(t, backend.DB,
			`SELECT COALESCE(SUM(ch.pin_count), 0) FROM file_chunk fc JOIN chunk ch ON ch.id = fc.chunk_id WHERE fc.logical_file_id = $1`, stored.FileID)
		if beforeCurrent != 1 || beforeMembership != 2 || pinCount != 0 {
			t.Fatalf("CK-V11316-001: invalid pre-Remove roots current=%d snapshot=%d pins=%d",
				beforeCurrent, beforeMembership, pinCount)
		}

		byIDResult, err := eng.Remove(ctx, engine.RemoveRequest{FileIDs: []int64{stored.FileID}})
		if err != nil {
			t.Fatalf("CK-V11316-001: by-ID Engine Remove: %v", err)
		}
		if len(byIDResult.Items) != 1 || byIDResult.Items[0].Status != engine.BatchItemFailed || byIDResult.Items[0].InvariantCode != invariants.CodeSnapshotRetainedDeleteBlocked {
			t.Fatalf("CK-V11316-001: by-ID snapshot guard weakened: %+v", byIDResult)
		}

		removeResult, removeErr := eng.RemoveStoredPaths(ctx, engine.RemoveStoredPathsRequest{StoredPaths: []string{stored.Path}})
		afterCurrent := phase6Count(t, backend.DB,
			`SELECT COUNT(*) FROM physical_file WHERE logical_file_id = $1`, stored.FileID)
		afterMembership := phase6Count(t, backend.DB,
			`SELECT COUNT(*) FROM snapshot_file WHERE logical_file_id = $1`, stored.FileID)
		if removeErr != nil {
			code, _ := invariants.Code(removeErr)
			t.Fatalf(
				"CK-V11316-001: normal Remove cannot construct required snapshot-only root: backend=%s code=%q err=%v result=%+v current_before=%d current_after=%d snapshot_before=%d snapshot_after=%d",
				backend.Name,
				code,
				removeErr,
				removeResult,
				beforeCurrent,
				afterCurrent,
				beforeMembership,
				afterMembership,
			)
		}
		if len(removeResult.Items) != 1 || removeResult.Items[0].Status != engine.BatchItemOK || !removeResult.Items[0].MappingRemoved || removeResult.Items[0].RemainingRefCount != 0 {
			t.Fatalf("CK-V11316-001: unexpected stored-path Engine/remove result: %+v", removeResult)
		}
		if afterCurrent != 0 || afterMembership != 2 {
			t.Fatalf("CK-V11316-001: invalid post-Remove roots current=%d snapshot=%d",
				afterCurrent, afterMembership)
		}
		if wrongLive := phase6Count(t, backend.DB, `
			SELECT COUNT(*)
			FROM file_chunk fc
			JOIN chunk ch ON ch.id = fc.chunk_id
			WHERE fc.logical_file_id = $1 AND ch.live_ref_count <> 0
		`, stored.FileID); wrongLive != 0 {
			t.Fatalf("CK-V11316-008: snapshot-only recipe has %d nonzero current-root chunk counters", wrongLive)
		}

		if err := writer.FinalizeContainer(); err != nil {
			t.Fatalf("CK-V11316-001: finalize writer before GC: %v", err)
		}

		plan, err := eng.GarbageCollect(ctx, engine.GarbageCollectRequest{DryRun: true})
		if err != nil {
			t.Fatalf("CK-V11316-001: snapshot-only GC plan: %v", err)
		}
		if plan.SnapshotOnlyRetainedLogicalFiles != 1 || plan.AffectedContainers == 0 || plan.BytesReclaimed == 0 {
			t.Fatalf("CK-V11316-001: snapshot-only/control plan classification mismatch: %+v", plan)
		}
		if phase6Count(t, backend.DB, `SELECT COUNT(*) FROM logical_file WHERE id = $1`, unreachable.FileID) != 1 {
			t.Fatal("CK-V11316-008: dry-run mutated unreachable logical recipe")
		}
		if phase6Count(t, backend.DB, `SELECT COUNT(*) FROM physical_file WHERE logical_file_id = $1`, currentLive.FileID) != 1 {
			t.Fatal("CK-V11316-008: current-live control lost its mapping during plan")
		}
		if backend.Capabilities.LiveGC {
			pinReady := make(chan struct{})
			pinRelease := make(chan struct{})
			var signalPin sync.Once
			var releasePin sync.Once
			releasePinnedRestore := func() { releasePin.Do(func() { close(pinRelease) }) }
			defer releasePinnedRestore()
			storage.ConfigureRestoreTestHooksForTesting(&storeContext, func(_ *sql.DB, _ int64) error {
				signalPin.Do(func() { close(pinReady) })
				<-pinRelease
				return nil
			}, nil)
			restoreErr := make(chan error, 1)
			pinnedRestorePath := filepath.Join(t.TempDir(), "pinned-control.restore")
			go func() {
				restoreErr <- storage.RestoreFileWithStorageContext(storeContext, pinnedRootless.FileID, pinnedRestorePath)
			}()
			select {
			case <-pinReady:
			case <-time.After(10 * time.Second):
				releasePinnedRestore()
				t.Fatal("CK-V11316-008: timeout waiting for active restore pin")
			}

			live, err := eng.GarbageCollect(ctx, engine.GarbageCollectRequest{})
			if err != nil {
				releasePinnedRestore()
				t.Fatalf("CK-V11316-001: snapshot-only live GC: %v", err)
			}
			if live.AffectedContainers == 0 || live.BytesReclaimed == 0 || live.SnapshotOnlyRetainedLogicalFiles != 1 {
				t.Fatalf("CK-V11316-001: live GC control classification mismatch: %+v", live)
			}
			if phase6Count(t, backend.DB, `SELECT COUNT(*) FROM logical_file WHERE id = $1`, unreachable.FileID) != 0 {
				t.Fatal("CK-V11316-008: live GC left unreachable control recipe")
			}
			if phase6Count(t, backend.DB, `SELECT COUNT(*) FROM physical_file WHERE logical_file_id = $1`, currentLive.FileID) != 1 {
				releasePinnedRestore()
				t.Fatal("CK-V11316-008: live GC removed current-live control")
			}
			if phase6Count(t, backend.DB, `SELECT COUNT(*) FROM logical_file WHERE id = $1`, pinnedRootless.FileID) != 1 {
				releasePinnedRestore()
				t.Fatal("CK-V11316-008: live GC removed actively pinned rootless recipe")
			}
			releasePinnedRestore()
			select {
			case err := <-restoreErr:
				if err != nil {
					t.Fatalf("CK-V11316-008: pinned restore completion: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("CK-V11316-008: timeout waiting for pinned restore completion")
			}
			storage.ConfigureRestoreTestHooksForTesting(&storeContext, nil, nil)
			if _, err := eng.GarbageCollect(ctx, engine.GarbageCollectRequest{}); err != nil {
				t.Fatalf("CK-V11316-008: live GC after pin release: %v", err)
			}
			if phase6Count(t, backend.DB, `SELECT COUNT(*) FROM logical_file WHERE id = $1`, pinnedRootless.FileID) != 0 {
				t.Fatal("CK-V11316-008: post-release live GC left pinned control recipe")
			}
		}

		restoreRoot := t.TempDir()
		restored, err := eng.SnapshotRestore(ctx, engine.SnapshotRestoreRequest{
			SnapshotID: snapshotIDs[1],
			Destination: engine.SnapshotRestoreDestination{
				Mode: engine.SnapshotRestoreDestinationPrefix,
				Path: restoreRoot,
			},
			Metadata: engine.SnapshotRestoreMetadataNone,
		})
		if err != nil {
			t.Fatalf("CK-V11316-001: restore snapshot after GC: %v", err)
		}
		if restored.RestoredFiles != 1 || len(restored.OutputPaths) != 1 {
			t.Fatalf("CK-V11316-001: unexpected snapshot restore result: %+v", restored)
		}
		restoredBytes, err := os.ReadFile(restored.OutputPaths[0])
		if err != nil {
			t.Fatalf("CK-V11316-001: read restored snapshot output: %v", err)
		}
		if !bytes.Equal(restoredBytes, original) {
			t.Fatalf("CK-V11316-001: post-GC snapshot restore bytes differ")
		}

		deleted, err := eng.SnapshotDelete(ctx, engine.SnapshotDeleteRequest{SnapshotID: snapshotIDs[0], Mode: engine.SnapshotDeleteModeExecute})
		if err != nil || !deleted.Deleted {
			t.Fatalf("CK-V11316-001: delete first snapshot: result=%+v err=%v", deleted, err)
		}
		stillRetained, err := eng.GarbageCollect(ctx, engine.GarbageCollectRequest{DryRun: true})
		if err != nil {
			t.Fatalf("CK-V11316-001: plan after first snapshot delete: %v", err)
		}
		if stillRetained.SnapshotOnlyRetainedLogicalFiles != 1 || stillRetained.SnapshotRetainedLogicalFiles != 1 {
			t.Fatalf("CK-V11316-001: remaining snapshot did not retain content: %+v", stillRetained)
		}

		deleted, err = eng.SnapshotDelete(ctx, engine.SnapshotDeleteRequest{SnapshotID: snapshotIDs[1], Mode: engine.SnapshotDeleteModeExecute})
		if err != nil || !deleted.Deleted {
			t.Fatalf("CK-V11316-001: delete last snapshot: result=%+v err=%v", deleted, err)
		}
		reclaimPlan, err := eng.GarbageCollect(ctx, engine.GarbageCollectRequest{DryRun: true})
		if err != nil {
			t.Fatalf("CK-V11316-008: plan after last snapshot delete: %v", err)
		}
		if reclaimPlan.AffectedContainers == 0 || reclaimPlan.BytesReclaimed == 0 {
			t.Fatalf("CK-V11316-008: rootless content not planned reclaimable: %+v", reclaimPlan)
		}
		if phase6Count(t, backend.DB, `SELECT COUNT(*) FROM logical_file WHERE id = $1`, stored.FileID) != 1 {
			t.Fatal("CK-V11316-008: dry-run mutated rootless logical recipe")
		}

		if backend.Capabilities.LiveGC {
			live, err := eng.GarbageCollect(ctx, engine.GarbageCollectRequest{})
			if err != nil {
				t.Fatalf("CK-V11316-008: live GC after last snapshot delete: %v", err)
			}
			if live.AffectedContainers == 0 || live.BytesReclaimed == 0 {
				t.Fatalf("CK-V11316-008: live GC did not reclaim rootless content: %+v", live)
			}
			if phase6Count(t, backend.DB, `SELECT COUNT(*) FROM logical_file WHERE id = $1`, stored.FileID) != 0 {
				t.Fatal("CK-V11316-008: live GC left rootless logical recipe")
			}
			if phase6Count(t, backend.DB, `SELECT COUNT(*) FROM physical_file WHERE logical_file_id = $1`, currentLive.FileID) != 1 {
				t.Fatal("CK-V11316-008: final live GC removed current-live control")
			}
		}
	})
}

func phase6Count(t *testing.T, dbconn *sql.DB, query string, args ...any) int64 {
	t.Helper()
	var count int64
	if err := dbconn.QueryRow(query, args...).Scan(&count); err != nil {
		t.Fatalf("CK-V11316-001: query catalog state: %v", err)
	}
	return count
}
