package recoveryexport

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/coordination"
	"github.com/franchoy/coldkeep/v2/internal/storage"
)

func TestRecoveryReadIndexContinuity(t *testing.T) {
	readers := map[string]string{"c1": filepath.Join(os.Getenv("COLDKEEP_PHASE3A_READERS"), "c1-reader"), "current": os.Getenv("COLDKEEP_RECOVERY_READER")}
	for _, path := range readers {
		if !filepath.IsAbs(path) {
			t.Fatal("required staged current and accepted C1 operators absent")
		}
		if _, e := os.Stat(path); e != nil {
			t.Fatal(e)
		}
	}
	for _, plain := range []bool{true, false} {
		t.Run(fmt.Sprintf("plaintext=%v", plain), func(t *testing.T) {
			f, files := recoveryHistory(t, plain)
			key := stageRecoveryKey(t, f)
			execSQL(t, f.db, `DELETE FROM snapshot_file WHERE logical_file_id=100`)
			files = files[:len(files)-1]
			b := bindHistory(t, f)
			sources := registerGenerationSources(t, f, b)
			upgrade := upgradeOptions(t, f, b)
			_, e := ApplyUpgrade(context.Background(), f.config, upgrade)
			must(t, e)
			policy, gid := generationID(t, "policy"), generationID(t, "generation")
			fixtureGenerationPolicy(t, f, policy, sources[0], "capture")
			insertGeneration(t, f, gid, sources[0], policy)
			insertGenerationEntry(t, f, gid, generationID(t, "entry"), nil, []byte{0xff, 'x'}, "file", "stored", 100)
			q := generationReadRequest(t, f, b)
			var lineage string
			must(t, f.db.QueryRow(generationLineageQuery()).Scan(&lineage))
			var final ExportOptions
			packages := t.TempDir()
			for _, indexed := range []bool{false, true} {
				t.Run(fmt.Sprintf("indexed=%v", indexed), func(t *testing.T) {
					if indexed {
						prepareReadFixture(t, f, q)
					}
					post := exportOptions(t)
					post.Destination = filepath.Join(packages, fmt.Sprintf("indexed-%v", indexed))
					_, e := Export(context.Background(), f.config, post)
					must(t, e)
					final = post
					for _, label := range []string{"c1", "current"} {
						t.Run(label, func(t *testing.T) {
							d, open := importTarget(t, f)
							opts := importOptionsFor(t, post)
							cfg := readerConfig(t, d)
							out, e := runReader(t, readers[label], "check", "-package", post.Destination)
							if e != nil {
								t.Fatalf("old/current package check: %v %s", e, out)
							}
							out, e = runReader(t, readers[label], "import", "-config", cfg, "-package", post.Destination, "-generation", opts.Generation, "-manifest-sha256", opts.ManifestSHA256, "-trusted-operator-backup", "-exclusive-target", "-database-capacity-bytes", "805306368")
							if e != nil {
								t.Fatalf("old/current import: %v %s", e, out)
							}
							var imported ImportResult
							must(t, json.Unmarshal(out, &imported))
							if !imported.Ready || imported.Receipt.Kind != "coldkeep-import-ready/v4" {
								t.Fatal(imported)
							}
							var after string
							must(t, open().QueryRow(generationLineageQuery()).Scan(&after))
							if after != lineage {
								t.Fatal("generation-only reference changed")
							}
							var hasIndex bool
							must(t, open().QueryRow(`SELECT to_regclass('public.coldkeep_generation_source_read_v1') IS NOT NULL`).Scan(&hasIndex))
							if hasIndex != indexed {
								t.Fatal("optional physical index changed during recovery")
							}
							output := filepath.Join(t.TempDir(), "history")
							out, e = runReader(t, readers[label], "restore", "-config", cfg, "-snapshot", "independent-history", "-output", output)
							if e != nil {
								t.Fatalf("old/current history restore: %v %s", e, out)
							}
							checkRestoredHistory(t, output, files)
							id, e := coordination.ResolveIdentity(d.PayloadRoot)
							must(t, e)
							lease, e := coordination.AcquireExisting(context.Background(), id)
							must(t, e)
							rc, e := OpenRecoverySession(context.Background(), Config(d), lease, *imported.Receipt)
							must(t, e)
							if !plain {
								t.Setenv("COLDKEEP_KEY", hex.EncodeToString(f.key))
							}
							output = filepath.Join(t.TempDir(), "generation-only")
							e = storage.RestoreFileWithStorageContext(storage.StorageContext{DB: rc.DB, ContainerDir: d.PayloadRoot}, 100, output)
							closeErr := rc.Close()
							releaseErr := lease.Release()
							must(t, e)
							must(t, closeErr)
							must(t, releaseErr)
							data, e := os.ReadFile(output)
							must(t, e)
							actual := sha256.Sum256(data)
							var expected string
							must(t, f.db.QueryRow(`SELECT file_hash FROM logical_file WHERE id=100`).Scan(&expected))
							if hex.EncodeToString(actual[:]) != expected {
								t.Fatal("generation-only payload changed")
							}
							if r, e := PrepareGenerationReadIndex(context.Background(), Config(d), q, exportOptions(t)); e == nil || r.Complete {
								t.Fatal("imported target maintained")
							}
						})
					}
				})
			}
			clear(f.key)
			t.Setenv("COLDKEEP_KEY", "")
			for _, label := range []string{"c1", "current"} {
				t.Run("isolated-"+label, func(t *testing.T) {
					t.Setenv("COLDKEEP_RECOVERY_READER", readers[label])
					isolatedRecovery(t, f, final, files, key, fmt.Sprintf("phase3c2-indexed-%v-%s", plain, label), b.Request.InstallationDirectory)
				})
			}
		})
	}
}
