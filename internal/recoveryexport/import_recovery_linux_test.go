package recoveryexport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/v2/internal/engine"
	"github.com/franchoy/coldkeep/v2/internal/storage"
)

type expectedRecoveryFile struct {
	ID   int64  `json:"id"`
	Path string `json:"path"`
	Hash string `json:"sha256"`
}

func recoveryHistory(t *testing.T, plain bool) (*fixture, []expectedRecoveryFile) {
	t.Helper()
	f := exportFixture(t)
	if plain {
		for _, q := range []string{"DELETE FROM physical_file WHERE logical_file_id>2", "DELETE FROM file_chunk WHERE logical_file_id>2", "DELETE FROM logical_file WHERE id>2", "DELETE FROM blocks WHERE chunk_id>1", "DELETE FROM chunk_block_refs WHERE chunk_id>1", "DELETE FROM storage_blocks WHERE id>1", "DELETE FROM chunk WHERE id>1"} {
			execSQL(t, f.db, q)
		}
		f.config.Key = nil
		f.config.KeyReference = ""
	}
	addLegacy(t, f, !plain)
	// Expected content is the preserved external old-writer oracle, fixed before
	// export, not recovered metadata or bytes supplied to the reader process.
	raw, e := os.ReadFile("../storage/testdata/compress_v1_18_supplemental/manifest.json")
	must(t, e)
	var oracle struct {
		Files []struct {
			ID   int64  `json:"file_id"`
			Hash string `json:"payload_sha256"`
		}
	}
	must(t, json.Unmarshal(raw, &oracle))
	files := []expectedRecoveryFile{}
	for _, entry := range oracle.Files {
		if plain && entry.ID > 2 {
			continue
		}
		name := fmt.Sprintf("history/file-%d.bin", entry.ID)
		if entry.ID == 1 {
			name = "history/Empty"
		}
		if entry.ID == 2 {
			name = "history/Case.txt"
		}
		if entry.ID == 3 {
			name = "history/case.txt"
		}
		if entry.ID == 4 {
			name = "history/é.txt"
		}
		files = append(files, expectedRecoveryFile{entry.ID, name, entry.Hash})
	}
	hash := sha256.Sum256([]byte("phase1b legacy historical recipe"))
	files = append(files, expectedRecoveryFile{100, "history/legacy.bin", hex.EncodeToString(hash[:])})
	execSQL(t, f.db, "INSERT INTO snapshot(id,created_at,type) VALUES('independent-history',NOW(),'full')")
	for _, file := range files {
		execSQL(t, f.db, "INSERT INTO snapshot_path(id,path) VALUES($1,$2)", file.ID, file.Path)
		execSQL(t, f.db, "INSERT INTO snapshot_file(snapshot_id,path_id,logical_file_id,size,mode,mtime) SELECT 'independent-history',$1,id,total_size,416,'2025-01-02T03:04:05Z' FROM logical_file WHERE id=$1", file.ID)
	}
	// Deliberately constructed old/current versions of the same stored path.
	// The historical recipe is tiny; its later current mapping is legacy content.
	var old, later string
	must(t, f.db.QueryRow("SELECT file_hash FROM logical_file WHERE id=2").Scan(&old))
	must(t, f.db.QueryRow("SELECT file_hash FROM logical_file WHERE id=100").Scan(&later))
	if old == later {
		t.Fatal("older/current oracle not distinct")
	}
	execSQL(t, f.db, "UPDATE physical_file SET path='/original/history/Case.txt', logical_file_id=100 WHERE logical_file_id=2")
	execSQL(t, f.db, "UPDATE logical_file SET ref_count=0 WHERE id=2; UPDATE logical_file SET ref_count=1 WHERE id=100; UPDATE chunk SET live_ref_count=1 WHERE id=100; UPDATE chunk SET live_ref_count=0 WHERE id=1")
	t.Logf("same path history/Case.txt: snapshot id=2 sha256=%s later current id=100 sha256=%s; remove current normally before export", old, later)
	sc := storage.StorageContext{DB: f.db, ContainerDir: f.root}
	eng, e := engine.New(engine.Config{DB: f.db, ContainerDir: f.root, StoreContext: &sc})
	must(t, e)
	rows, e := f.db.Query("SELECT path FROM physical_file ORDER BY path")
	must(t, e)
	var paths []string
	for rows.Next() {
		var p string
		must(t, rows.Scan(&p))
		paths = append(paths, p)
	}
	must(t, rows.Err())
	must(t, rows.Close())
	removed, e := eng.RemoveStoredPaths(context.Background(), engine.RemoveStoredPathsRequest{StoredPaths: paths})
	must(t, e)
	for _, item := range removed.Items {
		if item.Status != engine.BatchItemOK {
			t.Fatalf("remove: %+v", item)
		}
	}
	var n int
	must(t, f.db.QueryRow("SELECT count(*) FROM physical_file").Scan(&n))
	if n != 0 {
		t.Fatal("not zero-current history")
	}
	return f, files
}

func proofDocker(t *testing.T, input string, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdin = strings.NewReader(input)
	// Docker setup receives only the fixture credential selected for this run;
	// child recovery containers receive files, never the parent's environment.
	cmd.Env = append(os.Environ(), "POSTGRES_USER=ck_recovery_admin", "POSTGRES_PASSWORD="+os.Getenv("DB_PASSWORD"), "POSTGRES_DB=postgres")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	b, e := cmd.Output()
	if e != nil {
		t.Fatalf("docker %s failed: %v\n%s\n%s", args[0], e, b, stderr.String())
	}
	if stderr.Len() > 0 {
		t.Logf("recovery child stderr: %s", stderr.String())
	}
	return b
}

func isolatedRecovery(t *testing.T, f *fixture, o ExportOptions, files []expectedRecoveryFile, stagedKey, label string) {
	t.Helper()
	image := os.Getenv("COLDKEEP_RECOVERY_IMAGE")
	reader := os.Getenv("COLDKEEP_RECOVERY_READER")
	if image == "" || reader == "" {
		t.Fatal("explicit staged reader and PostgreSQL recovery image required")
	}
	name := fmt.Sprintf("ck-phase1d-recovery-%d-%d", os.Getpid(), fixtureSeq.Add(1))
	containerID := strings.TrimSpace(string(proofDocker(t, "", "run", "-d", "--name", name, "--label", "coldkeep.disposable=phase1d-recovery", "--memory", "1g", "--cpus", "2", "--tmpfs", "/var/lib/postgresql/data:rw,size=768m", "-e", "POSTGRES_USER", "-e", "POSTGRES_PASSWORD", "-e", "POSTGRES_DB", image)))
	t.Cleanup(func() { proofDocker(t, "", "rm", "-f", containerID) })
	for attempt := 0; attempt < 100; attempt++ {
		cmd := exec.Command("docker", "exec", containerID, "pg_isready", "-h", "127.0.0.1", "-U", "ck_recovery_admin", "-d", "postgres")
		if cmd.Run() == nil {
			break
		}
		if attempt == 99 {
			t.Fatal("recovery PostgreSQL startup timeout")
		}
		time.Sleep(100 * time.Millisecond)
	}
	custody := t.TempDir()
	recovered := t.TempDir()
	tools := t.TempDir()
	readerBytes, e := os.ReadFile(reader)
	must(t, e)
	must(t, os.WriteFile(filepath.Join(tools, "reader"), readerBytes, 0700))
	readerHash := sha256.Sum256(readerBytes)
	password := os.Getenv("DB_PASSWORD")
	must(t, os.WriteFile(filepath.Join(custody, "password"), []byte(password), 0600))
	keyFile := ""
	if f.config.Key != nil {
		keyFile = "/custody/key"
		keyBytes, e := os.ReadFile(stagedKey)
		must(t, e)
		must(t, os.WriteFile(filepath.Join(custody, "key"), keyBytes, 0600))
		clear(keyBytes)
		for _, v := range f.key {
			if v != 0 {
				t.Fatal("fixture parent key memory was not cleared before recovery")
			}
		}
	}
	// Bootstrap is separate from importer and reader. No admin credentials are
	// supplied to those processes; the role has no memberships/elevated flags.
	setup := "CREATE ROLE recovery_owner LOGIN PASSWORD '" + strings.ReplaceAll(password, "'", "''") + "';\nCREATE DATABASE recovery_target OWNER recovery_owner TEMPLATE template0;\n"
	proofDocker(t, setup, "exec", "-i", containerID, "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "ck_recovery_admin", "-d", "postgres")
	config := map[string]any{"host": "127.0.0.1", "port": 5432, "database": "recovery_target", "user": "recovery_owner", "payload_root": "/recovered/payload", "password_file": "/custody/password", "key_file": keyFile, "key_reference": f.config.KeyReference, "pg_dump": "/usr/lib/postgresql/16/bin/pg_dump", "pg_restore": "/usr/lib/postgresql/16/bin/pg_restore"}
	b, e := json.Marshal(config)
	must(t, e)
	must(t, os.WriteFile(filepath.Join(custody, "destination.json"), b, 0600))
	base := []string{"run", "--rm", "--network", "container:" + containerID, "--read-only", "--memory", "512m", "--cpus", "2", "--tmpfs", "/tmp:rw,size=64m", "--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()), "--mount", "type=bind,src=" + o.Destination + ",dst=/package,readonly", "--mount", "type=bind,src=" + custody + ",dst=/custody,readonly", "--mount", "type=bind,src=" + tools + ",dst=/tools,readonly", "--mount", "type=bind,src=" + recovered + ",dst=/recovered"}
	run := func(entry string, args ...string) []byte {
		cmd := append([]string{}, base...)
		cmd = append(cmd, "--entrypoint", "/usr/bin/env", image, "-i", "PATH=/usr/lib/postgresql/16/bin:/usr/bin:/bin", "LC_ALL=C", entry)
		cmd = append(cmd, args...)
		return proofDocker(t, "", cmd...)
	}
	// Negative access checks execute in precisely the recovery namespace. Only
	// destination PostgreSQL loopback exists; original source port/paths are absent.
	wd, e := os.Getwd()
	must(t, e)
	isolation := run("/bin/sh", "-c", `test ! -e "$1" && test ! -e "$2" && test ! -e "$3" || exit 1
IFS= read -r PGPASSWORD < /custody/password || :
export PGPASSWORD PGCONNECT_TIMEOUT=2
if /usr/lib/postgresql/16/bin/psql -X --no-password -h 127.0.0.1 -p "$4" -U recovery_owner -d "$5" -c 'SELECT 1'; then exit 1; fi
printf 'original catalog and original filesystem paths inaccessible in recovery namespace\n'`, "check", f.root, wd, filepath.Join(wd, "../storage/testdata/compress_v1_18_supplemental/manifest.json"), strconv.Itoa(int(f.config.Port)), f.config.Database)
	opts := importOptionsFor(t, o)
	before := treeFingerprint(t, o.Destination)
	var capacityFailure string
	if label == "original-loss-plain-true" {
		proofDocker(t, "CREATE DATABASE capacity_target OWNER recovery_owner TEMPLATE template0;\n", "exec", "-i", containerID, "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "ck_recovery_admin", "-d", "postgres")
		limited := map[string]any{}
		for k, v := range config {
			limited[k] = v
		}
		limited["database"] = "capacity_target"
		limited["payload_root"] = "/recovered/capacity-payload"
		limitedBytes, x := json.Marshal(limited)
		must(t, x)
		must(t, os.WriteFile(filepath.Join(custody, "capacity.json"), limitedBytes, 0600))
		// Deliberately exhaust only this operation-owned finite PostgreSQL tmpfs.
		proofDocker(t, "", "exec", containerID, "sh", "-c", `if dd if=/dev/zero of=/var/lib/postgresql/data/phase1d-capacity-fill bs=1048576 count=800; then exit 1; fi`)
		failure := run("/bin/sh", "-c", `if /tools/reader import -config /custody/capacity.json -package /package -generation "$1" -manifest-sha256 "$2" -trusted-operator-backup -exclusive-target -database-capacity-bytes 805306368; then exit 1; fi
test ! -e /recovered/capacity-payload/.coldkeep-control/IMPORT_READY`, "capacity", opts.Generation, opts.ManifestSHA256)
		capacityFailure = string(failure)
		proofDocker(t, "", "exec", containerID, "rm", "/var/lib/postgresql/data/phase1d-capacity-fill")
		if !strings.Contains(capacityFailure, `"ready":false`) || strings.Contains(capacityFailure, `"published":true`) {
			t.Fatal("capacity failure published", capacityFailure)
		}
		t.Log("D5 actual PostgreSQL tmpfs exhaustion leaves incomplete target; fresh success uses another database/root")
	}
	importBytes := run("/tools/reader", "import", "-config", "/custody/destination.json", "-package", "/package", "-generation", opts.Generation, "-manifest-sha256", opts.ManifestSHA256, "-trusted-operator-backup", "-exclusive-target", "-database-capacity-bytes", "805306368")
	var imported ImportResult
	must(t, json.Unmarshal(bytes.TrimSpace(importBytes), &imported))
	if !imported.Ready {
		t.Fatalf("import not ready: %s", importBytes)
	}
	restoreBytes := run("/usr/bin/timeout", "--kill-after=5s", "120s", "/tools/reader", "restore", "-config", "/custody/destination.json", "-snapshot", "independent-history", "-output", "/recovered/files")
	var restored struct {
		Files []struct {
			Path  string `json:"path"`
			Hash  string `json:"sha256"`
			Mode  uint32 `json:"mode"`
			MTime string `json:"mtime"`
		}
		PinsBefore int64 `json:"pins_before"`
		PinsAfter  int64 `json:"pins_after"`
	}
	must(t, json.Unmarshal(bytes.TrimSpace(restoreBytes), &restored))
	if len(restored.Files) != len(files) || restored.PinsBefore != restored.PinsAfter {
		t.Fatal("restore count/pin mismatch")
	}
	observed := map[string]string{}
	for _, file := range restored.Files {
		observed[file.Path] = file.Hash
		if file.Mode != 0640 || file.MTime != "2025-01-02T03:04:05Z" {
			t.Fatalf("metadata: %+v", file)
		}
	}
	for _, file := range files {
		data, e := os.ReadFile(filepath.Join(recovered, "files", file.Path))
		must(t, e)
		h := sha256.Sum256(data)
		if hex.EncodeToString(h[:]) != file.Hash || observed[file.Path] != file.Hash {
			t.Fatalf("restoration hash mismatch: %+v", file)
		}
	}
	if treeFingerprint(t, o.Destination) != before {
		t.Fatal("package changed during isolated recovery")
	}
	sessions := strings.TrimSpace(string(proofDocker(t, "", "exec", containerID, "psql", "-X", "-U", "ck_recovery_admin", "-d", "postgres", "-Atc", "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'coldkeep-%'")))
	if sessions != "0" {
		t.Fatalf("recovery session leak %s", sessions)
	}
	report := map[string]any{"scenario": label, "finite_postgres_capacity_failure": capacityFailure, "expected": files, "import": json.RawMessage(importBytes), "restore": json.RawMessage(restoreBytes), "negative_source_access": string(isolation), "reader_sha256": hex.EncodeToString(readerHash[:]), "image": image, "container_id": containerID, "key_custody": "private file staged before export; parent key memory cleared before recovery; no source key file mounted", "mounts": "package/read-only, independent custody/read-only, staged reader/read-only, fresh destination; no source installation or payload", "source_port": f.config.Port, "source_root": f.root, "postgres_capacity_bytes": 768 << 20, "reader_memory_bytes": 512 << 20, "remaining_owned_sessions": 0, "physical_independence": "unknown; same-host functional isolation"}
	if dir := os.Getenv("COLDKEEP_PHASE1D_REPORT_DIR"); dir != "" {
		b, e := json.MarshalIndent(report, "", "  ")
		must(t, e)
		must(t, os.WriteFile(filepath.Join(dir, label+".json"), b, 0600))
	}
	t.Logf("D7/D8/D10 %s: %d final files reread; snapshot-only zero-current; separate container/ordinary owner; reader=%x; no original installation access; pins balanced", label, len(files), readerHash)
}

func TestRecoveryOriginalLoss(t *testing.T) {
	for _, plain := range []bool{true, false} {
		t.Run(fmt.Sprintf("plaintext=%v", plain), func(t *testing.T) {
			f, files := recoveryHistory(t, plain)
			stagedKey := stageRecoveryKey(t, f)
			o := exportOptions(t)
			_, e := exportPreserved(t, f, o, nil)
			must(t, e)
			clear(f.key)
			isolatedRecovery(t, f, o, files, stagedKey, fmt.Sprintf("original-loss-plain-%v", plain))
		})
	}
}

func TestRecoveryPostLiveGC(t *testing.T) {
	f, files := recoveryHistory(t, false)
	stagedKey := stageRecoveryKey(t, f)
	o := exportOptions(t)
	_, e := exportPreserved(t, f, o, nil)
	must(t, e)
	before := treeFingerprint(t, o.Destination)
	check, e := Check(context.Background(), o.Destination, o.Limits)
	must(t, e)
	sc := storage.StorageContext{DB: f.db, ContainerDir: f.root}
	eng, e := engine.New(engine.Config{DB: f.db, ContainerDir: f.root, StoreContext: &sc})
	must(t, e)
	deleted, e := eng.SnapshotDelete(context.Background(), engine.SnapshotDeleteRequest{SnapshotID: "independent-history", Mode: engine.SnapshotDeleteModeExecute})
	must(t, e)
	if !deleted.Deleted {
		t.Fatal("snapshot not deleted")
	}
	live, e := eng.GarbageCollect(context.Background(), engine.GarbageCollectRequest{DryRun: false})
	must(t, e)
	if live.BytesReclaimed <= 0 || live.AffectedContainers == 0 {
		t.Fatalf("no actual source reclamation: %+v", live)
	}
	var remaining int
	must(t, f.db.QueryRow("SELECT count(*) FROM logical_file").Scan(&remaining))
	if remaining != 0 {
		t.Fatal("old recipes survive")
	}
	var reclaimed []string
	for _, member := range check.Manifest.Members[1:] {
		if _, e := os.Stat(filepath.Join(f.root, member.SourceName)); os.IsNotExist(e) {
			reclaimed = append(reclaimed, member.SourceName)
		}
	}
	if len(reclaimed) == 0 {
		t.Fatal("no exported object physically reclaimed")
	}
	if treeFingerprint(t, o.Destination) != before {
		t.Fatal("pre-GC package changed")
	}
	if dir := os.Getenv("COLDKEEP_PHASE1D_REPORT_DIR"); dir != "" {
		b, e := json.MarshalIndent(map[string]any{"gc": live, "reclaimed_exported_objects": reclaimed, "generation": check.Manifest.Generation, "package_unchanged": true, "remaining_recipes": remaining}, "", "  ")
		must(t, e)
		must(t, os.WriteFile(filepath.Join(dir, "live-gc.json"), b, 0600))
	}
	t.Logf("D10 real live GC reclaimed %d bytes, %d containers, removed all recipes; same pre-GC generation %s", live.BytesReclaimed, live.AffectedContainers, check.Manifest.Generation)
	clear(f.key)
	isolatedRecovery(t, f, o, files, stagedKey, "post-live-gc")
}

func stageRecoveryKey(t *testing.T, f *fixture) string {
	t.Helper()
	if f.config.Key == nil {
		return ""
	}
	path := filepath.Join(t.TempDir(), "independently-held-key.hex")
	must(t, os.WriteFile(path, []byte(hex.EncodeToString(f.key)), 0600))
	return path
}
