// Package productfixtures exercises fixed historical bytes and named legacy
// builders. It has no TestMain, ambient database connection or runtime startup.
package productfixtures

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/v2/internal/blocks"
	"github.com/franchoy/coldkeep/v2/internal/container"
	"github.com/franchoy/coldkeep/v2/internal/db"
	"github.com/franchoy/coldkeep/v2/internal/snapshot"
	"github.com/franchoy/coldkeep/v2/internal/storage"
	"github.com/franchoy/coldkeep/v2/internal/verify"
	"github.com/franchoy/coldkeep/v2/internal/version"
	_ "github.com/mattn/go-sqlite3"
)

type outputOracle struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Size   int    `json:"size"`
}

type expectation struct {
	Outcome        string         `json:"outcome"`
	ErrorContains  string         `json:"error_contains"`
	NormalizedName string         `json:"normalized_name"`
	Outputs        []outputOracle `json:"outputs"`
	Metadata       string         `json:"metadata"`
}

type fixture struct {
	ID        string      `json:"id"`
	Setup     string      `json:"setup"`
	Operation string      `json:"operation"`
	FileID    int64       `json:"file_id"`
	KeyRef    string      `json:"key_ref"`
	Expected  expectation `json:"expected"`
}

type fixtureManifest struct {
	Schema         string `json:"schema"`
	RetainedSource string `json:"retained_source"`
	Assets         []struct {
		Path   string `json:"path"`
		SHA256 string `json:"sha256"`
		Bytes  int    `json:"bytes"`
	} `json:"assets"`
	Fixtures []fixture `json:"fixtures"`
}

type observation struct {
	FixtureID                string         `json:"fixture_id"`
	KeyRef                   string         `json:"key_ref,omitempty"`
	Expected                 expectation    `json:"expected"`
	Outcome                  string         `json:"observed_outcome"`
	Error                    string         `json:"observed_refusal,omitempty"`
	NormalizedName           string         `json:"observed_normalized_name,omitempty"`
	Outputs                  []outputOracle `json:"observed_outputs"`
	Metadata                 string         `json:"observed_metadata"`
	CatalogSemanticUnchanged bool           `json:"catalog_semantic_unchanged"`
	PayloadBytesUnchanged    bool           `json:"payload_bytes_unchanged"`
	WorkingDirectory         string         `json:"working_directory,omitempty"`
}

func TestProductFixtures(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the selected fixture name/metadata profile is Linux; other targets are unqualified")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	must(t, err)
	raw, err := os.ReadFile(filepath.Join(root, "testdata/product-fixtures/manifest.json"))
	must(t, err)
	var manifest fixtureManifest
	must(t, json.Unmarshal(raw, &manifest))
	if manifest.Schema != "coldkeep-product-fixtures/v1" || len(manifest.Fixtures) == 0 {
		t.Fatal("unsupported or empty fixture manifest")
	}
	checkAssets(t, root, manifest)
	t.Cleanup(func() { checkAssets(t, root, manifest) })
	keyBytes, err := os.ReadFile(filepath.Join(root, manifest.RetainedSource, "manifest.json"))
	must(t, err)
	var keySource struct {
		Key string `json:"aes_key_hex"`
	}
	must(t, json.Unmarshal(keyBytes, &keySource))
	if len(keySource.Key) != 64 {
		t.Fatal("disposable fixture key is unavailable")
	}
	results := make([]observation, 0, len(manifest.Fixtures))
	seen := make(map[string]bool)
	for _, f := range manifest.Fixtures {
		if f.ID == "" || seen[f.ID] {
			t.Fatalf("duplicate/empty fixture ID %q", f.ID)
		}
		seen[f.ID] = true
		t.Run(f.ID, func(t *testing.T) {
			r := observation{FixtureID: f.ID, KeyRef: f.KeyRef, Expected: f.Expected, Outputs: []outputOracle{}}
			defer func() {
				if t.Failed() {
					r.Outcome = "UNEXPECTED_FAILURE"
				}
				results = append(results, r)
				encoded, _ := json.Marshal(r)
				t.Logf("PRODUCT_FIXTURE_RESULT %s", encoded)
			}()
			t.Setenv("COLDKEEP_KEY", keySource.Key)
			runFixture(t, root, manifest.RetainedSource, f, &r)
		})
	}
	if path := os.Getenv("COLDKEEP_PRODUCT_FIXTURE_REPORT"); path != "" {
		report := struct {
			Schema           string        `json:"schema"`
			ReaderVersion    string        `json:"reader_version"`
			DeclaredRevision string        `json:"declared_source_revision"`
			GoVersion        string        `json:"go_version"`
			ManifestSHA256   string        `json:"manifest_sha256"`
			Results          []observation `json:"fixtures"`
		}{"coldkeep-product-fixture-results/v1", version.String(), os.Getenv("COLDKEEP_PRODUCT_READER_REVISION"), runtime.Version(), hash(raw), results}
		encoded, err := json.MarshalIndent(report, "", "  ")
		must(t, err)
		out, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		must(t, err)
		_, err = out.Write(append(encoded, '\n'))
		must(t, err)
		must(t, out.Close())
	}
}

func runFixture(t *testing.T, root, source string, f fixture, r *observation) {
	t.Helper()
	if strings.HasPrefix(f.Setup, "name-") {
		input := "trailing.txt "
		if f.Setup == "name-normalization" {
			input = `back\slash.txt`
		}
		normalized, err := snapshot.NormalizeSnapshotPath(input)
		r.NormalizedName = normalized
		r.CatalogSemanticUnchanged, r.PayloadBytesUnchanged = true, true
		r.Metadata = "no repository open or mutation"
		if f.Expected.Outcome == "KNOWN_LIMIT" {
			must(t, err)
			if normalized != f.Expected.NormalizedName || normalized == input {
				t.Fatalf("name normalization: %q", normalized)
			}
			r.Outcome = "KNOWN_LIMIT"
			return
		}
		checkRefusal(t, f, r, err)
		return
	}
	work := t.TempDir()
	if parent := os.Getenv("COLDKEEP_PRODUCT_FIXTURE_WORK"); parent != "" {
		must(t, os.MkdirAll(parent, 0o700))
		var err error
		work, err = os.MkdirTemp(parent, f.ID+"-")
		must(t, err) // Retain working copies, including unexpected failures, for inspection.
	}
	r.WorkingDirectory = work
	copyTree(t, filepath.Join(root, source), work)
	conn, err := sql.Open("sqlite3", filepath.Join(work, "catalog.db")+"?_foreign_keys=on&_busy_timeout=5000")
	must(t, err)
	defer func() { must(t, conn.Close()) }()
	conn.SetMaxOpenConns(1)
	schema, err := db.CurrentSchemaVersion(conn)
	must(t, err)
	if schema != 17 {
		t.Fatalf("fixture schema %d, expected 17", schema)
	}
	containers := filepath.Join(work, "containers")
	fileID := f.FileID
	if strings.HasPrefix(f.Setup, "legacy-") || f.Setup == "mixed" {
		fileID = seedLegacy(t, conn, containers, f.Setup)
	}
	if f.Setup == "snapshots" || f.Setup == "names" {
		seedSnapshots(t, conn, f)
	}
	mutateFixture(t, conn, containers, f)
	beforeCatalog := catalogState(t, conn)
	beforePayload := treeState(t, containers)
	outputDir := filepath.Join(work, "output")
	must(t, os.Mkdir(outputDir, 0o700))
	ctx := storage.StorageContext{DB: conn, ContainerDir: containers}
	switch f.Operation {
	case "schema-inspect":
		_, err = db.CurrentSchemaVersion(conn)
	case "verify-fast":
		err = verify.VerifyRepositoryFast(conn, containers)
	case "decode-packed":
		var name string
		var offset, size int64
		must(t, conn.QueryRow(`SELECT c.filename,b.container_offset,b.stored_size FROM container c
			JOIN storage_blocks b ON b.container_id=c.id JOIN chunk_block_refs r ON r.block_id=b.id
			JOIN file_chunk f ON f.chunk_id=r.chunk_id WHERE f.logical_file_id=? LIMIT 1`, f.FileID).Scan(&name, &offset, &size))
		data, readErr := os.ReadFile(filepath.Join(containers, name))
		must(t, readErr)
		encoded := append([]byte{}, data[offset:offset+size]...)
		binary.LittleEndian.PutUint16(encoded[4:6], 2) // Private buffer, unsupported CKBL version.
		_, err = blocks.DecodeBlock(encoded)
	case "snapshot-restore":
		ids := []string{"older", "newer"}
		if f.Setup == "names" {
			ids = []string{"names"}
		}
		for _, id := range ids {
			destination := filepath.Join(outputDir, id)
			must(t, os.Mkdir(destination, 0o700))
			_, err = snapshot.RestoreSnapshot(context.Background(), conn, id, nil, snapshot.RestoreSnapshotOptions{
				DestinationMode: storage.RestoreDestinationPrefix, Destination: destination,
				StrictMetadata: true, StorageContext: &ctx,
			})
			if err != nil {
				break
			}
		}
	case "restore":
		_, err = storage.RestoreFileWithStorageContextResultOptions(ctx, fileID,
			filepath.Join(outputDir, "restored.bin"), storage.RestoreOptions{NoMetadata: true})
	default:
		t.Fatalf("unsupported fixture operation %q", f.Operation)
	}
	r.CatalogSemanticUnchanged = beforeCatalog == catalogState(t, conn)
	r.PayloadBytesUnchanged = beforePayload == treeState(t, containers)
	if !r.CatalogSemanticUnchanged || !r.PayloadBytesUnchanged {
		t.Fatal("reader changed catalog meaning or payload bytes")
	}
	if f.Expected.Outcome == "REFUSED" {
		checkRefusal(t, f, r, err)
		entries, readErr := os.ReadDir(outputDir)
		must(t, readErr)
		if len(entries) != 0 {
			t.Fatal("refusal published output or left temporary objects")
		}
		r.Metadata = "no destination published; no source repair"
		return
	}
	must(t, err)
	for _, expected := range f.Expected.Outputs {
		path := filepath.Join(outputDir, expected.Path)
		data, readErr := os.ReadFile(path)
		must(t, readErr)
		observed := outputOracle{expected.Path, hash(data), len(data)}
		r.Outputs = append(r.Outputs, observed)
		if observed != expected {
			t.Fatalf("output mismatch: got %+v expected %+v", observed, expected)
		}
		if f.Operation == "snapshot-restore" {
			info, statErr := os.Stat(path)
			must(t, statErr)
			if info.Mode().Perm() != 0o640 || !info.ModTime().Equal(fixtureTime()) {
				t.Fatalf("metadata mismatch: mode=%o mtime=%s", info.Mode().Perm(), info.ModTime())
			}
		}
	}
	if f.Operation == "snapshot-restore" {
		r.Metadata = "mode=0640; mtime=2020-01-02T03:04:05Z; Linux only"
	} else {
		r.Metadata = f.Expected.Metadata
	}
	r.Outcome = "RESTORED"
	if f.Setup == "future-block" && f.Expected.Outcome == "KNOWN_LIMIT" {
		r.Outcome = "KNOWN_LIMIT"
	}
}

func mutateFixture(t *testing.T, conn *sql.DB, containers string, f fixture) {
	t.Helper()
	switch f.Setup {
	case "missing-key":
		t.Setenv("COLDKEEP_KEY", "")
	case "processing":
		execSQL(t, conn, "UPDATE logical_file SET status='PROCESSING' WHERE id=?", f.FileID)
	case "future-schema":
		execSQL(t, conn, "UPDATE schema_version SET catalog_version=18")
	case "future-block":
		execSQL(t, conn, `UPDATE storage_blocks SET format_version=2 WHERE id IN
			(SELECT r.block_id FROM chunk_block_refs r JOIN file_chunk f ON f.chunk_id=r.chunk_id WHERE f.logical_file_id=?)`, f.FileID)
	case "missing", "truncated", "corrupt":
		var name string
		var offset, size int64
		must(t, conn.QueryRow(`SELECT c.filename, b.container_offset, b.stored_size FROM container c
			JOIN storage_blocks b ON b.container_id=c.id JOIN chunk_block_refs r ON r.block_id=b.id
			JOIN file_chunk f ON f.chunk_id=r.chunk_id WHERE f.logical_file_id=? LIMIT 1`, f.FileID).Scan(&name, &offset, &size))
		path := filepath.Join(containers, name)
		switch f.Setup {
		case "missing":
			must(t, os.Rename(path, filepath.Join(filepath.Dir(containers), "withheld-payload.bin")))
		case "truncated":
			must(t, os.Truncate(path, offset+size-1))
		case "corrupt":
			data, err := os.ReadFile(path)
			must(t, err)
			data[offset+size-1] ^= 0x01
			must(t, os.WriteFile(path, data, 0o600))
		}
	case "retained-copy", "legacy-plain", "legacy-aes-gcm", "mixed", "snapshots", "names":
	default:
		t.Fatalf("unsupported fixture setup %q", f.Setup)
	}
}

// Same legacy-container encoding and row builder pattern as restore_step8_test.go
// and store_test.go:writeReusableTestContainerFileWithPayload; no product writer.
func seedLegacy(t *testing.T, conn *sql.DB, containers, kind string) int64 {
	t.Helper()
	payload := []byte("legacy-v17-part-a-legacy-v17-part-b")
	var chunkID int64
	must(t, conn.QueryRow(`INSERT INTO chunk(chunk_hash,size,status,live_ref_count,chunker_version)
		VALUES(?,?,'COMPLETED',1,'v1-simple-rolling') RETURNING id`, hash(payload), len(payload)).Scan(&chunkID))
	codec := blocks.CodecPlain
	if kind == "legacy-aes-gcm" {
		codec = blocks.CodecAESGCM
	}
	transformer, err := blocks.GetBlockTransformer(codec)
	must(t, err)
	encoded, err := transformer.Encode(context.Background(), blocks.EncodeInput{ChunkID: chunkID, ChunkHash: hash(payload), Plaintext: payload})
	must(t, err)
	header := make([]byte, container.ContainerHdrLen)
	copy(header[:8], container.ContainerMagic)
	binary.LittleEndian.PutUint16(header[8:10], container.LegacyContainerFormatVersionMajor)
	binary.LittleEndian.PutUint16(header[10:12], 9)
	binary.LittleEndian.PutUint32(header[12:16], uint32(container.ContainerHdrLen))
	binary.LittleEndian.PutUint64(header[28:36], 1<<20)
	binary.LittleEndian.PutUint32(header[52:56], crc32.ChecksumIEEE(header[:52]))
	must(t, os.WriteFile(filepath.Join(containers, "legacy.bin"), append(header, encoded.Payload...), 0o600))
	var containerID, fileID int64
	must(t, conn.QueryRow(`INSERT INTO container(filename,current_size,max_size,sealed) VALUES('legacy.bin',?,?,TRUE) RETURNING id`, container.ContainerHdrLen+len(encoded.Payload), 1<<20).Scan(&containerID))
	execSQL(t, conn, `INSERT INTO blocks(chunk_id,codec,format_version,plaintext_size,stored_size,nonce,container_id,block_offset)
		VALUES(?,?,1,?,?,?,?,?)`, chunkID, string(codec), len(payload), len(encoded.Payload), encoded.Descriptor.Nonce, containerID, container.ContainerHdrLen)
	all := append([]byte{}, payload...)
	if kind == "mixed" {
		all = append(all, []byte("tiny")...)
	}
	must(t, conn.QueryRow(`INSERT INTO logical_file(original_name,total_size,file_hash,status,chunker_version)
		VALUES('legacy.bin',?,?,'COMPLETED','v1-simple-rolling') RETURNING id`, len(all), hash(all)).Scan(&fileID))
	execSQL(t, conn, "INSERT INTO file_chunk(logical_file_id,chunk_id,chunk_order) VALUES(?,?,0)", fileID, chunkID)
	if kind == "mixed" {
		execSQL(t, conn, "INSERT INTO file_chunk(logical_file_id,chunk_id,chunk_order) SELECT ?,chunk_id,1 FROM file_chunk WHERE logical_file_id=2", fileID)
	}
	return fileID
}

func seedSnapshots(t *testing.T, conn *sql.DB, f fixture) {
	t.Helper()
	ids := []string{"older", "newer"}
	if f.Setup == "names" {
		ids = []string{"names"}
	}
	for _, id := range ids {
		must(t, snapshot.InsertSnapshot(context.Background(), conn, snapshot.Snapshot{ID: id, CreatedAt: fixtureTime(), Type: "full"}))
	}
	for _, output := range f.Expected.Outputs {
		parts := strings.SplitN(output.Path, "/", 2)
		fileID := int64(2)
		if parts[0] == "newer" {
			fileID = 3
		}
		_, err := snapshot.InsertSnapshotFile(context.Background(), conn, snapshot.SnapshotFile{
			SnapshotID: parts[0], Path: parts[1], LogicalFileID: fileID,
			Size: sql.NullInt64{Int64: int64(output.Size), Valid: true}, Mode: sql.NullInt64{Int64: 0o640, Valid: true},
			MTime: sql.NullTime{Time: fixtureTime(), Valid: true},
		})
		must(t, err)
	}
}

func fixtureTime() time.Time  { return time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC) }
func hash(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func execSQL(t *testing.T, conn *sql.DB, query string, args ...any) {
	t.Helper()
	_, err := conn.Exec(query, args...)
	must(t, err)
}
func checkRefusal(t *testing.T, f fixture, r *observation, err error) {
	t.Helper()
	if err == nil || f.Expected.ErrorContains == "" || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(f.Expected.ErrorContains)) {
		t.Fatalf("expected refusal containing %q, got %v", f.Expected.ErrorContains, err)
	}
	r.Outcome, r.Error = "REFUSED", err.Error()
}

func checkAssets(t *testing.T, root string, manifest fixtureManifest) {
	t.Helper()
	for _, asset := range manifest.Assets {
		data, err := os.ReadFile(filepath.Join(root, asset.Path))
		must(t, err)
		if len(data) != asset.Bytes || hash(data) != asset.SHA256 {
			t.Fatalf("retained oracle changed: %s", asset.Path)
		}
	}
}

func copyTree(t *testing.T, source, target string) {
	t.Helper()
	must(t, filepath.WalkDir(source, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		out := filepath.Join(target, rel)
		if entry.IsDir() {
			return os.MkdirAll(out, 0o700)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("nonregular fixture member: %s", rel)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(out, data, 0o600)
	}))
}

func treeState(t *testing.T, root string) string {
	t.Helper()
	var entries []string
	must(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		entries = append(entries, rel+"\x00"+hash(data))
		return nil
	}))
	sort.Strings(entries)
	return hash([]byte(strings.Join(entries, "\n")))
}

// Compare catalog meaning, not SQLite transaction-header bytes: restore may pin
// and unpin chunks even when its final semantic state is unchanged.
func catalogState(t *testing.T, conn *sql.DB) string {
	t.Helper()
	rows, err := conn.Query("SELECT name,sql FROM sqlite_master WHERE type='table' ORDER BY name")
	must(t, err)
	var tables, state []string
	for rows.Next() {
		var name, ddl string
		must(t, rows.Scan(&name, &ddl))
		tables = append(tables, name)
		state = append(state, ddl)
	}
	must(t, rows.Err())
	must(t, rows.Close())
	for _, table := range tables {
		rows, err = conn.Query(`SELECT * FROM "` + strings.ReplaceAll(table, `"`, `""`) + `"`)
		must(t, err)
		columns, err := rows.Columns()
		must(t, err)
		var values []string
		for rows.Next() {
			cells := make([]any, len(columns))
			ptrs := make([]any, len(columns))
			for i := range cells {
				ptrs[i] = &cells[i]
			}
			must(t, rows.Scan(ptrs...))
			data, err := json.Marshal(cells)
			must(t, err)
			values = append(values, string(data))
		}
		must(t, rows.Err())
		must(t, rows.Close())
		sort.Strings(values)
		state = append(state, table+":"+strings.Join(values, "\n"))
	}
	return hash([]byte(strings.Join(state, "\n")))
}
