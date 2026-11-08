// Package proofdaemon is the bounded action 2.02 fixture adapter. It is not a
// product daemon, repository setup service, or durable job system.
package proofdaemon

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/franchoy/coldkeep/v2/internal/coordination"
	"github.com/franchoy/coldkeep/v2/internal/db"
	"github.com/franchoy/coldkeep/v2/internal/engine"
	"github.com/franchoy/coldkeep/v2/internal/snapshot"
	"github.com/franchoy/coldkeep/v2/internal/storage"
)

const MaxEntries = 256
const MaxFileBytes = 16 << 20

var ErrUnsupportedSQLite = errors.New("sqlite_cgo_required")
var ErrFixture = errors.New("fixture_configuration_refused")
var ErrBounds = errors.New("fixture_limit_exceeded")
var ErrEncoding = errors.New("fixture_encoding_refused")

// Config is supplied by the operator in a private file, never by the browser.
// Catalog is a SQLite absolute filename or an explicit PostgreSQL URL.
type Config struct {
	FixtureOnly bool   `json:"fixture_only"`
	Backend     string `json:"backend"`
	Catalog     string `json:"catalog"`
	PayloadRoot string `json:"payload_root"`
	Snapshot    string `json:"snapshot"`
	KeyFile     string `json:"key_file"`
	RestoreRoot string `json:"restore_root"`
	RuntimeDir  string `json:"runtime_dir"`
	Listen      string `json:"listen"`
	PublicURL   string `json:"public_url"`
	Container   bool   `json:"container"`
}

func ReadConfig(path string) (Config, error) {
	var c Config
	b, err := privateRead(path, 16384)
	if err != nil {
		return c, ErrFixture
	}
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.DisallowUnknownFields()
	if d.Decode(&c) != nil || d.Decode(new(any)) != io.EOF {
		return c, ErrFixture
	}
	return c, c.validate()
}
func privateRead(path string, limit int64) ([]byte, error) {
	st, e := os.Lstat(path)
	if e != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || st.Size() > limit {
		return nil, ErrFixture
	}
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer func() { _ = f.Close() }()
	got, e := f.Stat()
	if e != nil || !os.SameFile(st, got) {
		return nil, ErrFixture
	}
	b, e := io.ReadAll(io.LimitReader(f, limit+1))
	if int64(len(b)) > limit {
		return nil, ErrBounds
	}
	return b, e
}
func (c Config) validate() error {
	if !c.FixtureOnly || (c.Backend != "sqlite" && c.Backend != "postgres") || c.Catalog == "" || c.Snapshot == "" || len(c.Snapshot) > 128 || strings.TrimSpace(c.Snapshot) != c.Snapshot {
		return ErrFixture
	}
	if c.Backend == "sqlite" && !sqliteEnabled {
		return ErrUnsupportedSQLite
	}
	// The inherited engine trims this exact prefix before restoring. Refuse an
	// interpretation change before ownership/catalog work; never redirect output.
	if strings.TrimSpace(c.RestoreRoot) != c.RestoreRoot {
		return ErrFixture
	}
	roots := []string{c.PayloadRoot, c.RestoreRoot, c.RuntimeDir}
	for _, p := range roots {
		if !filepath.IsAbs(p) || filepath.Clean(p) != p {
			return ErrFixture
		}
		st, e := os.Lstat(p)
		if e != nil || !st.IsDir() {
			return ErrFixture
		}
		resolved, e := filepath.EvalSymlinks(p)
		if e != nil || resolved != p {
			return ErrFixture
		}
	}
	for i, a := range roots {
		for j, b := range roots {
			if i != j && (a == b || strings.HasPrefix(a, b+string(os.PathSeparator))) {
				return ErrFixture
			}
		}
	}
	st, e := os.Stat(c.RuntimeDir)
	if e != nil || st.Mode().Perm() != 0700 {
		return ErrFixture
	}
	f, e := os.Open(c.RestoreRoot)
	if e != nil {
		return ErrFixture
	}
	_, e = f.Readdirnames(1)
	_ = f.Close()
	if e != io.EOF {
		return ErrFixture
	}
	u, e := url.Parse(c.PublicURL)
	if e != nil || u.Scheme != "http" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.Port() == "" {
		return ErrFixture
	}
	ip := net.ParseIP(u.Hostname())
	if ip == nil || !ip.IsLoopback() {
		return ErrFixture
	}
	h, p, e := net.SplitHostPort(c.Listen)
	if e != nil || p == "0" || p == "" {
		return ErrFixture
	}
	bind := net.ParseIP(h)
	if bind == nil || (!c.Container && (!bind.IsLoopback() || c.Listen != u.Host)) {
		return ErrFixture
	}
	if c.Backend == "sqlite" {
		if !filepath.IsAbs(c.Catalog) || strings.HasPrefix(c.Catalog, c.PayloadRoot+string(os.PathSeparator)) {
			return ErrFixture
		}
		st, e := os.Lstat(c.Catalog)
		if e != nil || !st.Mode().IsRegular() {
			return ErrFixture
		}
	}
	if c.Backend == "postgres" {
		u, e := url.Parse(c.Catalog)
		if e != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Hostname() == "" || u.User == nil || !strings.HasPrefix(u.Path, "/ck_phase2_") {
			return ErrFixture
		}
	}
	return nil
}

type entry struct{ ID, Name, Size string }
type Runtime struct {
	cfg        Config
	lease      coordination.ExistingLease
	catalog    *sql.DB
	connection *joinedConnector
	storage    *storage.StorageContext
	engine     engine.Engine
	entries    []entry
	gate       chan struct{}
	closeOnce  sync.Once
	closeErr   error
}

// Open obtains the legacy canonical-root lock BEFORE opening any catalog,
// schema inspector or engine. Preparation is a separate fixture-build action.
func Open(c Config) (r *Runtime, err error) {
	if err = c.validate(); err != nil {
		return nil, err
	}
	id, e := coordination.ResolveIdentity(c.PayloadRoot)
	if e != nil {
		return nil, ErrFixture
	}
	lease, e := coordination.AcquireExisting(context.Background(), id)
	if e != nil {
		return nil, e
	}
	r = &Runtime{cfg: c, lease: lease, gate: make(chan struct{}, 1)}
	defer func() {
		if err != nil {
			err = errors.Join(err, r.Close())
			r = nil
		}
	}()
	if err = checkPayloadDirectory(id.CanonicalPath); err != nil {
		return r, err
	}
	key := ""
	if c.KeyFile != "" {
		raw, e := privateRead(c.KeyFile, 65)
		if e != nil {
			return r, ErrFixture
		}
		key = strings.TrimSpace(string(raw))
		b, e := hex.DecodeString(key)
		if e != nil || len(b) != 32 {
			return r, ErrFixture
		}
	}
	if err = os.Setenv("COLDKEEP_KEY", key); err != nil {
		return r, ErrFixture
	}
	r.catalog, r.connection, err = openJoined(c)
	if err != nil {
		return r, errors.New("catalog_open_failed")
	}
	v, e := db.CurrentSchemaVersionContext(context.Background(), r.catalog)
	if e != nil || v != 17 {
		return r, errors.New("catalog_schema_refused")
	}
	if err = checkEnvelope(r.catalog); err != nil {
		return r, err
	}
	var encrypted int
	if e := r.catalog.QueryRow("SELECT (SELECT COUNT(*) FROM blocks WHERE codec='aes-gcm') + (SELECT COUNT(*) FROM storage_blocks WHERE codec='aes-gcm')").Scan(&encrypted); e != nil {
		return r, errors.New("catalog_key_profile_refused")
	}
	if encrypted > 0 && key == "" {
		return r, errors.New("fixture_key_required")
	}
	r.storage = &storage.StorageContext{DB: r.catalog, ContainerDir: id.CanonicalPath}
	r.engine, err = engine.New(engine.Config{DB: r.catalog, ContainerDir: id.CanonicalPath, StoreContext: r.storage})
	if err != nil {
		return r, errors.New("engine_open_failed")
	}
	if _, e := r.engine.Verify(context.Background(), engine.VerifyRequest{Level: "deep", Target: "system"}); e != nil {
		return r, errors.New("fixture_catalog_payload_key_mismatch")
	}
	history, e := r.engine.SnapshotShow(context.Background(), engine.SnapshotShowRequest{SnapshotID: c.Snapshot, Query: engine.SnapshotQuery{Limit: MaxEntries + 1}})
	if e != nil {
		return r, errors.New("history_unavailable")
	}
	if len(history.Files) > MaxEntries {
		return r, ErrBounds
	}
	for _, f := range history.Files {
		if f.Size == nil || *f.Size < 0 || *f.Size > MaxFileBytes {
			return r, ErrBounds
		}
		r.entries = append(r.entries, entry{ID: randomToken(), Name: f.StoredPath, Size: fmt.Sprint(*f.Size)})
	}
	return r, nil
}

// Scalar counts/lengths precede the existing engine's allocation-producing
// queries. Fixed SQL identifiers only. No HTTP parameter reaches this SQL.
func checkEnvelope(conn *sql.DB) error {
	// SQLite octet_length(column) uses stored byte metadata without materializing
	// the complete text. Admit UTF-8 only: the pinned driver returns UTF-8 via
	// sqlite3_column_text/bytes, so UTF-16 storage sizes would understate some
	// Go strings. PostgreSQL must use UTF8 for both storage and client transfer.
	// These are read-only admission checks, never encoding conversion/repair.
	switch db.BackendFromDB(conn) {
	case db.BackendSQLite:
		var encoding string
		if err := conn.QueryRow("PRAGMA main.encoding").Scan(&encoding); err != nil || encoding != "UTF-8" {
			return ErrEncoding
		}
	case db.BackendPostgres:
		for _, query := range []string{"SHOW server_encoding", "SHOW client_encoding"} {
			var encoding string
			if err := conn.QueryRow(query).Scan(&encoding); err != nil || encoding != "UTF8" {
				return ErrEncoding
			}
		}
	default:
		return ErrEncoding
	}
	for table, bound := range map[string]int{"snapshot": 32, "snapshot_file": MaxEntries, "snapshot_path": MaxEntries, "logical_file": MaxEntries, "physical_file": MaxEntries, "file_chunk": 4096, "chunk": 4096, "blocks": 4096, "storage_blocks": 4096, "chunk_block_refs": 4096, "container": 128, "repository_config": 64, "store_repair_attempt": 256, "store_repair_container": 128, "store_repair_block": 4096, "store_repair_chunk": 4096, "retired_chunk_block_ref": 4096, "retired_legacy_block_extent": 4096} {
		var n int
		if e := conn.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); e != nil {
			return errors.New("catalog_shape_refused")
		}
		if n > bound {
			return ErrBounds
		}
	}
	for _, q := range []string{
		"SELECT COUNT(*) FROM logical_file WHERE total_size<0 OR total_size>16777216 OR OCTET_LENGTH(original_name)>1024",
		"SELECT COUNT(*) FROM snapshot_path WHERE OCTET_LENGTH(path)>1024",
		"SELECT COUNT(*) FROM chunk WHERE size>16777216 OR OCTET_LENGTH(chunk_hash)>128",
		"SELECT COUNT(*) FROM blocks WHERE stored_size>16777216 OR plaintext_size>16777216",
		"SELECT COUNT(*) FROM storage_blocks WHERE stored_size>16777216 OR plaintext_size>16777216",
		"SELECT COUNT(*) FROM snapshot WHERE OCTET_LENGTH(id)>128 OR OCTET_LENGTH(label)>1024",
		"SELECT COUNT(*) FROM container WHERE current_size>67108864 OR OCTET_LENGTH(filename)>255",
	} {
		var n int
		if e := conn.QueryRow(q).Scan(&n); e != nil {
			return errors.New("catalog_shape_refused")
		}
		if n != 0 {
			return ErrBounds
		}
	}
	rows, e := conn.Query("SELECT path FROM snapshot_path LIMIT 257")
	if e != nil {
		return e
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var p string
		if e = rows.Scan(&p); e != nil {
			return e
		}
		norm, e := snapshot.NormalizeSnapshotPath(p)
		if e != nil || norm != p || !utf8.ValidString(p) || len(p) > 1024 {
			return errors.New("exact_name_unsupported")
		}
	}
	return rows.Err()
}
func checkPayloadDirectory(path string) error {
	f, e := os.Open(path)
	if e != nil {
		return ErrFixture
	}
	defer func() { _ = f.Close() }()
	entries, e := f.ReadDir(130)
	if e != nil && e != io.EOF {
		return ErrFixture
	}
	if len(entries) >= 130 {
		return ErrBounds
	}
	for _, entry := range entries {
		if entry.Name() == coordination.ControlDirectoryName {
			continue
		}
		st, e := entry.Info()
		if e != nil || !st.Mode().IsRegular() || st.Size() > 64<<20 {
			return ErrBounds
		}
	}
	return nil
}

func randomToken() string {
	b := make([]byte, 32)
	if _, e := rand.Read(b); e != nil {
		panic("system entropy unavailable")
	}
	return hex.EncodeToString(b)
}
func (r *Runtime) Close() error {
	r.closeOnce.Do(func() {
		// Run calls this only after every handler has joined. Physical driver Close
		// completion is joined before the old lease can be released.
		if r.connection != nil {
			r.closeErr = r.connection.close(r.catalog)
		}
		if r.lease != nil {
			r.closeErr = errors.Join(r.closeErr, r.lease.Release())
		}
	})
	return r.closeErr
}
func (r *Runtime) restore(id string) RestoreResult {
	select {
	case r.gate <- struct{}{}:
		defer func() { <-r.gate }()
	default:
		return RestoreResult{Outcome: "refused", Code: "operation_busy", Detail: "One engine operation is already running."}
	}
	if r.lease.Check() != nil {
		return RestoreResult{Outcome: "refused", Code: "ownership_lost", Detail: "Repository ownership is unavailable."}
	}
	var selected *entry
	for i := range r.entries {
		if r.entries[i].ID == id {
			selected = &r.entries[i]
			break
		}
	}
	if selected == nil {
		return RestoreResult{Outcome: "refused", Code: "unknown_entry", Detail: "Select an entry from this session's history."}
	}
	// A fresh, operator-selected output root; exact historical path, no overwrite.
	result, e := r.engine.SnapshotRestore(context.Background(), engine.SnapshotRestoreRequest{SnapshotID: r.cfg.Snapshot, Paths: []string{selected.Name}, Destination: engine.SnapshotRestoreDestination{Mode: engine.SnapshotRestoreDestinationPrefix, Path: r.cfg.RestoreRoot}, Metadata: engine.SnapshotRestoreMetadataStrict})
	if e != nil {
		return RestoreResult{Outcome: "failed_or_partial", Code: "restore_failed", Detail: "Restore failed. Bytes may be visible or metadata incomplete; inspect the operator output before retrying."}
	}
	if result.RestoredFiles != 1 || len(result.Warnings) != 0 {
		return RestoreResult{Outcome: "failed_or_partial", Code: "restore_incomplete", Detail: "The engine did not confirm one complete file with selected metadata."}
	}
	return RestoreResult{Outcome: "completed", Code: "restored", Detail: "Content verified by the engine and installed with selected metadata. Independent reread is a separate proof."}
}
