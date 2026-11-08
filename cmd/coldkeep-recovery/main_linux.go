// coldkeep-recovery is the bounded Linux operator adapter for package-v1 import
// and the existing schema-17 snapshot reader. It never initializes a catalog.
package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/franchoy/coldkeep/v2/internal/coordination"
	"github.com/franchoy/coldkeep/v2/internal/recoveryexport"
	"github.com/franchoy/coldkeep/v2/internal/snapshot"
	"github.com/franchoy/coldkeep/v2/internal/storage"
	"github.com/lib/pq"
)

type config struct {
	Host         string `json:"host"`
	Port         uint16 `json:"port"`
	Database     string `json:"database"`
	User         string `json:"user"`
	PayloadRoot  string `json:"payload_root"`
	PasswordFile string `json:"password_file"`
	KeyFile      string `json:"key_file"`
	KeyReference string `json:"key_reference"`
	PGDump       string `json:"pg_dump"`
	PGRestore    string `json:"pg_restore"`
}

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) < 2 {
		return errors.New("usage: coldkeep-recovery check|import|restore [explicit options]; see recovery-package.md")
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ContinueOnError)
	pkg := fs.String("package", "", "completed generation path")
	cfgPath := fs.String("config", "", "private explicit destination JSON")
	gen := fs.String("generation", "", "trusted generation identity")
	digest := fs.String("manifest-sha256", "", "trusted exact manifest SHA-256")
	trusted := fs.Bool("trusted-operator-backup", false, "consent to execute this trusted operator backup")
	exclusive := fs.Bool("exclusive-target", false, "operator controls exclusive access to dedicated target")
	capacity := fs.Int64("database-capacity-bytes", 0, "dedicated PostgreSQL heap/index/temp/WAL budget")
	snap := fs.String("snapshot", "", "exact selected snapshot ID")
	out := fs.String("output", "", "fresh absolute restore output directory")
	if e := fs.Parse(os.Args[2:]); e != nil {
		return e
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected arguments")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if os.Args[1] == "check" {
		r, e := recoveryexport.Check(ctx, *pkg, recoveryexport.DefaultExportLimits())
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(r)
	}
	var c config
	if e := readJSON(*cfgPath, &c); e != nil {
		return e
	}
	password, e := privateBytes(c.PasswordFile, 4096)
	if e != nil {
		return e
	}
	defer clear(password)
	var key []byte
	if c.KeyFile != "" {
		raw, e := privateBytes(c.KeyFile, 64)
		if e != nil {
			return e
		}
		key, e = hex.DecodeString(string(raw))
		clear(raw)
		if e != nil || len(key) != 32 {
			return recoveryexport.ErrKeyUnavailable
		}
		defer clear(key)
	}
	d := recoveryexport.Destination{Backend: "postgres", Profile: "manual-postgresql-docker", Transport: "local-plaintext", Host: c.Host, Port: c.Port, Database: c.Database, User: c.User, PayloadRoot: c.PayloadRoot, Password: func(context.Context) (string, error) { return string(password), nil }, KeyReference: c.KeyReference}
	if key != nil {
		d.Key = func(context.Context) ([]byte, error) { return key, nil }
	}
	switch os.Args[1] {
	case "import":
		r, e := recoveryexport.Import(ctx, d, recoveryexport.ImportOptions{Package: *pkg, Generation: *gen, ManifestSHA256: *digest, TrustedOperatorBackup: *trusted, ExclusiveTarget: *exclusive, DatabaseCapacityBytes: *capacity, Tools: recoveryexport.Tools{PGDump: c.PGDump, PGRestore: c.PGRestore}, Limits: recoveryexport.DefaultExportLimits(), InventoryLimits: recoveryexport.DefaultLimits()})
		return errors.Join(e, json.NewEncoder(os.Stdout).Encode(r))
	case "restore":
		return restore(ctx, c, string(password), key, *snap, *out)
	default:
		return errors.New("unknown recovery operation")
	}
}
func privateBytes(path string, max int64) ([]byte, error) {
	info, e := os.Lstat(path)
	if e != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > max {
		return nil, errors.New("private regular credential file required")
	}
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	after, e := f.Stat()
	if e != nil || !os.SameFile(info, after) {
		return nil, errors.Join(errors.New("credential identity changed"), f.Close())
	}
	b, e := io.ReadAll(io.LimitReader(f, max+1))
	e = errors.Join(e, f.Close())
	if int64(len(b)) > max {
		return nil, errors.New("credential limit")
	}
	return b, e
}
func readJSON(path string, v any) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	d := json.NewDecoder(io.LimitReader(f, 1<<20))
	d.DisallowUnknownFields()
	e = d.Decode(v)
	if e == nil {
		var extra any
		if d.Decode(&extra) != io.EOF {
			e = errors.New("trailing JSON")
		}
	}
	return errors.Join(e, f.Close())
}

func restore(ctx context.Context, c config, password string, key []byte, selection, output string) (err error) {
	if selection == "" || !filepath.IsAbs(output) || filepath.Clean(output) != output {
		return errors.New("explicit snapshot and fresh absolute output required")
	}
	id, e := coordination.ResolveIdentity(c.PayloadRoot)
	if e != nil {
		return e
	}
	parent, e := filepath.EvalSymlinks(filepath.Dir(output))
	if e != nil || parent != filepath.Dir(output) {
		return errors.New("unsafe output parent")
	}
	rel, e := filepath.Rel(id.CanonicalPath, output)
	if e != nil || rel == "." || rel != ".." && !strings.HasPrefix(rel, "../") {
		return errors.New("output overlaps payload root")
	}
	root, e := os.OpenRoot(id.CanonicalPath)
	if e != nil {
		return e
	}
	receipt, e := recoveryexport.ReadImportReceipt(ctx, root)
	if e = errors.Join(e, root.Close()); e != nil {
		return e
	}
	if receipt.Kind != "coldkeep-import-ready/v1" || receipt.TargetRootHash != id.Hash || receipt.Database != c.Database || receipt.Role != c.User || receipt.Host != c.Host || receipt.Port != c.Port || receipt.KeyRequired != (len(key) > 0) || receipt.KeyReference != c.KeyReference {
		return errors.New("ready receipt/target/key mismatch")
	}
	for _, v := range os.Environ() {
		if strings.HasPrefix(v, "PG") || strings.HasPrefix(v, "PQGO_DEBUG=") || strings.HasPrefix(v, "COLDKEEP_KEY=") {
			return errors.New("ambient database/key settings refused")
		}
	}
	if len(key) > 0 {
		if e = os.Setenv("COLDKEEP_KEY", hex.EncodeToString(key)); e != nil {
			return e
		}
		defer func() { err = errors.Join(err, os.Unsetenv("COLDKEEP_KEY")) }()
	}
	quote := func(v string) string { return "'" + strings.NewReplacer("\\", "\\\\", "'", "\\'").Replace(v) + "'" }
	pc, e := pq.NewConfig("host=" + quote(c.Host) + " port=" + strconv.Itoa(int(c.Port)) + " dbname=" + quote(c.Database) + " user=" + quote(c.User) + " password=" + quote(password) + " sslmode=disable connect_timeout=5 application_name=coldkeep-recovery-reader default_transaction_read_only=off row_security=off statement_timeout=60000 lock_timeout=1000")
	if e != nil {
		return errors.New("explicit reader connection invalid")
	}
	connector, e := pq.NewConnectorConfig(pc)
	if e != nil {
		return errors.New("reader connector invalid")
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	lease, e := coordination.AcquireExisting(ctx, id)
	if e != nil {
		return errors.Join(e, db.Close())
	}
	defer func() { err = errors.Join(err, db.Close(), lease.Release()) }()
	var oid, pinsBefore, pinsAfter int64
	if e = db.QueryRowContext(ctx, "SELECT oid::bigint FROM pg_database WHERE datname=current_database()").Scan(&oid); e != nil || oid != receipt.DatabaseOID {
		return errors.New("reader database identity changed")
	}
	if e = db.QueryRowContext(ctx, "SELECT COALESCE(sum(pin_count),0) FROM chunk").Scan(&pinsBefore); e != nil {
		return e
	}
	entries, e := snapshot.ListSnapshotFiles(ctx, db, selection, 20000, nil)
	if e != nil {
		return e
	}
	if e = os.Mkdir(output, 0700); e != nil {
		return e
	}
	sc := storage.StorageContext{DB: db, ContainerDir: id.CanonicalPath}
	// The established snapshot wrapper checks cancellation between files. Run
	// this adapter in an owned, time-bounded child for in-flight cancellation.
	r, e := snapshot.RestoreSnapshot(ctx, db, selection, nil, snapshot.RestoreSnapshotOptions{DestinationMode: storage.RestoreDestinationPrefix, Destination: output, StorageContext: &sc, StrictMetadata: true})
	if e != nil {
		return e
	}
	if e = db.QueryRowContext(ctx, "SELECT COALESCE(sum(pin_count),0) FROM chunk").Scan(&pinsAfter); e != nil {
		return e
	}
	if pinsBefore != pinsAfter {
		return errors.New("reader pins unbalanced")
	}
	type fileResult struct {
		Path   string `json:"path"`
		SHA256 string `json:"sha256"`
		Mode   uint32 `json:"mode"`
		MTime  string `json:"mtime"`
	}
	files := []fileResult{}
	for _, p := range r.OutputPaths {
		rel, e := filepath.Rel(output, p)
		if e != nil || rel == ".." || strings.HasPrefix(rel, "../") {
			return errors.New("restore containment failure")
		}
		f, e := os.Open(p)
		if e != nil {
			return e
		}
		h := sha256.New()
		_, e = io.Copy(h, f)
		info, x := f.Stat()
		e = errors.Join(e, x, f.Close())
		if e != nil {
			return e
		}
		files = append(files, fileResult{rel, hex.EncodeToString(h.Sum(nil)), uint32(info.Mode().Perm()), info.ModTime().UTC().Format(time.RFC3339Nano)})
	}
	if e = lease.Check(); e != nil {
		return e
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"snapshot": selection, "selection": entries, "files": files, "pins_before": pinsBefore, "pins_after": pinsAfter, "warnings": r.Warnings, "target": receipt.Database, "reader": "coldkeep-recovery schema17"})
}
