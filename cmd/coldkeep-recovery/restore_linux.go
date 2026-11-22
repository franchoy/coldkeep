package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/franchoy/coldkeep/v2/internal/coordination"
	"github.com/franchoy/coldkeep/v2/internal/recoveryexport"
	"github.com/franchoy/coldkeep/v2/internal/snapshot"
	"github.com/franchoy/coldkeep/v2/internal/storage"
)

func restore(ctx context.Context, c config, password string, key []byte, selection, output string) (err error) {
	id, receipt, e := admitRestore(ctx, c, key, selection, output)
	if e != nil {
		return e
	}
	if len(key) > 0 {
		if e = os.Setenv("COLDKEEP_KEY", hex.EncodeToString(key)); e != nil {
			return e
		}
		defer func() { err = errors.Join(err, os.Unsetenv("COLDKEEP_KEY")) }()
	}
	lease, e := coordination.AcquireExisting(ctx, id)
	if e != nil {
		return e
	}
	defer func() { err = errors.Join(err, lease.Release()) }()
	reader, e := recoveryexport.OpenRecoverySession(ctx, restoreConfig(c, password, key), lease, receipt)
	if e != nil {
		return e
	}
	// Registration order keeps physical reader cleanup before lease release,
	// then environment removal. Encoding still occurs before these defers.
	defer func() { err = errors.Join(err, reader.Close()) }()
	result, e := restoreSnapshotOutput(ctx, reader.DB, id.CanonicalPath, selection, output)
	if e != nil {
		return e
	}
	if e = lease.Check(); e != nil {
		return e
	}
	result["target"] = receipt.Database
	result["reader"] = receipt.Reader
	return json.NewEncoder(os.Stdout).Encode(result)
}

func restoreOutputIdentity(c config, selection, output string) (coordination.Identity, error) {
	if selection == "" || !filepath.IsAbs(output) || filepath.Clean(output) != output {
		return coordination.Identity{}, errors.New("explicit snapshot and fresh absolute output required")
	}
	id, e := coordination.ResolveIdentity(c.PayloadRoot)
	if e != nil {
		return id, e
	}
	return id, safeRestoreParent(id.CanonicalPath, output)
}

func safeRestoreParent(root, output string) error {
	parent, e := filepath.EvalSymlinks(filepath.Dir(output))
	if e != nil || parent != filepath.Dir(output) {
		return errors.New("unsafe output parent")
	}
	rel, e := filepath.Rel(root, output)
	if e != nil || rel == "." || rel != ".." && !strings.HasPrefix(rel, "../") {
		return errors.New("output overlaps payload root")
	}
	return nil
}

func admitRestore(ctx context.Context, c config, key []byte, selection, output string) (coordination.Identity, recoveryexport.ImportReceipt, error) {
	id, e := restoreOutputIdentity(c, selection, output)
	if e != nil {
		return id, recoveryexport.ImportReceipt{}, e
	}
	receipt, e := readRestoreReceipt(ctx, id.CanonicalPath)
	if e != nil {
		return id, receipt, e
	}
	if !restoreReceiptMatches(receipt, id, c, key) {
		return id, receipt, errors.New("ready receipt/target/key mismatch")
	}
	return id, receipt, refuseRestoreEnvironment()
}

func readRestoreReceipt(ctx context.Context, path string) (recoveryexport.ImportReceipt, error) {
	root, e := os.OpenRoot(path)
	if e != nil {
		return recoveryexport.ImportReceipt{}, e
	}
	receipt, e := recoveryexport.ReadImportReceipt(ctx, root)
	return receipt, errors.Join(e, root.Close())
}

func restoreReceiptKind(kind string) bool {
	return kind == "coldkeep-import-ready/v1" || kind == "coldkeep-import-ready/v2" || kind == "coldkeep-import-ready/v3" || kind == "coldkeep-import-ready/v4"
}

func restoreReceiptMatches(receipt recoveryexport.ImportReceipt, id coordination.Identity, c config, key []byte) bool {
	if !restoreReceiptKind(receipt.Kind) {
		return false
	}
	return receipt.TargetRootHash == id.Hash && receipt.Database == c.Database && receipt.Role == c.User && receipt.Host == c.Host && receipt.Port == c.Port && receipt.KeyRequired == (len(key) > 0) && receipt.KeyReference == c.KeyReference
}

func refuseRestoreEnvironment() error {
	for _, v := range os.Environ() {
		if strings.HasPrefix(v, "PG") || strings.HasPrefix(v, "PQGO_DEBUG=") || strings.HasPrefix(v, "COLDKEEP_KEY=") {
			return errors.New("ambient database/key settings refused")
		}
	}
	return nil
}

func restoreConfig(c config, password string, key []byte) recoveryexport.Config {
	validation := recoveryexport.Config{Backend: "postgres", Profile: "manual-postgresql-docker", Transport: "local-plaintext", Host: c.Host, Port: c.Port, Database: c.Database, User: c.User, PayloadRoot: c.PayloadRoot, KeyReference: c.KeyReference, Password: func(context.Context) (string, error) { return password, nil }}
	if key != nil {
		validation.Key = func(context.Context) ([]byte, error) { return key, nil }
	}
	return validation
}

func restoreSnapshotOutput(ctx context.Context, db *sql.DB, root, selection, output string) (map[string]any, error) {
	var pinsBefore int64
	if e := db.QueryRowContext(ctx, "SELECT COALESCE(sum(pin_count),0) FROM chunk").Scan(&pinsBefore); e != nil {
		return nil, e
	}
	entries, e := snapshot.ListSnapshotFiles(ctx, db, selection, 20000, nil)
	if e != nil {
		return nil, e
	}
	if e = os.Mkdir(output, 0700); e != nil {
		return nil, e
	}
	sc := storage.StorageContext{DB: db, ContainerDir: root}
	// The established snapshot wrapper checks cancellation between files. Run
	// this adapter in an owned, time-bounded child for in-flight cancellation.
	r, e := snapshot.RestoreSnapshot(ctx, db, selection, nil, snapshot.RestoreSnapshotOptions{DestinationMode: storage.RestoreDestinationPrefix, Destination: output, StorageContext: &sc, StrictMetadata: true})
	if e != nil {
		return nil, e
	}
	pinsAfter, e := restorePinsAfter(ctx, db, pinsBefore)
	if e != nil {
		return nil, e
	}
	files, e := collectRestoreFiles(output, r.OutputPaths)
	if e != nil {
		return nil, e
	}
	return map[string]any{"snapshot": selection, "selection": entries, "files": files, "pins_before": pinsBefore, "pins_after": pinsAfter, "warnings": r.Warnings}, nil
}

func restorePinsAfter(ctx context.Context, db *sql.DB, pinsBefore int64) (int64, error) {
	var pinsAfter int64
	if e := db.QueryRowContext(ctx, "SELECT COALESCE(sum(pin_count),0) FROM chunk").Scan(&pinsAfter); e != nil {
		return pinsAfter, e
	}
	if pinsBefore != pinsAfter {
		return pinsAfter, errors.New("reader pins unbalanced")
	}
	return pinsAfter, nil
}

type restoreFileResult struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Mode   uint32 `json:"mode"`
	MTime  string `json:"mtime"`
}

func collectRestoreFiles(output string, paths []string) ([]restoreFileResult, error) {
	files := []restoreFileResult{}
	for _, p := range paths {
		file, e := readRestoreFile(output, p)
		if e != nil {
			return nil, e
		}
		files = append(files, file)
	}
	return files, nil
}

func readRestoreFile(output, path string) (restoreFileResult, error) {
	rel, e := filepath.Rel(output, path)
	if e != nil || rel == ".." || strings.HasPrefix(rel, "../") {
		return restoreFileResult{}, errors.New("restore containment failure")
	}
	f, e := os.Open(path)
	if e != nil {
		return restoreFileResult{}, e
	}
	h := sha256.New()
	_, e = io.Copy(h, f)
	info, x := f.Stat()
	e = errors.Join(e, x, f.Close())
	if e != nil {
		return restoreFileResult{}, e
	}
	return restoreFileResult{rel, hex.EncodeToString(h.Sum(nil)), uint32(info.Mode().Perm()), info.ModTime().UTC().Format(time.RFC3339Nano)}, nil
}
