//go:build linux

package recoveryexport

import (
	"bytes"
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
	"syscall"
	"testing"
	"time"
)

func assertIncomplete(t *testing.T, o ExportOptions, r ExportResult, e error) {
	t.Helper()
	if e == nil || r.Complete || r.Published {
		t.Fatalf("expected incomplete: result=%+v error=%v", r, e)
	}
	if c, x := Check(context.Background(), o.Destination, o.Limits); x == nil || c.ByteComplete {
		t.Fatalf("checker accepted incomplete generation: %+v %v", c, x)
	}
}
func TestRecoveryFilesystemFailures(t *testing.T) {
	for _, name := range []string{"short-write", "enospc", "payload-close", "manifest-sync", "directory-sync", "post-publication-sync", "destination-digest", "source-missing", "source-changed", "archive-truncated"} {
		t.Run(name, func(t *testing.T) {
			f := exportFixture(t)
			o := exportOptions(t)
			h := &exportHooks{}
			published := false
			switch name {
			case "short-write":
				h.write = func(_ *os.File, b []byte) (int, error) { return len(b) - 1, nil }
			case "enospc":
				h.write = func(_ *os.File, _ []byte) (int, error) { return 0, syscall.ENOSPC }
			case "payload-close":
				h.close = func(file *os.File) error {
					e := file.Close()
					if strings.HasPrefix(filepath.Base(file.Name()), "object-") {
						return errors.Join(e, syscall.EIO)
					}
					return e
				}
			case "manifest-sync", "directory-sync", "post-publication-sync":
				h.event = func(stage string) error {
					if stage == "published" {
						published = true
					}
					return nil
				}
				h.sync = func(file *os.File) error {
					info, e := file.Stat()
					if e != nil {
						return e
					}
					if name == "manifest-sync" && filepath.Base(file.Name()) == "manifest.json" || name == "directory-sync" && info.IsDir() || name == "post-publication-sync" && info.IsDir() && published {
						return syscall.EIO
					}
					return file.Sync()
				}
			case "destination-digest":
				h.event = func(stage string) error {
					if stage == "payloads-written" {
						return os.WriteFile(filepath.Join(o.Destination, "object-000001.bin"), []byte("altered"), 0600)
					}
					return nil
				}
			case "source-missing", "source-changed":
				p := payloadPath(t, f)
				h.event = func(stage string) error {
					if stage == "snapshot-exported" {
						if name == "source-missing" {
							return os.Remove(p)
						}
						b, e := os.ReadFile(p)
						if e != nil {
							return e
						}
						b[len(b)-1] ^= 1
						return os.WriteFile(p, b, 0600)
					}
					return nil
				}
			case "archive-truncated":
				h.event = func(stage string) error {
					if stage == "archive-written" {
						return os.Truncate(filepath.Join(o.Destination, "catalog.dump"), 16)
					}
					return nil
				}
			}
			var r ExportResult
			var e error
			if strings.HasPrefix(name, "source-") {
				r, e = export(context.Background(), f.config, o, h)
				assertNoExportSessions(t, f)
			} else {
				r, e = exportPreserved(t, f, o, h)
			}
			if name == "short-write" && !errors.Is(e, io.ErrShortWrite) {
				t.Fatalf("lost short-write error: %v", e)
			}
			if name == "enospc" && !errors.Is(e, syscall.ENOSPC) {
				t.Fatalf("lost ENOSPC: %v", e)
			}
			if name == "post-publication-sync" {
				c, x := Check(context.Background(), o.Destination, o.Limits)
				if e == nil || !r.Published || r.Complete || x != nil || !c.ByteComplete {
					t.Fatalf("published error not reconciled: %+v %v %v", r, e, x)
				}
			} else {
				assertIncomplete(t, o, r, e)
			}
			t.Logf("SCENARIO filesystem=%s expected refusal (post-publication retains marker) observed published=%v error=%v", name, r.Published, e)
		})
	}
}
func TestRecoveryCancellationStages(t *testing.T) {
	for _, stage := range []string{"archive-written", "payload-copy", "source-cleaned", "before-publication"} {
		t.Run(stage, func(t *testing.T) {
			f := exportFixture(t)
			o := exportOptions(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			catalog, tree := catalogFingerprint(t, f.db), treeFingerprint(t, f.root)
			r, e := export(ctx, f.config, o, &exportHooks{event: func(s string) error {
				if s == stage {
					cancel()
				}
				return nil
			}})
			assertIncomplete(t, o, r, e)
			assertNoExportSessions(t, f)
			if catalogFingerprint(t, f.db) != catalog || treeFingerprint(t, f.root) != tree {
				t.Fatal("source mutation")
			}
		})
	}
}
func TestRecoveryDestinationAndLimits(t *testing.T) {
	for _, name := range []string{"source-contained", "source-alias", "existing", "relative", "archive", "manifest", "total", "members", "duration", "overflow"} {
		t.Run(name, func(t *testing.T) {
			f := exportFixture(t)
			o := exportOptions(t)
			sentinel := filepath.Join(t.TempDir(), "sentinel")
			must(t, os.WriteFile(sentinel, []byte("unrelated"), 0600))
			switch name {
			case "source-contained":
				o.Destination = filepath.Join(f.root, "generation")
			case "source-alias":
				alias := filepath.Join(t.TempDir(), "alias")
				must(t, os.Symlink(f.root, alias))
				o.Destination = filepath.Join(alias, "generation")
			case "existing":
				must(t, os.Mkdir(o.Destination, 0700))
				must(t, os.WriteFile(filepath.Join(o.Destination, "keep"), []byte("unrelated"), 0600))
			case "relative":
				o.Destination = "relative-generation"
			case "archive":
				o.Limits.ArchiveBytes = 16
			case "manifest":
				o.Limits.ManifestBytes = 16
			case "total":
				o.Limits.TotalBytes = 16
			case "members":
				o.Limits.Members = 1
			case "duration":
				o.Limits.Duration = time.Nanosecond
			case "overflow":
				o.Limits.TotalBytes = int64(^uint64(0) >> 1)
			}
			r, e := exportPreserved(t, f, o, nil)
			assertIncomplete(t, o, r, e)
			b, x := os.ReadFile(sentinel)
			must(t, x)
			if string(b) != "unrelated" {
				t.Fatal("unrelated destination changed")
			}
			if name == "existing" {
				b, x = os.ReadFile(filepath.Join(o.Destination, "keep"))
				must(t, x)
				if string(b) != "unrelated" {
					t.Fatal("existing generation replaced")
				}
			}
		})
	}
}
func TestRecoveryToolAndConfigRefusals(t *testing.T) {
	for _, name := range []string{"missing-tool", "wrong-major", "mismatched-client", "nonzero", "empty", "stdout-overflow", "stderr-overflow", "expired-snapshot", "privilege", "policy", "schema", "hostile-database", "hostile-user"} {
		t.Run(name, func(t *testing.T) {
			f := exportFixture(t)
			o := exportOptions(t)
			h := &exportHooks{}
			switch name {
			case "missing-tool":
				o.Tools.PGDump = "/nonexistent/coldkeep-pg-dump"
			case "wrong-major", "mismatched-client":
				version := "17.0"
				if name == "mismatched-client" {
					version = "16.0"
				}
				p := filepath.Join(t.TempDir(), "pg_dump")
				must(t, os.WriteFile(p, []byte("#!/bin/sh\nprintf 'pg_dump (PostgreSQL) "+version+"\\n'\n"), 0700))
				o.Tools.PGDump = p
			case "nonzero":
				o.Tools.PGDump = fakeDump(t, "exit 9")
			case "empty":
				o.Tools.PGDump = fakeDump(t, "exit 0")
			case "stdout-overflow":
				o.Limits.ArchiveBytes = 1024
				o.Tools.PGDump = fakeDump(t, "exec /usr/bin/head -c 65536 /dev/zero")
			case "stderr-overflow":
				o.Tools.PGDump = fakeDump(t, "exec /usr/bin/head -c 131072 /dev/zero >&2")
			case "expired-snapshot":
				var tx *sql.Tx
				h.cut = &hooks{event: func(stage string, t *sql.Tx) error {
					if t != nil {
						tx = t
					}
					return nil
				}}
				h.event = func(stage string) error {
					if stage == "snapshot-exported" {
						return tx.Rollback()
					}
					return nil
				}
			case "privilege":
				execSQL(t, f.db, "REVOKE SELECT ON ALL SEQUENCES IN SCHEMA public FROM "+pqIdent(f.config.User))
			case "policy":
				execSQL(t, f.db, "ALTER TABLE logical_file ENABLE ROW LEVEL SECURITY; CREATE POLICY hidden ON logical_file USING(false)")
			case "schema":
				execSQL(t, f.db, "CREATE SCHEMA unsupported")
			case "hostile-database":
				f.config.Database = "host=127.0.0.1 port=1 dbname=postgres"
			case "hostile-user":
				f.config.User = "user=x dbname=postgres"
			}
			r, e := exportPreserved(t, f, o, h)
			assertIncomplete(t, o, r, e)
		})
	}
}
func TestRecoveryAmbientIsolation(t *testing.T) {
	f := exportFixture(t)
	o := exportOptions(t)
	h := &exportHooks{event: func(stage string) error {
		if stage == "snapshot-exported" {
			for k, v := range map[string]string{"PGHOST": "203.0.113.1", "PGPORT": "1", "PGDATABASE": "postgres", "PGUSER": "untrusted", "PGPASSWORD": "untrusted", "PGSERVICE": "untrusted", "PGSERVICEFILE": "/nonexistent", "PGPASSFILE": "/nonexistent", "PGOPTIONS": "-c search_path=untrusted", "PGSSLMODE": "verify-full"} {
				t.Setenv(k, v)
			}
		}
		return nil
	}}
	r, e := exportPreserved(t, f, o, h)
	must(t, e)
	if !r.Complete {
		t.Fatal(r)
	}
	entries, e := os.ReadDir(o.Destination)
	must(t, e)
	key, e := f.config.Key(context.Background())
	must(t, e)
	defer clear(key)
	password, e := f.config.Password(context.Background())
	must(t, e)
	for _, entry := range entries {
		b, e := os.ReadFile(filepath.Join(o.Destination, entry.Name()))
		must(t, e)
		for _, secret := range [][]byte{key, []byte(hex.EncodeToString(key)), []byte(password)} {
			if len(secret) > 0 && bytes.Contains(b, secret) {
				t.Fatal("secret included in generation")
			}
		}
	}
}

func TestRecoveryCompletedChecker(t *testing.T) {
	if os.Getenv("COLDKEEP_PHASE1C_TEST") != "1" {
		t.Skip("requires explicit Phase 1C disposable PostgreSQL and client tools")
	}
	// The parent owns the completed generation; child cleanup destroys its source
	// root and databases before Check is exercised without any source capability.
	dest := filepath.Join(t.TempDir(), "generation")
	limits := DefaultExportLimits()
	t.Run("produce-and-remove-source", func(t *testing.T) {
		f := exportFixture(t)
		o := exportOptions(t)
		o.Destination = dest
		r, e := exportPreserved(t, f, o, nil)
		must(t, e)
		if !r.Complete {
			t.Fatal(r)
		}
	})
	c, e := Check(context.Background(), dest, limits)
	must(t, e)
	if !c.ByteComplete {
		t.Fatal(c)
	}
	clone := func(t *testing.T) string {
		t.Helper()
		p := t.TempDir()
		entries, e := os.ReadDir(dest)
		must(t, e)
		for _, entry := range entries {
			b, e := os.ReadFile(filepath.Join(dest, entry.Name()))
			must(t, e)
			must(t, os.WriteFile(filepath.Join(p, entry.Name()), b, 0600))
		}
		return p
	}
	for _, name := range []string{"missing-marker", "missing-member", "object-altered", "manifest-altered", "marker-generation", "future-version", "wrong-size", "wrong-digest", "overflow", "duplicate-member", "duplicate-key", "traversal", "absolute", "symlink", "fifo", "hardlink", "extra", "nested", "depth", "manifest-limit", "count-limit", "total-limit", "unknown-field", "archive-header"} {
		t.Run(name, func(t *testing.T) {
			p := clone(t)
			l := limits
			m := c.Manifest
			m.Members = append([]Member(nil), m.Members...)
			manifestChange := false
			member := filepath.Join(p, "object-000001.bin")
			switch name {
			case "missing-marker":
				must(t, os.Remove(filepath.Join(p, "COMPLETE")))
			case "missing-member":
				must(t, os.Remove(member))
			case "object-altered":
				must(t, os.WriteFile(member, []byte("wrong"), 0600))
			case "manifest-altered":
				must(t, os.WriteFile(filepath.Join(p, "manifest.json"), []byte("{}"), 0600))
			case "marker-generation":
				must(t, os.WriteFile(filepath.Join(p, "COMPLETE"), []byte(`{"package_version":1,"generation":"00000000000000000000000000000000","manifest_sha256":"x"}`), 0600))
			case "future-version":
				m.Version++
				manifestChange = true
			case "wrong-size":
				m.Members[1].Size++
				manifestChange = true
			case "wrong-digest":
				m.Members[1].SHA256 = strings.Repeat("0", 64)
				manifestChange = true
			case "overflow":
				m.Members[1].Size = int64(^uint64(0) >> 1)
				manifestChange = true
			case "duplicate-member":
				m.Members = append(m.Members, m.Members[1])
				manifestChange = true
			case "traversal":
				m.Members[1].Name = "../outside"
				manifestChange = true
			case "absolute":
				m.Members[1].Name = "/etc/passwd"
				manifestChange = true
			case "symlink":
				must(t, os.Remove(member))
				must(t, os.Symlink(filepath.Join(dest, "object-000001.bin"), member))
			case "fifo":
				must(t, os.Remove(member))
				must(t, syscall.Mkfifo(member, 0600))
			case "hardlink":
				must(t, os.Link(member, filepath.Join(p, "alias")))
			case "extra":
				must(t, os.WriteFile(filepath.Join(p, "extra"), []byte("x"), 0600))
			case "nested":
				must(t, os.Mkdir(filepath.Join(p, "nested"), 0700))
			case "depth":
				must(t, os.WriteFile(filepath.Join(p, "manifest.json"), []byte(strings.Repeat("[", 20)+"0"+strings.Repeat("]", 20)), 0600))
			case "manifest-limit":
				l.ManifestBytes = 16
			case "count-limit":
				l.Members = 1
			case "total-limit":
				l.TotalBytes = 16
			case "duplicate-key", "unknown-field":
				b, e := os.ReadFile(filepath.Join(p, "manifest.json"))
				must(t, e)
				prefix := `{"package_version":1,`
				if name == "unknown-field" {
					prefix = `{"unknown":true,`
				}
				must(t, os.WriteFile(filepath.Join(p, "manifest.json"), append([]byte(prefix), b[1:]...), 0600))
			case "archive-header":
				b, e := os.ReadFile(filepath.Join(p, "catalog.dump"))
				must(t, e)
				b[6]++
				must(t, os.WriteFile(filepath.Join(p, "catalog.dump"), b, 0600))
				digest := sha256.Sum256(b)
				m.Members[0].SHA256 = hex.EncodeToString(digest[:])
				manifestChange = true
			}
			if manifestChange {
				b, e := json.Marshal(m)
				must(t, e)
				must(t, os.WriteFile(filepath.Join(p, "manifest.json"), b, 0600))
				digest := sha256.Sum256(b)
				marker, e := json.Marshal(completionMarker{packageVersion, m.Generation, hex.EncodeToString(digest[:])})
				must(t, e)
				must(t, os.WriteFile(filepath.Join(p, "COMPLETE"), marker, 0600))
			}
			got, e := Check(context.Background(), p, l)
			if e == nil || got.ByteComplete {
				t.Fatalf("checker accepted %s", name)
			}
		})
	}
}
