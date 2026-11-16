package recoveryexport

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/db"
	"golang.org/x/sys/unix"
)

func TestBindingSourceCapacityAndAvailability(t *testing.T) {
	f, o := bindingFixture(t)
	_, e := ApplyBinding(context.Background(), f.config, o)
	must(t, e)
	source := t.TempDir()
	var selected string
	for n := 1; n <= BindingMaxSources; n++ {
		id, e := db.NewIdentity("source")
		must(t, e)
		q := nextBinding(t, f, o, BindingSourceRegister, id, source, 0)
		_, e = ApplyBinding(context.Background(), f.config, q)
		must(t, e)
		selected = id
		if n == BindingMaxSources-1 || n == BindingMaxSources {
			var count int
			must(t, f.db.QueryRow("SELECT count(*) FROM configured_source").Scan(&count))
			if count != n {
				t.Fatal(count)
			}
			t.Logf("actual configured sources=%d", count)
		}
	}
	id, e := db.NewIdentity("source")
	must(t, e)
	op, e := bindingID("bind")
	must(t, e)
	encoded, e := EncodeBindingPath(source)
	must(t, e)
	q := BindingRequest{Action: BindingSourceRegister, InstallationDirectory: o.Request.InstallationDirectory, ExpectedSchema: 19, ExpectedRevision: 65, OperationID: op, SourceID: id, SourcePathBase64: encoded}
	before := catalogFingerprint(t, f.db)
	if _, e = PreviewBinding(context.Background(), f.config, q, DefaultLimits()); e == nil {
		t.Fatal("65th source admitted")
	}
	if before != catalogFingerprint(t, f.db) {
		t.Fatal("source capacity mutated catalog")
	}
	t.Run("read-only", func(t *testing.T) {
		must(t, os.Chmod(source, 0500))
		r, e := SourceBindingStatus(context.Background(), o.Request.InstallationDirectory, f.config, selected)
		must(t, e)
		if r.Status != "source-available; capture disabled" {
			t.Fatal(r)
		}
	})
	original := source + "-original"
	must(t, os.Rename(source, original))
	defer func() { must(t, os.RemoveAll(original)) }()
	t.Run("missing", func(t *testing.T) {
		r, e := SourceBindingStatus(context.Background(), o.Request.InstallationDirectory, f.config, selected)
		if e == nil || r.Status != "source-unavailable" {
			t.Fatal(r, e)
		}
	})
	must(t, os.Mkdir(source, 0700))
	t.Run("empty-substitution", func(t *testing.T) {
		watch := bindingWatchNoWrites(t, source)
		r, e := SourceBindingStatus(context.Background(), o.Request.InstallationDirectory, f.config, selected)
		if e == nil || r.Status != "source-binding-mismatch" {
			t.Fatal(r, e)
		}
		watch()
	})
}
func bindingWatchNoWrites(t *testing.T, path string) func() {
	t.Helper()
	fd, e := unix.InotifyInit1(unix.IN_NONBLOCK | unix.IN_CLOEXEC)
	must(t, e)
	_, e = unix.InotifyAddWatch(fd, path, unix.IN_CREATE|unix.IN_MOVED_TO|unix.IN_MODIFY|unix.IN_DELETE)
	must(t, e)
	return func() {
		defer func() { must(t, unix.Close(fd)) }()
		b := make([]byte, 8192)
		n, e := unix.Read(fd, b)
		if n > 0 || e != unix.EAGAIN {
			t.Fatalf("transient mutation observed: bytes=%d error=%v at %s", n, e, filepath.Base(path))
		}
	}
}
