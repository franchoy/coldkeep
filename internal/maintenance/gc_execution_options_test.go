package maintenance

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/fsx"
	"github.com/franchoy/coldkeep/internal/fsx/faultfs"
	_ "github.com/mattn/go-sqlite3"
)

func newGCExecutionOptionsTestDB(t *testing.T, filenames ...string) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "gc-options.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	for _, filename := range filenames {
		if _, err := dbconn.Exec(
			`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
			 VALUES ($1, 0, $2, TRUE, FALSE)`,
			filename,
			container.GetContainerMaxSize(),
		); err != nil {
			t.Fatalf("insert container %q: %v", filename, err)
		}
	}
	return dbconn
}

func writeGCExecutionOptionFiles(t *testing.T, dir string, filenames ...string) {
	t.Helper()
	for _, filename := range filenames {
		if err := os.WriteFile(filepath.Join(dir, filename), []byte(filename), 0o600); err != nil {
			t.Fatalf("write container %q: %v", filename, err)
		}
	}
}

func TestGCDefaultExecutionOptionsPreserveExistingBehavior(t *testing.T) {
	t.Parallel()

	defaultDB := newGCExecutionOptionsTestDB(t, "default.bin")
	optionsDB := newGCExecutionOptionsTestDB(t, "default.bin")
	defaultDir := t.TempDir()
	optionsDir := t.TempDir()
	writeGCExecutionOptionFiles(t, defaultDir, "default.bin")
	writeGCExecutionOptionFiles(t, optionsDir, "default.bin")
	defaultResult, err := RunGCWithDB(context.Background(), defaultDB, true, defaultDir)
	if err != nil {
		t.Fatalf("RunGCWithDB: %v", err)
	}
	optionsResult, err := runGCWithDBOptions(context.Background(), optionsDB, true, optionsDir, gcExecutionOptions{})
	if err != nil {
		t.Fatalf("runGCWithDBOptions: %v", err)
	}
	if !reflect.DeepEqual(defaultResult, optionsResult) {
		t.Fatalf("default result = %+v, options result = %+v", defaultResult, optionsResult)
	}
}

func TestGCInjectedFilesystemIsInvocationScoped(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	faultedPath := filepath.Join(dir, "faulted.bin")
	defaultPath := filepath.Join(dir, "default.bin")
	if err := os.WriteFile(faultedPath, []byte("faulted"), 0o600); err != nil {
		t.Fatalf("write faulted fixture: %v", err)
	}
	if err := os.WriteFile(defaultPath, []byte("default"), 0o600); err != nil {
		t.Fatalf("write default fixture: %v", err)
	}

	script := faultfs.NewScript(faultfs.Fault{Op: faultfs.OpRemove, Err: faultfs.ErrFaultRemove})
	faultedOptions := gcExecutionOptions{fs: faultfs.New(fsx.Default(), script)}
	defaultOptions := gcExecutionOptions{}
	removeContainerFileWithFS(faultedOptions.effectiveFS(), faultedPath)
	removeContainerFileWithFS(defaultOptions.effectiveFS(), defaultPath)

	if _, err := os.Stat(faultedPath); err != nil {
		t.Fatalf("faulted invocation file should remain: %v", err)
	}
	if _, err := os.Stat(defaultPath); !os.IsNotExist(err) {
		t.Fatalf("default invocation file should be removed, stat error = %v", err)
	}
	if got := script.CallCount(faultfs.OpRemove); got != 1 {
		t.Fatalf("faulted invocation remove calls = %d, want 1", got)
	}
}

func TestGCDispatchObserverSeesCurrentSerialUnits(t *testing.T) {
	t.Parallel()

	dbconn := newGCExecutionOptionsTestDB(t, "a.bin", "b.bin")
	dir := t.TempDir()
	writeGCExecutionOptionFiles(t, dir, "a.bin", "b.bin")
	var observed []gcDispatchUnit
	result, err := runGCWithDBOptions(context.Background(), dbconn, true, dir, gcExecutionOptions{
		dispatchObserver: func(unit gcDispatchUnit) { observed = append(observed, unit) },
	})
	if err != nil {
		t.Fatalf("run GC: %v", err)
	}
	if result.AffectedContainers != 2 {
		t.Fatalf("affected containers = %d, want 2", result.AffectedContainers)
	}
	if len(observed) != 2 {
		t.Fatalf("observed units = %v, want 2", observed)
	}
	for i, wantFilename := range []string{"a.bin", "b.bin"} {
		if observed[i].Kind != gcDispatchSealedContainer || observed[i].Filename != wantFilename {
			t.Fatalf("observed[%d] = %+v, want sealed %q", i, observed[i], wantFilename)
		}
	}
}

func TestGCOptionsDoNotLeakAcrossConcurrentInvocations(t *testing.T) {
	t.Parallel()

	dbA := newGCExecutionOptionsTestDB(t, "a.bin")
	dbB := newGCExecutionOptionsTestDB(t, "b.bin")
	enteredA := make(chan struct{})
	releaseA := make(chan struct{})
	observedA := make(chan gcDispatchUnit, 1)
	observedB := make(chan gcDispatchUnit, 1)
	errA := make(chan error, 1)
	errB := make(chan error, 1)
	dirA := t.TempDir()
	dirB := t.TempDir()
	writeGCExecutionOptionFiles(t, dirA, "a.bin")
	writeGCExecutionOptionFiles(t, dirB, "b.bin")

	go func() {
		_, err := runGCWithDBOptions(context.Background(), dbA, true, dirA, gcExecutionOptions{
			dispatchObserver: func(unit gcDispatchUnit) {
				observedA <- unit
				close(enteredA)
				<-releaseA
			},
		})
		errA <- err
	}()
	<-enteredA
	go func() {
		_, err := runGCWithDBOptions(context.Background(), dbB, true, dirB, gcExecutionOptions{
			dispatchObserver: func(unit gcDispatchUnit) { observedB <- unit },
		})
		errB <- err
	}()
	if err := <-errB; err != nil {
		close(releaseA)
		t.Fatalf("invocation B: %v", err)
	}
	close(releaseA)
	if err := <-errA; err != nil {
		t.Fatalf("invocation A: %v", err)
	}
	if got := <-observedA; got.Filename != "a.bin" {
		t.Fatalf("invocation A observed %+v", got)
	}
	if got := <-observedB; got.Filename != "b.bin" {
		t.Fatalf("invocation B observed %+v", got)
	}
}
