package maintenance

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/fsx"
	"github.com/franchoy/coldkeep/internal/fsx/faultfs"
)

func TestGarbageCollectWorkersBoundConcurrencyAndPreserveOrder(t *testing.T) {
	dbconn := newGCExecutionOptionsTestDB(t, "a.bin", "b.bin")
	containersDir := t.TempDir()
	for _, filename := range []string{"a.bin", "b.bin"} {
		if err := os.WriteFile(filepath.Join(containersDir, filename), []byte(filename), 0o600); err != nil {
			t.Fatalf("write %s: %v", filename, err)
		}
	}

	entered := make(chan gcDispatchUnit, 2)
	release := make(chan struct{})
	type runResult struct {
		result GCResult
		err    error
	}
	done := make(chan runResult, 1)
	go func() {
		result, err := runGCWithDBOptions(context.Background(), dbconn, true, containersDir, gcExecutionOptions{
			workers: 2,
			dispatchObserver: func(unit gcDispatchUnit) {
				entered <- unit
				<-release
			},
		})
		done <- runResult{result: result, err: err}
	}()

	first := <-entered
	var second gcDispatchUnit
	select {
	case second = <-entered:
		close(release)
	case <-time.After(2 * time.Second):
		close(release)
		<-done
		t.Fatalf("Workers=2 observed only one executing unit (%+v); GC remained serial", first)
	}
	run := <-done
	if run.err != nil {
		t.Fatalf("run GC: %v", run.err)
	}
	observed := map[string]bool{first.Filename: true, second.Filename: true}
	if !observed["a.bin"] || !observed["b.bin"] {
		t.Fatalf("observed units = [%+v %+v], want a.bin and b.bin", first, second)
	}
	if run.result.AffectedContainers != 2 {
		t.Fatalf("affected containers = %d, want 2", run.result.AffectedContainers)
	}
	if len(run.result.ContainerFilenames) != 2 || run.result.ContainerFilenames[0] != "a.bin" || run.result.ContainerFilenames[1] != "b.bin" {
		t.Fatalf("result order = %v, want [a.bin b.bin]", run.result.ContainerFilenames)
	}
}

func phase11Plan(filenames ...string) []gcPlannedUnit {
	plan := make([]gcPlannedUnit, len(filenames))
	for i, filename := range filenames {
		plan[i] = gcPlannedUnit{
			index: i,
			dispatch: gcDispatchUnit{
				Kind:        gcDispatchSealedContainer,
				ContainerID: int64(i + 1),
				Filename:    filename,
			},
		}
	}
	return plan
}

func TestGCWorkerNormalizationAndUpperBounds(t *testing.T) {
	for _, test := range []struct {
		workers int
		limit   int
	}{
		{workers: 0, limit: 1},
		{workers: 1, limit: 1},
		{workers: 2, limit: 2},
		{workers: 4, limit: 4},
	} {
		t.Run(string(rune('0'+test.workers)), func(t *testing.T) {
			plan := phase11Plan("a", "b", "c", "d", "e", "f")
			entered := make(chan struct{}, len(plan))
			release := make(chan struct{})
			var mu sync.Mutex
			current := 0
			maxObserved := 0
			done := make(chan []gcUnitResult, 1)
			go func() {
				done <- executeGCPlan(context.Background(), plan, gcExecutionOptions{
					workers: test.workers,
					dispatchObserver: func(gcDispatchUnit) {
						mu.Lock()
						current++
						if current > maxObserved {
							maxObserved = current
						}
						mu.Unlock()
						entered <- struct{}{}
						<-release
						mu.Lock()
						current--
						mu.Unlock()
					},
				}, func(unit gcPlannedUnit) gcUnitResult {
					return gcUnitResult{plan: unit, outcome: sealedContainerAffected, physicalBytes: 1}
				})
			}()

			initial := test.limit
			if initial > len(plan) {
				initial = len(plan)
			}
			for i := 0; i < initial; i++ {
				<-entered
			}
			started := initial
			for started < len(plan) {
				release <- struct{}{}
				<-entered
				started++
			}
			for i := 0; i < initial; i++ {
				release <- struct{}{}
			}
			results := <-done
			if len(results) != len(plan) {
				t.Fatalf("started results = %d, want %d", len(results), len(plan))
			}
			if maxObserved != initial {
				t.Fatalf("workers=%d max observed=%d, want %d", test.workers, maxObserved, initial)
			}
		})
	}
}

func TestGCNonErrorTerminalResultsContinueDispatch(t *testing.T) {
	plan := phase11Plan("retained", "skipped", "affected")
	outcomes := []sealedContainerGCResult{sealedContainerRetained, sealedContainerSkipped, sealedContainerAffected}
	var observed []string
	results := executeGCPlan(context.Background(), plan, gcExecutionOptions{
		workers: 1,
		dispatchObserver: func(unit gcDispatchUnit) {
			observed = append(observed, unit.Filename)
		},
	}, func(unit gcPlannedUnit) gcUnitResult {
		return gcUnitResult{plan: unit, outcome: outcomes[unit.index], physicalBytes: 1}
	})
	if len(results) != 3 || !reflect.DeepEqual(observed, []string{"retained", "skipped", "affected"}) {
		t.Fatalf("non-error terminal dispatch = %v (%d results)", observed, len(results))
	}
}

func TestGCErrorAndCancellationStopNewDispatch(t *testing.T) {
	t.Run("unit error", func(t *testing.T) {
		wantErr := errors.New("phase11 unit failure")
		var observed []string
		results := executeGCPlan(context.Background(), phase11Plan("a", "b"), gcExecutionOptions{
			workers: 1,
			dispatchObserver: func(unit gcDispatchUnit) {
				observed = append(observed, unit.Filename)
			},
		}, func(unit gcPlannedUnit) gcUnitResult {
			return gcUnitResult{plan: unit, err: wantErr}
		})
		if !reflect.DeepEqual(observed, []string{"a"}) || len(results) != 1 || !errors.Is(results[0].err, wantErr) {
			t.Fatalf("error stop results=%+v observed=%v", results, observed)
		}
	})

	t.Run("caller cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var observed []string
		results := executeGCPlan(ctx, phase11Plan("a", "b"), gcExecutionOptions{
			workers: 1,
			dispatchObserver: func(unit gcDispatchUnit) {
				observed = append(observed, unit.Filename)
			},
		}, func(unit gcPlannedUnit) gcUnitResult {
			cancel()
			return gcUnitResult{plan: unit, outcome: sealedContainerSkipped}
		})
		if !reflect.DeepEqual(observed, []string{"a"}) || len(results) != 1 {
			t.Fatalf("cancellation stop results=%+v observed=%v", results, observed)
		}
	})
}

func TestGCStartedResultsAndMultipleErrorsUsePlanOrder(t *testing.T) {
	errA := errors.New("phase11 error a")
	errB := errors.New("phase11 error b")
	entered := make(chan int, 2)
	release := []chan struct{}{make(chan struct{}), make(chan struct{})}
	returned := make(chan int, 2)
	done := make(chan []gcUnitResult, 1)
	plan := phase11Plan("a", "b")
	go func() {
		done <- executeGCPlan(context.Background(), plan, gcExecutionOptions{workers: 2}, func(unit gcPlannedUnit) gcUnitResult {
			entered <- unit.index
			<-release[unit.index]
			returned <- unit.index
			if unit.index == 0 {
				return gcUnitResult{plan: unit, err: errA}
			}
			return gcUnitResult{plan: unit, err: errB}
		})
	}()
	<-entered
	<-entered
	close(release[1])
	if got := <-returned; got != 1 {
		t.Fatalf("first completed unit = %d, want reverse-order unit 1", got)
	}
	close(release[0])
	results := <-done
	if len(results) != 2 || results[0].plan.index != 0 || results[1].plan.index != 1 {
		t.Fatalf("result plan order = %+v", results)
	}
	var result GCResult
	joined := aggregateGCUnitResults(results, nil, &result)
	if !errors.Is(joined, errA) || !errors.Is(joined, errB) {
		t.Fatalf("joined error = %v, want both sentinels", joined)
	}
	if strings.Index(joined.Error(), errA.Error()) > strings.Index(joined.Error(), errB.Error()) {
		t.Fatalf("error order = %q, want plan order", joined)
	}
}

func TestGCPartialResultsKeepStartedSuccessesAndExcludeUnstartedUnits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wantErr := errors.New("phase11 planned unit failure")
	plan := phase11Plan("one", "two", "three", "four")
	entered := make(chan int, 3)
	canceled := make(chan struct{})
	release := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	done := make(chan []gcUnitResult, 1)
	go func() {
		done <- executeGCPlan(ctx, plan, gcExecutionOptions{workers: 3}, func(unit gcPlannedUnit) gcUnitResult {
			entered <- unit.index
			<-release[unit.index]
			if unit.index == 1 {
				cancel()
				close(canceled)
				return gcUnitResult{plan: unit, err: wantErr}
			}
			return gcUnitResult{plan: unit, outcome: sealedContainerAffected, physicalBytes: int64(unit.index + 1)}
		})
	}()
	for i := 0; i < 3; i++ {
		<-entered
	}
	close(release[1])
	<-canceled
	close(release[0])
	close(release[2])
	results := <-done
	var result GCResult
	joined := aggregateGCUnitResults(results, ctx.Err(), &result)
	if !errors.Is(joined, wantErr) || !errors.Is(joined, context.Canceled) {
		t.Fatalf("partial error = %v", joined)
	}
	if result.AffectedContainers != 2 || !reflect.DeepEqual(result.ContainerFilenames, []string{"one", "three"}) || result.BytesReclaimed != 4 {
		t.Fatalf("partial result = %+v", result)
	}
	if len(results) != 3 {
		t.Fatalf("started results = %d, want 3; unit four must remain unstarted", len(results))
	}
}

type phase11FileInfo struct {
	name string
	size int64
}

func (i phase11FileInfo) Name() string       { return i.name }
func (i phase11FileInfo) Size() int64        { return i.size }
func (i phase11FileInfo) Mode() fs.FileMode  { return 0o600 }
func (i phase11FileInfo) ModTime() time.Time { return time.Time{} }
func (i phase11FileInfo) IsDir() bool        { return false }
func (i phase11FileInfo) Sys() any           { return nil }

type phase11StatFS struct {
	fsx.FS
	mu        sync.Mutex
	sizes     map[string]int64
	errors    map[string]error
	statCalls map[string]int
}

func (f *phase11StatFS) Stat(name string) (fs.FileInfo, error) {
	base := filepath.Base(name)
	f.mu.Lock()
	if f.statCalls == nil {
		f.statCalls = map[string]int{}
	}
	f.statCalls[base]++
	err, hasErr := f.errors[base]
	size, hasSize := f.sizes[base]
	f.mu.Unlock()
	if hasErr {
		return nil, err
	}
	if hasSize {
		return phase11FileInfo{name: base, size: size}, nil
	}
	return f.FS.Stat(name)
}

func (f *phase11StatFS) calls(filename string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.statCalls[filename]
}

func assertPhase11ContainerCount(t *testing.T, dbconn *sql.DB, want int) {
	t.Helper()
	var got int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container`).Scan(&got); err != nil {
		t.Fatalf("count container rows: %v", err)
	}
	if got != want {
		t.Fatalf("container rows=%d, want %d", got, want)
	}
}

func TestGCPhysicalStatExactlyOnceAndFaultsEarnNoCredit(t *testing.T) {
	t.Run("exactly once and exact MaxInt64", func(t *testing.T) {
		dbconn := newGCExecutionOptionsTestDB(t, "a.bin", "b.bin")
		dir := t.TempDir()
		writeGCExecutionOptionFiles(t, dir, "a.bin", "b.bin")
		fsys := &phase11StatFS{
			FS:     fsx.Default(),
			sizes:  map[string]int64{"a.bin": math.MaxInt64 - 7, "b.bin": 7},
			errors: map[string]error{},
		}
		result, err := runGCWithDBOptions(context.Background(), dbconn, true, dir, gcExecutionOptions{fs: fsys, workers: 2})
		if err != nil {
			t.Fatalf("run GC: %v", err)
		}
		if result.BytesReclaimed != math.MaxInt64 || result.AffectedContainers != 2 {
			t.Fatalf("result = %+v", result)
		}
		if fsys.calls("a.bin") != 1 || fsys.calls("b.bin") != 1 {
			t.Fatalf("Stat calls: a=%d b=%d", fsys.calls("a.bin"), fsys.calls("b.bin"))
		}
	})

	t.Run("Stat failure", func(t *testing.T) {
		dbconn := newGCExecutionOptionsTestDB(t, "fault.bin")
		dir := t.TempDir()
		writeGCExecutionOptionFiles(t, dir, "fault.bin")
		fsys := &phase11StatFS{FS: fsx.Default(), errors: map[string]error{"fault.bin": faultfs.ErrFaultStat}, sizes: map[string]int64{}}
		result, err := runGCWithDBOptions(context.Background(), dbconn, true, dir, gcExecutionOptions{fs: fsys})
		if !errors.Is(err, faultfs.ErrFaultStat) || result.AffectedContainers != 0 || result.BytesReclaimed != 0 || len(result.ContainerFilenames) != 0 {
			t.Fatalf("Stat fault result=%+v err=%v", result, err)
		}
		if fsys.calls("fault.bin") != 1 {
			t.Fatalf("Stat calls=%d, want 1", fsys.calls("fault.bin"))
		}
		assertPhase11ContainerCount(t, dbconn, 1)
	})

	t.Run("missing file", func(t *testing.T) {
		dbconn := newGCExecutionOptionsTestDB(t, "missing.bin")
		result, err := runGCWithDBOptions(context.Background(), dbconn, true, t.TempDir(), gcExecutionOptions{})
		if err == nil || result.AffectedContainers != 0 || result.BytesReclaimed != 0 || len(result.ContainerFilenames) != 0 {
			t.Fatalf("missing result=%+v err=%v", result, err)
		}
		assertPhase11ContainerCount(t, dbconn, 1)
	})

	t.Run("invalid path", func(t *testing.T) {
		dbconn := newGCExecutionOptionsTestDB(t, "../escape.bin")
		result, err := runGCWithDBOptions(context.Background(), dbconn, true, t.TempDir(), gcExecutionOptions{})
		if err == nil || !strings.Contains(err.Error(), "invalid container filename") ||
			result.AffectedContainers != 0 || result.BytesReclaimed != 0 || len(result.ContainerFilenames) != 0 {
			t.Fatalf("invalid-path result=%+v err=%v", result, err)
		}
		assertPhase11ContainerCount(t, dbconn, 1)
	})

	t.Run("negative size", func(t *testing.T) {
		dbconn := newGCExecutionOptionsTestDB(t, "negative.bin")
		dir := t.TempDir()
		writeGCExecutionOptionFiles(t, dir, "negative.bin")
		fsys := &phase11StatFS{FS: fsx.Default(), sizes: map[string]int64{"negative.bin": -1}, errors: map[string]error{}}
		result, err := runGCWithDBOptions(context.Background(), dbconn, true, dir, gcExecutionOptions{fs: fsys})
		if err == nil || !strings.Contains(err.Error(), "negative size") || result.AffectedContainers != 0 || result.BytesReclaimed != 0 {
			t.Fatalf("negative-size result=%+v err=%v", result, err)
		}
		assertPhase11ContainerCount(t, dbconn, 1)
	})
}

func TestGCByteOverflowPreservesTruthfulPopulationAndExactPrefix(t *testing.T) {
	plan := phase11Plan("first.bin", "overflow.bin", "later.bin")
	results := []gcUnitResult{
		{plan: plan[0], outcome: sealedContainerAffected, physicalBytes: math.MaxInt64 - 4},
		{plan: plan[1], outcome: sealedContainerAffected, physicalBytes: 5},
		{plan: plan[2], outcome: sealedContainerAffected, physicalBytes: 9},
	}
	var result GCResult
	err := aggregateGCUnitResults(results, nil, &result)
	if err == nil || !strings.Contains(err.Error(), `plan index 1 kind=sealed_container container_id=2 filename="overflow.bin"`) {
		t.Fatalf("overflow error = %v", err)
	}
	if result.BytesReclaimed != math.MaxInt64-4 || result.BytesReclaimed < 0 {
		t.Fatalf("overflow bytes = %d", result.BytesReclaimed)
	}
	if result.AffectedContainers != 3 || !reflect.DeepEqual(result.ContainerFilenames, []string{"first.bin", "overflow.bin", "later.bin"}) {
		t.Fatalf("overflow population = %+v", result)
	}

	firstPlan := phase11Plan("active-overflow.bin")
	firstPlan[0].dispatch.Kind = gcDispatchActiveContainer
	firstPlan[0].index = 7
	preexisting := GCResult{BytesReclaimed: 1}
	err = aggregateGCUnitResults([]gcUnitResult{{plan: firstPlan[0], outcome: sealedContainerAffected, physicalBytes: math.MaxInt64}}, nil, &preexisting)
	if err == nil || preexisting.BytesReclaimed != 1 || preexisting.AffectedContainers != 1 || preexisting.BytesReclaimed < 0 {
		t.Fatalf("first-overflow result=%+v err=%v", preexisting, err)
	}
}

func newPhase11Postgres(t *testing.T, maxOpen int) *sql.DB {
	t.Helper()
	requireDB(t)
	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect DB: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	applySchema(t, dbconn)
	resetDB(t, dbconn)
	dbconn.SetMaxOpenConns(maxOpen)
	return dbconn
}

func insertPhase11Container(t *testing.T, dbconn *sql.DB, dir, filename string, payload []byte, sealed bool) int64 {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, filename), payload, 0o600); err != nil {
		t.Fatalf("write %s: %v", filename, err)
	}
	var id int64
	if err := dbconn.QueryRow(`
		INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		VALUES ($1, $2, $3, $4, FALSE)
		RETURNING id
	`, filename, int64(len(payload)), container.GetContainerMaxSize(), sealed).Scan(&id); err != nil {
		t.Fatalf("insert %s: %v", filename, err)
	}
	return id
}

func insertPhase11DeadLegacyUnit(t *testing.T, dbconn *sql.DB, containerID int64, suffix string, size int64) {
	t.Helper()
	var chunkID int64
	if err := dbconn.QueryRow(`
		INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version)
		VALUES ($1, $2, 'COMPLETED', 0, 0, 'v2-fastcdc')
		RETURNING id
	`, "phase11-dead-"+suffix, size).Scan(&chunkID); err != nil {
		t.Fatalf("insert dead chunk %s: %v", suffix, err)
	}
	if _, err := dbconn.Exec(`
		INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		VALUES ($1, 'plain', 1, $2, $2, $3, 0)
	`, chunkID, size, containerID); err != nil {
		t.Fatalf("insert dead block %s: %v", suffix, err)
	}
}

func TestGCPostgresWorkersTwoActuallyOverlap(t *testing.T) {
	dbconn := newPhase11Postgres(t, 3)
	dir := t.TempDir()
	insertPhase11Container(t, dbconn, dir, "a.bin", []byte("aaaa"), true)
	insertPhase11Container(t, dbconn, dir, "b.bin", []byte("bbbbbb"), true)

	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var mu sync.Mutex
	current := 0
	maxObserved := 0
	type runResult struct {
		result GCResult
		err    error
	}
	done := make(chan runResult, 1)
	go func() {
		result, err := runGCWithDBOptions(context.Background(), dbconn, true, dir, gcExecutionOptions{
			workers: 2,
			dispatchObserver: func(gcDispatchUnit) {
				mu.Lock()
				current++
				if current > maxObserved {
					maxObserved = current
				}
				mu.Unlock()
				entered <- struct{}{}
				<-release
				mu.Lock()
				current--
				mu.Unlock()
			},
		})
		done <- runResult{result: result, err: err}
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for deterministic Workers=2 overlap")
		}
	}
	close(release)
	run := <-done
	if run.err != nil {
		t.Fatalf("run GC: %v", run.err)
	}
	if maxObserved < 2 || maxObserved > 2 {
		t.Fatalf("maxObserved=%d, want >=2 and <=2", maxObserved)
	}
	if !reflect.DeepEqual(run.result.ContainerFilenames, []string{"a.bin", "b.bin"}) || run.result.BytesReclaimed != 10 {
		t.Fatalf("result=%+v", run.result)
	}
}

type phase11BlockingStatFS struct {
	fsx.FS
	entered chan string
	release chan struct{}
}

func (f *phase11BlockingStatFS) Stat(name string) (fs.FileInfo, error) {
	f.entered <- filepath.Base(name)
	<-f.release
	return f.FS.Stat(name)
}

func TestGCPostgresSmallerPoolNaturallySerializesWorkersTwo(t *testing.T) {
	dbconn := newPhase11Postgres(t, 2)
	dir := t.TempDir()
	insertPhase11Container(t, dbconn, dir, "a.bin", []byte("a"), true)
	insertPhase11Container(t, dbconn, dir, "b.bin", []byte("bb"), true)
	fsys := &phase11BlockingStatFS{FS: fsx.Default(), entered: make(chan string, 2), release: make(chan struct{})}
	baselineWait := dbconn.Stats().WaitCount
	type runResult struct {
		result GCResult
		err    error
	}
	done := make(chan runResult, 1)
	go func() {
		result, err := runGCWithDBOptions(context.Background(), dbconn, true, dir, gcExecutionOptions{fs: fsys, workers: 2})
		done <- runResult{result: result, err: err}
	}()
	first := <-fsys.entered
	deadline := time.After(10 * time.Second)
	for dbconn.Stats().WaitCount <= baselineWait {
		select {
		case <-deadline:
			t.Fatal("second worker transaction never waited for the smaller pool")
		default:
			runtime.Gosched()
		}
	}
	select {
	case second := <-fsys.entered:
		t.Fatalf("smaller pool allowed concurrent Stat boundaries: first=%q second=%q", first, second)
	default:
	}
	fsys.release <- struct{}{}
	second := <-fsys.entered
	if second == first {
		t.Fatalf("second serialized unit=%q, same as first", second)
	}
	fsys.release <- struct{}{}
	run := <-done
	if run.err != nil || run.result.AffectedContainers != 2 || run.result.BytesReclaimed != 3 {
		t.Fatalf("serialized result=%+v err=%v", run.result, run.err)
	}
}

func TestGCPostgresLiveBytesAndActiveAccounting(t *testing.T) {
	dbconn := newPhase11Postgres(t, 3)
	dir := t.TempDir()
	activePayload := []byte("active")
	sealedPayload := []byte("sealed-bytes")
	activeID := insertPhase11Container(t, dbconn, dir, "active.bin", activePayload, false)
	insertPhase11DeadLegacyUnit(t, dbconn, activeID, "active", int64(len(activePayload)))
	sealedID := insertPhase11Container(t, dbconn, dir, "sealed.bin", sealedPayload, true)
	var observed []gcDispatchUnit
	result, err := runGCWithDBOptions(context.Background(), dbconn, false, dir, gcExecutionOptions{
		workers: 2,
		dispatchObserver: func(unit gcDispatchUnit) {
			observed = append(observed, unit)
		},
	})
	if err != nil {
		t.Fatalf("live GC: %v", err)
	}
	if result.AffectedContainers != 2 || !reflect.DeepEqual(result.ContainerFilenames, []string{"sealed.bin", "active.bin"}) || result.BytesReclaimed != int64(len(activePayload)+len(sealedPayload)) {
		t.Fatalf("live result=%+v", result)
	}
	if len(observed) != 2 || observed[0].Kind != gcDispatchSealedContainer || observed[1].Kind != gcDispatchActiveContainer {
		t.Fatalf("pass dispatch order=%+v, want sealed fully terminal before active", observed)
	}
	for _, filename := range []string{"sealed.bin", "active.bin"} {
		if _, err := os.Stat(filepath.Join(dir, filename)); !os.IsNotExist(err) {
			t.Fatalf("%s still exists: %v", filename, err)
		}
	}
	var remaining int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE id IN ($1, $2)`, activeID, sealedID).Scan(&remaining); err != nil || remaining != 0 {
		t.Fatalf("remaining rows=%d err=%v", remaining, err)
	}
}

func TestGCPostgresRemoveFailureReturnsZeroUnitCredit(t *testing.T) {
	dbconn := newPhase11Postgres(t, 2)
	dir := t.TempDir()
	containerID := insertPhase11Container(t, dbconn, dir, "remove-fail.bin", []byte("remove-fail"), true)
	script := faultfs.NewScript(faultfs.Fault{Op: faultfs.OpRemove, Err: faultfs.ErrFaultRemove})
	result, err := runGCWithDBOptions(context.Background(), dbconn, false, dir, gcExecutionOptions{
		fs: faultfs.New(fsx.Default(), script),
	})
	if !errors.Is(err, faultfs.ErrFaultRemove) || result.AffectedContainers != 0 || len(result.ContainerFilenames) != 0 || result.BytesReclaimed != 0 {
		t.Fatalf("remove-failure result=%+v err=%v", result, err)
	}
	if script.CallCount(faultfs.OpStat) != 1 || script.CallCount(faultfs.OpRemove) != 1 {
		t.Fatalf("live fault boundary calls: Stat=%d Remove=%d, want 1/1", script.CallCount(faultfs.OpStat), script.CallCount(faultfs.OpRemove))
	}
	var remaining int
	if queryErr := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE id = $1`, containerID).Scan(&remaining); queryErr != nil || remaining != 0 {
		t.Fatalf("committed metadata remaining=%d err=%v", remaining, queryErr)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "remove-fail.bin")); statErr != nil {
		t.Fatalf("remove-failed file must remain: %v", statErr)
	}
}

type phase11PartialRemoveFS struct {
	fsx.FS
	failName string
	failErr  error
	cancel   context.CancelFunc
	atRemove chan string
	allReady <-chan struct{}
	release  <-chan struct{}
}

func (f *phase11PartialRemoveFS) Remove(name string) error {
	base := filepath.Base(name)
	f.atRemove <- base
	if base == f.failName {
		<-f.allReady
		f.cancel()
		return f.failErr
	}
	<-f.release
	return f.FS.Remove(name)
}

func TestGCPostgresPhysicalPartialResultsPreserveStartedSuccesses(t *testing.T) {
	dbconn := newPhase11Postgres(t, 4)
	dir := t.TempDir()
	payloads := map[string][]byte{
		"a-success.bin":   []byte("a-success"),
		"b-failure.bin":   []byte("b-failure"),
		"c-success.bin":   []byte("c-success-longer"),
		"d-unstarted.bin": []byte("d-unstarted"),
	}
	ids := map[string]int64{}
	for _, filename := range []string{"a-success.bin", "b-failure.bin", "c-success.bin", "d-unstarted.bin"} {
		ids[filename] = insertPhase11Container(t, dbconn, dir, filename, payloads[filename], true)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wantErr := errors.New("phase11 deterministic remove failure")
	allReady := make(chan struct{})
	release := make(chan struct{})
	fsys := &phase11PartialRemoveFS{
		FS:       fsx.Default(),
		failName: "b-failure.bin",
		failErr:  wantErr,
		cancel:   cancel,
		atRemove: make(chan string, 3),
		allReady: allReady,
		release:  release,
	}
	type runResult struct {
		result GCResult
		err    error
	}
	var observedMu sync.Mutex
	var observed []string
	done := make(chan runResult, 1)
	go func() {
		result, err := runGCWithDBOptions(ctx, dbconn, false, dir, gcExecutionOptions{
			fs:      fsys,
			workers: 3,
			dispatchObserver: func(unit gcDispatchUnit) {
				observedMu.Lock()
				observed = append(observed, unit.Filename)
				observedMu.Unlock()
			},
		})
		done <- runResult{result: result, err: err}
	}()

	reachedRemove := map[string]bool{}
	for i := 0; i < 3; i++ {
		select {
		case filename := <-fsys.atRemove:
			reachedRemove[filename] = true
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for three started units to reach post-commit Remove")
		}
	}
	for _, filename := range []string{"a-success.bin", "b-failure.bin", "c-success.bin"} {
		if !reachedRemove[filename] {
			t.Fatalf("started Remove population=%v, missing %s", reachedRemove, filename)
		}
	}
	close(allReady)
	<-ctx.Done()
	close(release)
	run := <-done
	if !errors.Is(run.err, wantErr) || !errors.Is(run.err, context.Canceled) {
		t.Fatalf("partial error=%v, want remove sentinel plus cancellation", run.err)
	}
	wantBytes := int64(len(payloads["a-success.bin"]) + len(payloads["c-success.bin"]))
	if run.result.AffectedContainers != 2 ||
		!reflect.DeepEqual(run.result.ContainerFilenames, []string{"a-success.bin", "c-success.bin"}) ||
		run.result.BytesReclaimed != wantBytes {
		t.Fatalf("physical partial result=%+v, want successful units one and three", run.result)
	}
	observedMu.Lock()
	observedCopy := append([]string(nil), observed...)
	observedMu.Unlock()
	for _, filename := range observedCopy {
		if filename == "d-unstarted.bin" {
			t.Fatalf("unit four started after stop condition: %v", observedCopy)
		}
	}
	if len(observedCopy) != 3 {
		t.Fatalf("observed started units=%v, want exactly three", observedCopy)
	}
	for _, filename := range []string{"a-success.bin", "c-success.bin"} {
		if _, err := os.Stat(filepath.Join(dir, filename)); !os.IsNotExist(err) {
			t.Fatalf("successful file %s remains: %v", filename, err)
		}
	}
	for _, filename := range []string{"b-failure.bin", "d-unstarted.bin"} {
		if _, err := os.Stat(filepath.Join(dir, filename)); err != nil {
			t.Fatalf("non-credited file %s missing: %v", filename, err)
		}
	}
	var failedRemaining, unstartedRemaining int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE id = $1`, ids["b-failure.bin"]).Scan(&failedRemaining); err != nil {
		t.Fatalf("count failed row: %v", err)
	}
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE id = $1`, ids["d-unstarted.bin"]).Scan(&unstartedRemaining); err != nil {
		t.Fatalf("count unstarted row: %v", err)
	}
	if failedRemaining != 0 || unstartedRemaining != 1 {
		t.Fatalf("failed row=%d unstarted row=%d, want 0/1", failedRemaining, unstartedRemaining)
	}
}
