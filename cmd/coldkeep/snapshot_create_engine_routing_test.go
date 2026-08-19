package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/snapshot"
	"github.com/franchoy/coldkeep/internal/storage"
)

type failingReader struct{}

func (failingReader) Read(_ []byte) (int, error) {
	return 0, errors.New("unexpected entropy read")
}

type trackingQueryDriver struct {
	queryCount *int32
}

var trackingQueryDriverSequence uint64

type trackingQueryConn struct {
	queryCount *int32
}

func (d trackingQueryDriver) Open(string) (driver.Conn, error) {
	return trackingQueryConn(d), nil
}

func (c trackingQueryConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare not supported")
}

func (c trackingQueryConn) Close() error {
	return nil
}

func (c trackingQueryConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions not supported")
}

func (c trackingQueryConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	atomic.AddInt32(c.queryCount, 1)
	return nil, errors.New("unexpected query")
}

func openTrackingQueryDB(t *testing.T, queryCount *int32) *sql.DB {
	t.Helper()

	dbconn, err := newTrackingQueryDB(queryCount)
	if err != nil {
		t.Fatalf("sql.Open tracking driver: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	return dbconn
}

func newTrackingQueryDB(queryCount *int32) (*sql.DB, error) {
	// database/sql registrations are process-global. The driver retains the
	// caller's counter pointer, so every fixture needs its own driver instance.
	driverName := fmt.Sprintf(
		"snapshot-create-routing-driver-%d",
		atomic.AddUint64(&trackingQueryDriverSequence, 1),
	)
	sql.Register(driverName, trackingQueryDriver{queryCount: queryCount})
	return sql.Open(driverName, "")
}

func TestTrackingQueryDBFixtureIsRepeatableAndIsolated(t *testing.T) {
	var firstCount, secondCount int32
	first := openTrackingQueryDB(t, &firstCount)
	_ = openTrackingQueryDB(t, &secondCount)

	if _, err := first.QueryContext(context.Background(), "SELECT legacy_count"); err == nil {
		t.Fatal("expected tracking driver query to fail")
	}
	if got := atomic.LoadInt32(&firstCount); got != 1 {
		t.Fatalf("expected first fixture to observe one query, got %d", got)
	}
	if got := atomic.LoadInt32(&secondCount); got != 0 {
		t.Fatalf("expected second fixture to remain isolated, got %d queries", got)
	}
}

func TestTrackingQueryDBFixtureParallelCreation(t *testing.T) {
	const fixtureCount = 16
	errs := make(chan error, fixtureCount)
	for range fixtureCount {
		go func() {
			var queryCount int32
			dbconn, err := newTrackingQueryDB(&queryCount)
			if err != nil {
				errs <- fmt.Errorf("create fixture: %w", err)
				return
			}
			defer func() { _ = dbconn.Close() }()
			if _, err := dbconn.QueryContext(context.Background(), "SELECT legacy_count"); err == nil {
				errs <- errors.New("expected tracking driver query to fail")
				return
			}
			if got := atomic.LoadInt32(&queryCount); got != 1 {
				errs <- fmt.Errorf("expected isolated fixture query count 1, got %d", got)
				return
			}
			errs <- nil
		}()
	}
	for range fixtureCount {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

func TestRunSnapshotCommandCreateOmitsIDForEngineGeneration(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalCreate := createSnapshotPhase
	originalEngine := newCommandEngine
	originalReader := rand.Reader
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		createSnapshotPhase = originalCreate
		newCommandEngine = originalEngine
		rand.Reader = originalReader
	})

	rand.Reader = failingReader{}
	dbconn := openSnapshotRoutingDB(t)
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{DB: dbconn}, nil
	}
	createSnapshotPhase = func(_ context.Context, _ *sql.DB, _ snapshot.SnapshotCreateOptions) error {
		t.Fatal("expected routed snapshot create to avoid createSnapshotPhase")
		return nil
	}

	newCommandEngine = func(_ *sql.DB, _ string) (engine.Engine, error) {
		return stubCommandEngine{
			snapshotCreateFunc: func(_ context.Context, req engine.SnapshotCreateRequest) (engine.SnapshotCreateResult, error) {
				if req.ID != "" {
					t.Fatalf("expected omitted --id to forward empty ID, got %q", req.ID)
				}
				if req.Label != "generated-label" {
					t.Fatalf("expected label forwarding, got %q", req.Label)
				}
				if len(req.Paths) != 2 || req.Paths[0] != "docs/" || req.Paths[1] != "docs/" {
					t.Fatalf("expected raw paths preserved, got %v", req.Paths)
				}
				return engine.SnapshotCreateResult{
					SnapshotID:    "snap-engine-generated",
					Type:          engine.SnapshotTypePartial,
					PathsCount:    2,
					FilesInserted: 4,
					Label:         req.Label,
				}, nil
			},
		}, nil
	}

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"create", "docs/", "docs/"},
			flags: map[string][]string{
				"label":  {"generated-label"},
				"output": {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runSnapshotCommand create: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse snapshot create JSON: %v output=%q", err, output)
	}
	data := payload["data"].(map[string]any)
	if got := data["snapshot_id"]; got != "snap-engine-generated" {
		t.Fatalf("expected engine-generated snapshot ID in output, got %v", got)
	}
	if got := int(data["files_inserted"].(float64)); got != 4 {
		t.Fatalf("expected files_inserted=4, got %d", got)
	}
}

func TestRunSnapshotCommandCreateUsesEngineResultProjection(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalEngine := newCommandEngine
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		newCommandEngine = originalEngine
	})

	dbconn := openSnapshotRoutingDB(t)
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{DB: dbconn}, nil
	}
	newCommandEngine = func(_ *sql.DB, _ string) (engine.Engine, error) {
		return stubCommandEngine{
			snapshotCreateFunc: func(_ context.Context, req engine.SnapshotCreateRequest) (engine.SnapshotCreateResult, error) {
				return engine.SnapshotCreateResult{
					SnapshotID:    "snap-parented",
					Type:          engine.SnapshotTypeFull,
					PathsCount:    7,
					FilesInserted: 9,
					Label:         req.Label,
					ParentID:      req.ParentID,
				}, nil
			},
		}, nil
	}

	jsonOutput := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"create"},
			flags: map[string][]string{
				"id":     {"snap-explicit"},
				"label":  {"release"},
				"from":   {"snap-base"},
				"output": {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runSnapshotCommand JSON create: %v", err)
		}
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(jsonOutput)), &payload); err != nil {
		t.Fatalf("parse snapshot create JSON: %v output=%q", err, jsonOutput)
	}
	data := payload["data"].(map[string]any)
	if got := int(data["paths_count"].(float64)); got != 7 {
		t.Fatalf("expected engine PathsCount=7 in JSON, got %d", got)
	}
	if got := int(data["files_inserted"].(float64)); got != 9 {
		t.Fatalf("expected engine FilesInserted=9 in JSON, got %d", got)
	}
	if got := data["parent_id"]; got != "snap-base" {
		t.Fatalf("expected engine parent_id in JSON, got %v", got)
	}

	textOutput := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method:      "snapshot",
			positionals: []string{"create"},
			flags: map[string][]string{
				"id":   {"snap-explicit"},
				"from": {"snap-base"},
			},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runSnapshotCommand text create: %v", err)
		}
	})
	for _, want := range []string{
		`Snapshot "snap-parented" created from parent "snap-base"`,
		"  Files: 9",
		"  Duration: ",
		"  Hint: " + doctorOperationalHint,
	} {
		if !strings.Contains(textOutput, want) {
			t.Fatalf("expected text output to contain %q, got:\n%s", want, textOutput)
		}
	}
}

func TestRunSnapshotCommandCreateRejectsBlankFlagsBeforeInitialization(t *testing.T) {
	tests := []struct {
		name    string
		flags   map[string][]string
		wantErr string
	}{
		{name: "id", flags: map[string][]string{"id": {"   "}}, wantErr: "--id cannot be empty"},
		{name: "label", flags: map[string][]string{"label": {"   "}}, wantErr: "--label cannot be empty"},
		{name: "from", flags: map[string][]string{"from": {"   "}}, wantErr: "--from cannot be empty"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			originalLoad := loadDefaultStorageContextPhase
			originalEngine := newCommandEngine
			t.Cleanup(func() {
				loadDefaultStorageContextPhase = originalLoad
				newCommandEngine = originalEngine
			})

			loadCalled := false
			engineCalled := false
			loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
				loadCalled = true
				return storage.StorageContext{}, errors.New("unexpected storage initialization")
			}
			newCommandEngine = func(_ *sql.DB, _ string) (engine.Engine, error) {
				engineCalled = true
				return stubCommandEngine{}, nil
			}

			err := runSnapshotCommand(parsedCommandLine{
				method:      "snapshot",
				positionals: []string{"create"},
				flags:       tc.flags,
			}, outputModeText)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected %q, got %v", tc.wantErr, err)
			}
			if loadCalled {
				t.Fatal("expected blank-flag validation before repository initialization")
			}
			if engineCalled {
				t.Fatal("expected blank-flag validation before engine initialization")
			}
		})
	}
}

func TestRunSnapshotCommandCreateSkipsLegacyCountQuery(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalCreate := createSnapshotPhase
	originalEngine := newCommandEngine
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		createSnapshotPhase = originalCreate
		newCommandEngine = originalEngine
	})

	var queryCount int32
	dbconn := openTrackingQueryDB(t, &queryCount)
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{DB: dbconn}, nil
	}
	createSnapshotPhase = func(_ context.Context, _ *sql.DB, _ snapshot.SnapshotCreateOptions) error {
		t.Fatal("expected routed snapshot create to avoid createSnapshotPhase")
		return nil
	}
	newCommandEngine = func(_ *sql.DB, _ string) (engine.Engine, error) {
		return stubCommandEngine{
			snapshotCreateFunc: func(_ context.Context, req engine.SnapshotCreateRequest) (engine.SnapshotCreateResult, error) {
				return engine.SnapshotCreateResult{
					SnapshotID:    "snap-no-query",
					Type:          engine.SnapshotTypeFull,
					PathsCount:    len(req.Paths),
					FilesInserted: 3,
				}, nil
			},
		}, nil
	}

	if err := runSnapshotCommand(parsedCommandLine{
		method:      "snapshot",
		positionals: []string{"create"},
		flags: map[string][]string{
			"id": {"snap-explicit"},
		},
	}, outputModeText); err != nil {
		t.Fatalf("runSnapshotCommand create: %v", err)
	}

	if got := atomic.LoadInt32(&queryCount); got != 0 {
		t.Fatalf("expected no legacy snapshot_file count query, got %d query calls", got)
	}
}
