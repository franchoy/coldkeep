package main

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/storage"
)

func TestRunConfigGetUsesEngineJSONParity(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	level := int64(5)
	installConfigurationCommandStubs(t, dbconn, stubCommandEngine{
		getConfigurationFunc: func(_ context.Context, req engine.GetConfigurationRequest) (engine.GetConfigurationResult, error) {
			if req.Key != engine.ConfigurationCompressionLevel {
				t.Fatalf("unexpected configuration key: %s", req.Key)
			}
			return engine.GetConfigurationResult{Key: req.Key, Value: "5", IntegerValue: &level}, nil
		},
	})

	output := captureStdout(t, func() {
		if err := runConfigCommand(parsedCommandLine{
			method: "config", positionals: []string{"get", "compression-level"},
		}, outputModeJSON); err != nil {
			t.Fatalf("runConfigCommand: %v", err)
		}
	})
	if strings.TrimSpace(output) != `{"command":"config get","data":{"key":"compression-level","value":5},"status":"ok"}` {
		t.Fatalf("unexpected config get JSON: %s", output)
	}
}

func TestRunConfigSetUsesEngineAndChangedProjection(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	installConfigurationCommandStubs(t, dbconn, stubCommandEngine{
		setConfigurationFunc: func(_ context.Context, req engine.SetConfigurationRequest) (engine.SetConfigurationResult, error) {
			if req.Key != engine.ConfigurationCompression || req.Value != " zstd " {
				t.Fatalf("unexpected SetConfiguration request: %+v", req)
			}
			return engine.SetConfigurationResult{Key: req.Key, Value: "zstd", Changed: true}, nil
		},
	})

	output := captureStdout(t, func() {
		if err := runConfigCommand(parsedCommandLine{
			method: "config", positionals: []string{"set", "compression", " zstd "},
		}, outputModeText); err != nil {
			t.Fatalf("runConfigCommand: %v", err)
		}
	})
	for _, want := range []string{"compression set to zstd", "This affects only NEW blocks", "Blocks remain readable"} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected config set output to contain %q, got:\n%s", want, output)
		}
	}
}

func TestRunConfigSetMapsEngineValidationToUsage(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	installConfigurationCommandStubs(t, dbconn, stubCommandEngine{
		setConfigurationFunc: func(context.Context, engine.SetConfigurationRequest) (engine.SetConfigurationResult, error) {
			return engine.SetConfigurationResult{}, engine.NewError(
				engine.ErrorInvalidArgument, "set configuration",
				"invalid compression codec \"xz\", must be 'none' or 'zstd'", "", nil,
			)
		},
	})
	err := runConfigCommand(parsedCommandLine{
		method: "config", positionals: []string{"set", "compression", "xz"},
	}, outputModeText)
	if err == nil || classifyExitCode(err) != exitUsage || !strings.Contains(err.Error(), "invalid compression codec") {
		t.Fatalf("expected usage-class engine validation, got %v", err)
	}
}

func installConfigurationCommandStubs(t *testing.T, dbconn *sql.DB, stub stubCommandEngine) {
	t.Helper()
	originalLoad := loadDefaultStorageContextPhase
	originalNewEngine := newConfigurationCommandEngine
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		newConfigurationCommandEngine = originalNewEngine
	})
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{DB: dbconn}, nil
	}
	newConfigurationCommandEngine = func(storage.StorageContext) (engine.Engine, error) {
		return stub, nil
	}
}
