package engine_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/engine"
)

func TestEngineConfigurationRoundTripsAndChangedSemantics(t *testing.T) {
	dbconn := openSnapshotTestDB(t)
	eng, err := engine.New(engine.Config{DB: dbconn})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	for _, tc := range []struct {
		key   engine.ConfigurationKey
		value string
		want  string
		level int64
	}{
		{engine.ConfigurationDefaultChunker, string(chunk.VersionV1SimpleRolling), string(chunk.VersionV1SimpleRolling), 0},
		{engine.ConfigurationCompression, " zstd ", "zstd", 0},
		{engine.ConfigurationCompressionLevel, "5", "5", 5},
	} {
		first, err := eng.SetConfiguration(context.Background(), engine.SetConfigurationRequest{Key: tc.key, Value: tc.value})
		if err != nil || !first.Changed || first.Value != tc.want {
			t.Fatalf("SetConfiguration(%s): got (%+v, %v)", tc.key, first, err)
		}
		second, err := eng.SetConfiguration(context.Background(), engine.SetConfigurationRequest{Key: tc.key, Value: tc.value})
		if err != nil || second.Changed {
			t.Fatalf("unchanged SetConfiguration(%s): got (%+v, %v)", tc.key, second, err)
		}
		got, err := eng.GetConfiguration(context.Background(), engine.GetConfigurationRequest{Key: tc.key})
		if err != nil || got.Value != tc.want {
			t.Fatalf("GetConfiguration(%s): got (%+v, %v)", tc.key, got, err)
		}
		if tc.level != 0 && (got.IntegerValue == nil || *got.IntegerValue != tc.level) {
			t.Fatalf("GetConfiguration(%s) integer projection: %+v", tc.key, got)
		}
	}
}

func TestEngineConfigurationDefaultsAndValidation(t *testing.T) {
	dbconn := openSnapshotTestDB(t)
	if _, err := dbconn.Exec(`DELETE FROM repository_config`); err != nil {
		t.Fatalf("delete repository configuration: %v", err)
	}
	eng, err := engine.New(engine.Config{DB: dbconn})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	for key, want := range map[engine.ConfigurationKey]string{
		engine.ConfigurationDefaultChunker:   string(chunk.DefaultChunkerVersion),
		engine.ConfigurationCompression:      "none",
		engine.ConfigurationCompressionLevel: "3",
	} {
		got, err := eng.GetConfiguration(context.Background(), engine.GetConfigurationRequest{Key: key})
		if err != nil || got.Value != want {
			t.Fatalf("default GetConfiguration(%s): got (%+v, %v), want %q", key, got, err, want)
		}
	}
	for _, req := range []engine.SetConfigurationRequest{
		{Key: "unknown", Value: "x"},
		{Key: engine.ConfigurationDefaultChunker, Value: "v9-future-cdc"},
		{Key: engine.ConfigurationCompression, Value: "xz"},
		{Key: engine.ConfigurationCompressionLevel, Value: "0"},
		{Key: engine.ConfigurationCompressionLevel, Value: "bad"},
	} {
		if _, err := eng.SetConfiguration(context.Background(), req); !engine.IsCode(err, engine.ErrorInvalidArgument) {
			t.Fatalf("invalid SetConfiguration(%+v): %v", req, err)
		}
	}
}

func TestEngineConfigurationDeprecationAndCancellation(t *testing.T) {
	dbconn := openSnapshotTestDB(t)
	eng, err := engine.New(engine.Config{
		DB: dbconn,
		ChunkerDeprecationPolicy: func(version chunk.Version) (bool, string) {
			return version == chunk.VersionV2FastCDC, "scheduled removal"
		},
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	_, err = eng.SetConfiguration(context.Background(), engine.SetConfigurationRequest{
		Key: engine.ConfigurationDefaultChunker, Value: string(chunk.VersionV2FastCDC),
	})
	if !engine.IsCode(err, engine.ErrorInvalidArgument) || !strings.Contains(err.Error(), "scheduled removal") {
		t.Fatalf("deprecated chunker: %v", err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = eng.GetConfiguration(cancelled, engine.GetConfigurationRequest{Key: engine.ConfigurationCompression})
	if !errors.Is(err, context.Canceled) || !engine.IsCode(err, engine.ErrorCancelled) {
		t.Fatalf("cancelled GetConfiguration: %v", err)
	}
}
