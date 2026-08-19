package engine_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
	filestate "github.com/franchoy/coldkeep/internal/status"
	"github.com/franchoy/coldkeep/internal/testutil/backendtest"
)

func TestEngineCurrentFilesAndConfigurationAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		seedEngineCurrentFile(t, backend.DB, 1901, "/phase19/report.txt", "phase19-report", 120, filestate.LogicalFileCompleted)
		seedEngineCurrentFile(t, backend.DB, 1902, "/phase19/notes.txt", "phase19-notes", 40, filestate.LogicalFileCompleted)
		seedEngineCurrentFile(t, backend.DB, 1903, "/phase19/aborted.txt", "phase19-aborted", 80, filestate.LogicalFileAborted)
		eng, err := engine.New(engine.Config{DB: backend.DB})
		if err != nil {
			t.Fatalf("engine.New: %v", err)
		}

		listed, err := eng.ListFiles(context.Background(), engine.ListFilesRequest{})
		if err != nil {
			t.Fatalf("ListFiles: %v", err)
		}
		if got, want := currentFileNames(listed.Files), []string{"/phase19/notes.txt", "/phase19/report.txt"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("ListFiles paths: got=%v want=%v", got, want)
		}

		searched, err := eng.SearchFiles(context.Background(), engine.SearchFilesRequest{
			NameContains: []string{"phase19", "report"},
			MinSizeBytes: []int64{100},
			MaxSizeBytes: []int64{200},
		})
		if err != nil || len(searched.Files) != 1 || searched.Files[0].Name != "/phase19/report.txt" {
			t.Fatalf("SearchFiles: got=(%+v, %v)", searched, err)
		}

		set, err := eng.SetConfiguration(context.Background(), engine.SetConfigurationRequest{
			Key: engine.ConfigurationCompression, Value: " zstd ",
		})
		if err != nil || !set.Changed || set.Value != "zstd" {
			t.Fatalf("SetConfiguration: got=(%+v, %v)", set, err)
		}
		unchanged, err := eng.SetConfiguration(context.Background(), engine.SetConfigurationRequest{
			Key: engine.ConfigurationCompression, Value: "zstd",
		})
		if err != nil || unchanged.Changed {
			t.Fatalf("unchanged SetConfiguration: got=(%+v, %v)", unchanged, err)
		}
		got, err := eng.GetConfiguration(context.Background(), engine.GetConfigurationRequest{Key: engine.ConfigurationCompression})
		if err != nil || got.Value != "zstd" {
			t.Fatalf("GetConfiguration: got=(%+v, %v)", got, err)
		}

		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		operations := []struct {
			name string
			run  func() error
		}{
			{"list", func() error {
				_, err := eng.ListFiles(cancelled, engine.ListFilesRequest{})
				return err
			}},
			{"search", func() error {
				_, err := eng.SearchFiles(cancelled, engine.SearchFilesRequest{NameContains: []string{"phase19"}})
				return err
			}},
			{"get configuration", func() error {
				_, err := eng.GetConfiguration(cancelled, engine.GetConfigurationRequest{Key: engine.ConfigurationCompression})
				return err
			}},
			{"set configuration", func() error {
				_, err := eng.SetConfiguration(cancelled, engine.SetConfigurationRequest{Key: engine.ConfigurationCompression, Value: "none"})
				return err
			}},
		}
		for _, operation := range operations {
			if err := operation.run(); !errors.Is(err, context.Canceled) || !engine.IsCode(err, engine.ErrorCancelled) {
				t.Errorf("cancelled %s: %v", operation.name, err)
			}
		}
	})
}
