package main

import (
	"context"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
)

const (
	phase9RestoreByIDUsage = "coldkeep restore <fileID> [<fileID> ...] <outputDir>\n" +
		"  [--input <file>] [--dry-run] [--overwrite] [--fail-fast]\n" +
		"  [--output <human|text|json>] [--json]"
	phase9RestoreStoredPathUsage = "coldkeep restore --stored-path <path>\n" +
		"  [--mode <original|prefix|override>] [--destination <path>]\n" +
		"  [--overwrite] [--strict|--no-metadata]\n" +
		"  [--output <human|text|json>] [--json]"
)

func TestRestoreHelpAdvertisesStoredPathContract(t *testing.T) {
	for _, flag := range []string{"--help", "-h"} {
		t.Run(flag, func(t *testing.T) {
			parsed, err := parseCommandLine([]string{"restore", flag}, flagsWithValues)
			if err != nil {
				t.Fatalf("parse restore help: %v", err)
			}
			if policy := repositoryCoordinationPolicyFor(parsed); policy.Required {
				t.Fatalf("restore %s unexpectedly requires repository coordination", flag)
			}

			stdout, stderr, code := runCLIWithCapturedIO(t, []string{"restore", flag})
			if code != exitSuccess {
				t.Fatalf("restore %s exit=%d stderr=%q", flag, code, stderr)
			}
			if strings.TrimSpace(stderr) != "" {
				t.Fatalf("restore %s wrote stderr: %q", flag, stderr)
			}
			for _, want := range []string{phase9RestoreByIDUsage, phase9RestoreStoredPathUsage} {
				if !strings.Contains(stdout, want) {
					t.Fatalf("restore %s help missing %q\noutput:\n%s", flag, want, stdout)
				}
			}
		})
	}
}

func TestRestoreStoredPathUsageMatchesAcceptedSyntax(t *testing.T) {
	t.Run("by ID", func(t *testing.T) {
		err := runRestoreCommand(parsedCommandLine{method: "restore", flags: map[string][]string{}}, outputModeText)
		if err == nil || !strings.Contains(err.Error(), phase9RestoreByIDUsage) {
			t.Fatalf("by-ID usage error = %v, want complete accepted syntax", err)
		}
	})

	t.Run("stored path", func(t *testing.T) {
		err := runRestoreCommand(parsedCommandLine{
			method:      "restore",
			positionals: []string{"unexpected"},
			flags:       map[string][]string{"stored-path": {"/docs/a.txt"}},
		}, outputModeText)
		if err == nil || !strings.Contains(err.Error(), phase9RestoreStoredPathUsage) {
			t.Fatalf("stored-path usage error = %v, want complete accepted syntax", err)
		}
	})
}

func TestRestoreByIDRejectsStoredPathOnlyFlags(t *testing.T) {
	for _, flag := range []string{"mode", "destination"} {
		t.Run(flag, func(t *testing.T) {
			err := runRestoreCommand(parsedCommandLine{
				method:      "restore",
				positionals: []string{"not-a-file-id", t.TempDir()},
				flags:       map[string][]string{flag: {"prefix"}},
			}, outputModeText)
			const want = "--mode and --destination are only supported with --stored-path"
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("by-ID restore with --%s error = %v, want %q", flag, err, want)
			}
		})
	}
}

func TestRestoreStoredPathRejectsBlankOptionValues(t *testing.T) {
	testCases := []struct {
		name  string
		flags map[string][]string
		want  string
	}{
		{
			name:  "stored path",
			flags: map[string][]string{"stored-path": {" "}},
			want:  "--stored-path cannot be empty",
		},
		{
			name: "mode",
			flags: map[string][]string{
				"stored-path": {"/docs/a.txt"},
				"mode":        {" "},
				"destination": {"/tmp/a.txt"},
			},
			want: "--mode cannot be empty",
		},
		{
			name: "destination",
			flags: map[string][]string{
				"stored-path": {"/docs/a.txt"},
				"mode":        {"override"},
				"destination": {" "},
			},
			want: "--destination cannot be empty",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := runRestoreCommand(parsedCommandLine{
				method: "restore",
				flags:  tc.flags,
			}, outputModeText)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("blank option error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestRestoreStoredPathOutputAliasesUseExistingJSONProjection(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	installStoredPathCommandStubs(t, dbconn, stubCommandEngine{
		restoreStoredPathFunc: func(_ context.Context, req engine.RestoreStoredPathRequest) (engine.RestoreStoredPathResult, error) {
			return engine.RestoreStoredPathResult{
				StoredPath:      req.StoredPath,
				FileID:          42,
				DestinationMode: req.DestinationMode,
				DestinationPath: "/tmp/out/docs/routed.txt",
				RestoredHash:    "abc123",
			}, nil
		},
	}, nil, nil)

	for _, args := range [][]string{
		{"restore", "--stored-path", "/docs/routed.txt", "--mode", "prefix", "--destination", "/tmp/out", "--output", "json"},
		{"restore", "--stored-path", "/docs/routed.txt", "--mode", "prefix", "--destination", "/tmp/out", "--json"},
	} {
		name := args[len(args)-1]
		t.Run(name, func(t *testing.T) {
			parsed, err := parseCommandLine(args, flagsWithValues)
			if err != nil {
				t.Fatalf("parse stored-path restore: %v", err)
			}
			outputMode, err := resolveOutputMode(parsed)
			if err != nil {
				t.Fatalf("resolve stored-path output mode: %v", err)
			}
			if outputMode != outputModeJSON {
				t.Fatalf("output mode = %q, want JSON", outputMode)
			}

			output := captureStdout(t, func() {
				if err := runRestoreCommand(parsed, outputMode); err != nil {
					t.Fatalf("run stored-path restore: %v", err)
				}
			})
			assertStoredPathRestoreJSONParity(t, output)
		})
	}
}
