//go:build linux && cgo

// Behavioral follow-up regressions, qualified against the submitted source.
package proofdaemon

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
)

func reviewP1PayloadState(t *testing.T, root string) map[string]string {
	t.Helper()
	state := make(map[string]string)
	must(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.IsDir() {
			state[rel] = fmt.Sprintf("directory:%o:%d", info.Mode().Perm(), info.Sys().(*syscall.Stat_t).Ino)
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("unexpected nonregular fixture member %q", rel)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		state[rel] = fmt.Sprintf("regular:%o:%d:%x", info.Mode().Perm(), info.Sys().(*syscall.Stat_t).Ino, sha256.Sum256(data))
		return nil
	}))
	return state
}

func reviewP1RestoreCase(t *testing.T, runtime *Runtime) RestoreResult {
	t.Helper()
	for _, entry := range runtime.entries {
		if entry.Name == "history/Case.txt" {
			return runtime.restore(entry.ID)
		}
	}
	t.Fatal("fixed historical Case.txt fixture entry was not admitted")
	return RestoreResult{}
}

func TestProofReviewRejectsRestoreRootTrimAlias(t *testing.T) {
	t.Setenv("COLDKEEP_KEY", "")
	for _, tc := range []struct {
		name   string
		suffix string
	}{
		{name: "ascii_space", suffix: " "},
		{name: "unicode_nonbreaking_space", suffix: "\u00a0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := fixture(t, t.TempDir())
			c.RestoreRoot = c.PayloadRoot + tc.suffix
			must(t, os.Mkdir(c.RestoreRoot, 0700))
			if strings.TrimSpace(c.RestoreRoot) != c.PayloadRoot {
				t.Fatal("counterexample setup did not produce the engine interpretation alias")
			}
			before := reviewP1PayloadState(t, c.PayloadRoot)
			beforeCatalog := reviewP1PayloadState(t, c.Catalog)

			runtime, openErr := Open(c)
			if openErr == nil {
				// Qualifying submitted-source observation: exercise the actual old
				// engine on disposable bytes, then fail for missing admission refusal.
				defer func() { must(t, runtime.Close()) }()
				result := reviewP1RestoreCase(t, runtime)
				actual, actualErr := os.ReadFile(filepath.Join(c.PayloadRoot, "history", "Case.txt"))
				configured, configuredErr := os.ReadDir(c.RestoreRoot)
				t.Logf("actual source-root path=%q; catalog before=%v after=%v", filepath.Join(c.PayloadRoot, "history", "Case.txt"), beforeCatalog, reviewP1PayloadState(t, c.Catalog))
				t.Fatalf("want ErrFixture before opening a trim-aliased restore root; Open returned nil; actual restore=%+v, source-root bytes=%q, source-read error=%v, configured-root entries=%d, configured-root read error=%v", result, actual, actualErr, len(configured), configuredErr)
			}
			if runtime != nil {
				must(t, runtime.Close())
			}
			if !errors.Is(openErr, ErrFixture) {
				t.Fatalf("want ErrFixture, got %v", openErr)
			}
			// Config validation is the pre-ownership/pre-catalog stage of Open.
			if err := c.validate(); !errors.Is(err, ErrFixture) {
				t.Fatalf("restore root must refuse during config validation, got %v", err)
			}
			if after := reviewP1PayloadState(t, c.PayloadRoot); !reflect.DeepEqual(before, after) {
				t.Fatal("payload/control directory changed despite admission refusal")
			}
			if !reflect.DeepEqual(beforeCatalog, reviewP1PayloadState(t, c.Catalog)) {
				t.Fatal("catalog changed after refusal")
			}
			t.Logf("PROOF_EVENT restore_alias_refused_preserved: root=%q payload/control/catalog identity, bytes and modes unchanged", c.RestoreRoot)
			entries, err := os.ReadDir(c.RestoreRoot)
			must(t, err)
			if len(entries) != 0 {
				t.Fatal("configured output changed despite admission refusal")
			}
		})
	}
}

func TestProofReviewRestoreRootValidControls(t *testing.T) {
	t.Setenv("COLDKEEP_KEY", "")
	for _, name := range []string{"ordinary_sibling", "internal_space"} {
		t.Run(name, func(t *testing.T) {
			c := fixture(t, t.TempDir())
			if name == "internal_space" {
				c.RestoreRoot = filepath.Join(filepath.Dir(c.RestoreRoot), "fixture output")
				must(t, os.Mkdir(c.RestoreRoot, 0700))
			}
			before := reviewP1PayloadState(t, c.PayloadRoot)
			runtime, err := Open(c)
			must(t, err)
			defer func() { must(t, runtime.Close()) }()
			if result := reviewP1RestoreCase(t, runtime); result.Outcome != "completed" {
				t.Fatalf("valid restore root was refused: %+v", result)
			}
			output := filepath.Join(c.RestoreRoot, "history", "Case.txt")
			data, err := os.ReadFile(output)
			must(t, err)
			if got := fmt.Sprintf("%x", sha256.Sum256(data)); got != "8950abfda7b727630760dd35bcf5c3daa7631aff223a90f7728c0d2521dde10c" {
				t.Fatalf("fixed historical content mismatch: %s", got)
			}
			info, err := os.Stat(output)
			must(t, err)
			if info.Mode().Perm() != 0640 || info.ModTime().Unix() != 1577934245 {
				t.Fatal("fixed Linux metadata mismatch")
			}
			if after := reviewP1PayloadState(t, c.PayloadRoot); !reflect.DeepEqual(before, after) {
				t.Fatal("source payload/control directory changed after valid destination restore")
			}
			t.Logf("PROOF_EVENT restore_valid_root_hash_metadata: output=%q sha256=%x mode=%o mtime=%d source payload/control preserved", output, sha256.Sum256(data), info.Mode().Perm(), info.ModTime().Unix())
		})
	}
}
