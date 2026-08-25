package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	testutils "github.com/franchoy/coldkeep/tests/utils"
)

func TestSchemaStartupOperatorMessagingReleaseGateUsesIsolatedAbsoluteStorage(t *testing.T) {
	t.Run("blank root panics", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("DefaultCLIEnv must panic for a blank storage root")
			}
		}()
		_ = testutils.DefaultCLIEnv(" \t\n ")
	})

	t.Run("relative root becomes absolute", func(t *testing.T) {
		workingDir := t.TempDir()
		oldWorkingDir, err := os.Getwd()
		if err != nil {
			t.Fatalf("get working directory: %v", err)
		}
		if err := os.Chdir(workingDir); err != nil {
			t.Fatalf("change working directory: %v", err)
		}
		t.Cleanup(func() {
			if err := os.Chdir(oldWorkingDir); err != nil {
				t.Errorf("restore working directory: %v", err)
			}
		})

		env := testutils.DefaultCLIEnv(filepath.Join("relative", "containers"))
		want := filepath.Join(workingDir, "relative", "containers")
		if got := env["COLDKEEP_STORAGE_DIR"]; got != want || !filepath.IsAbs(got) {
			t.Fatalf("relative storage root = %q, want absolute %q", got, want)
		}
	})

	t.Run("absolute root and parent environment isolation", func(t *testing.T) {
		t.Setenv("COLDKEEP_STORAGE_DIR", filepath.Join(t.TempDir(), "parent-override"))
		root := filepath.Join(t.TempDir(), "containers", "..", "containers")
		want, err := filepath.Abs(filepath.Clean(root))
		if err != nil {
			t.Fatalf("resolve expected absolute root: %v", err)
		}
		env := testutils.DefaultCLIEnv(root)
		if got := env["COLDKEEP_STORAGE_DIR"]; got != want {
			t.Fatalf("supplied storage root lost authority: got %q want %q", got, want)
		}
	})

	t.Run("subprocess alternate cwd cannot redirect root", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "containers")
		env := testutils.DefaultCLIEnv(root)
		cmd := exec.Command(os.Args[0], "-test.run=^TestPhase14StorageEnvProbe$")
		cmd.Dir = t.TempDir()
		cmd.Env = testutils.BuildCommandEnv(map[string]string{
			"COLDKEEP_PHASE14_STORAGE_ENV_PROBE": "1",
			"COLDKEEP_STORAGE_DIR":               env["COLDKEEP_STORAGE_DIR"],
			"COLDKEEP_TEST_DB":                   "",
		})
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("run alternate-cwd storage probe: %v\n%s", err, output)
		}
		if !strings.Contains(string(output), env["COLDKEEP_STORAGE_DIR"]) {
			t.Fatalf("probe output %q does not contain absolute root %q", output, env["COLDKEEP_STORAGE_DIR"])
		}
	})

	t.Run("schema subtest roots are unique", func(t *testing.T) {
		first := testutils.DefaultCLIEnv(filepath.Join(t.TempDir(), "containers"))["COLDKEEP_STORAGE_DIR"]
		second := testutils.DefaultCLIEnv(filepath.Join(t.TempDir(), "containers"))["COLDKEEP_STORAGE_DIR"]
		if first == second {
			t.Fatalf("independent schema subtests shared storage root %q", first)
		}
	})

	t.Run("temporary root cleanup owns generated state", func(t *testing.T) {
		var root string
		if ok := t.Run("owner", func(t *testing.T) {
			root = filepath.Join(t.TempDir(), "containers")
			if err := os.MkdirAll(filepath.Join(root, ".coldkeep-control"), 0o700); err != nil {
				t.Fatalf("create generated control state: %v", err)
			}
		}); !ok {
			t.Fatal("temporary-root owner subtest failed")
		}
		if _, err := os.Stat(root); !os.IsNotExist(err) {
			t.Fatalf("temporary storage root survived owner cleanup: stat error=%v", err)
		}
	})
}

func TestPhase14StorageEnvProbe(t *testing.T) {
	if os.Getenv("COLDKEEP_PHASE14_STORAGE_ENV_PROBE") != "1" {
		t.Skip("Phase 14 helper subprocess only")
	}
	root := os.Getenv("COLDKEEP_STORAGE_DIR")
	if !filepath.IsAbs(root) {
		t.Fatalf("subprocess storage root is not absolute: %q", root)
	}
	fmt.Print(root)
}
