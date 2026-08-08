package coordination

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestPrepareControlNamespaceCreatesMissingNamespace(t *testing.T) {
	containerDir := filepath.Join(t.TempDir(), "repository", "containers")

	prepared, err := PrepareControlNamespace(containerDir)
	if err != nil {
		t.Fatalf("PrepareControlNamespace: %v", err)
	}
	if prepared.Identity.CanonicalPath != containerDir {
		t.Fatalf("canonical path=%q want=%q", prepared.Identity.CanonicalPath, containerDir)
	}
	wantControlDirectory := filepath.Join(containerDir, ControlDirectoryName)
	if prepared.ControlDirectory != wantControlDirectory {
		t.Fatalf("control directory=%q want=%q", prepared.ControlDirectory, wantControlDirectory)
	}
	if prepared.LockArtifactPath != filepath.Join(wantControlDirectory, LockArtifactName) {
		t.Fatalf("lock artifact path=%q", prepared.LockArtifactPath)
	}
	if prepared.OwnerMetadataPath != filepath.Join(wantControlDirectory, OwnerMetadataName) {
		t.Fatalf("owner metadata path=%q", prepared.OwnerMetadataPath)
	}

	info, err := os.Lstat(wantControlDirectory)
	if err != nil {
		t.Fatalf("inspect control directory: %v", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("control path is not a real directory: mode=%v", info.Mode())
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o700 {
		t.Fatalf("new control directory mode=%#o want=0700", info.Mode().Perm())
	}
	assertNoCoordinationArtifacts(t, prepared)
}

func TestPrepareControlNamespaceUsesDistinctCreationModes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix-style permission bits are not authoritative on Windows")
	}

	root := t.TempDir()
	containerModeReference := filepath.Join(root, "container-mode-reference")
	if err := os.Mkdir(containerModeReference, 0o755); err != nil {
		t.Fatalf("create container mode reference: %v", err)
	}
	controlModeReference := filepath.Join(root, "control-mode-reference")
	if err := os.Mkdir(controlModeReference, 0o700); err != nil {
		t.Fatalf("create control mode reference: %v", err)
	}

	containerDir := filepath.Join(root, "missing-parent", "containers")
	prepared, err := PrepareControlNamespace(containerDir)
	if err != nil {
		t.Fatalf("PrepareControlNamespace: %v", err)
	}
	wantContainerMode := mustDirectoryMode(t, containerModeReference)
	for _, path := range []string{filepath.Dir(containerDir), containerDir} {
		if got := mustDirectoryMode(t, path); got != wantContainerMode {
			t.Fatalf("directory %q mode=%#o want container mode %#o", filepath.Base(path), got, wantContainerMode)
		}
	}
	if got, want := mustDirectoryMode(t, prepared.ControlDirectory), mustDirectoryMode(t, controlModeReference); got != want {
		t.Fatalf("control directory mode=%#o want control mode %#o", got, want)
	}
}

func TestPrepareControlNamespaceAcceptsExistingDirectoryAndIsIdempotent(t *testing.T) {
	containerDir := t.TempDir()
	controlDirectory := filepath.Join(containerDir, ControlDirectoryName)
	if err := os.Mkdir(controlDirectory, 0o750); err != nil {
		t.Fatalf("create control directory: %v", err)
	}
	before, err := os.Stat(controlDirectory)
	if err != nil {
		t.Fatalf("stat control directory before preparation: %v", err)
	}

	first, err := PrepareControlNamespace(containerDir)
	if err != nil {
		t.Fatalf("first PrepareControlNamespace: %v", err)
	}
	second, err := PrepareControlNamespace(containerDir)
	if err != nil {
		t.Fatalf("second PrepareControlNamespace: %v", err)
	}
	if first != second {
		t.Fatalf("idempotent preparation changed result: first=%+v second=%+v", first, second)
	}
	after, err := os.Stat(controlDirectory)
	if err != nil {
		t.Fatalf("stat control directory after preparation: %v", err)
	}
	if after.Mode().Perm() != before.Mode().Perm() {
		t.Fatalf("existing control directory mode changed from %#o to %#o", before.Mode().Perm(), after.Mode().Perm())
	}
	assertNoCoordinationArtifacts(t, second)
}

func TestPrepareControlNamespaceRejectsRegularFile(t *testing.T) {
	containerDir := t.TempDir()
	controlPath := filepath.Join(containerDir, ControlDirectoryName)
	if err := os.WriteFile(controlPath, []byte("unsafe"), 0o600); err != nil {
		t.Fatalf("create control file: %v", err)
	}

	_, err := PrepareControlNamespace(containerDir)
	if !errors.Is(err, ErrRepositoryIdentityInvalid) {
		t.Fatalf("error=%v, want invalid repository identity", err)
	}
}

func TestPrepareControlNamespaceRejectsSymlink(t *testing.T) {
	root := t.TempDir()
	containerDir := filepath.Join(root, "containers")
	target := filepath.Join(root, "outside")
	if err := os.MkdirAll(containerDir, 0o755); err != nil {
		t.Fatalf("create container directory: %v", err)
	}
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatalf("create symlink target: %v", err)
	}
	if err := os.Symlink(target, filepath.Join(containerDir, ControlDirectoryName)); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}

	_, err := PrepareControlNamespace(containerDir)
	if !errors.Is(err, ErrRepositoryIdentityInvalid) {
		t.Fatalf("error=%v, want invalid repository identity", err)
	}
	for _, artifact := range []string{LockArtifactName, OwnerMetadataName} {
		if _, err := os.Lstat(filepath.Join(target, artifact)); !os.IsNotExist(err) {
			t.Fatalf("unsafe target artifact %q exists, stat err=%v", artifact, err)
		}
	}
}

func TestPrepareControlNamespacePreservesExistingSymlinkPrefixIdentity(t *testing.T) {
	root := t.TempDir()
	realParent := filepath.Join(root, "real")
	if err := os.MkdirAll(realParent, 0o755); err != nil {
		t.Fatalf("create real parent: %v", err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(realParent, alias); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	configured := filepath.Join(alias, "repository", "containers")

	prepared, err := PrepareControlNamespace(configured)
	if err != nil {
		t.Fatalf("PrepareControlNamespace: %v", err)
	}
	wantCanonical := filepath.Join(realParent, "repository", "containers")
	if prepared.Identity.CanonicalPath != wantCanonical {
		t.Fatalf("canonical path=%q want=%q", prepared.Identity.CanonicalPath, wantCanonical)
	}
	if prepared.ControlDirectory != filepath.Join(wantCanonical, ControlDirectoryName) {
		t.Fatalf("control directory=%q", prepared.ControlDirectory)
	}
	assertNoCoordinationArtifacts(t, prepared)
}

func TestPrepareControlNamespaceRejectsIdentityChangeAfterCreation(t *testing.T) {
	initial := mustIdentity(t, filepath.Join(t.TempDir(), "containers"))
	final := mustIdentity(t, t.TempDir())
	calls := 0
	resolver := func(string) (Identity, error) {
		calls++
		if calls == 1 {
			return initial, nil
		}
		return final, nil
	}

	_, err := prepareControlNamespace("configured-container-directory", resolver)
	if !errors.Is(err, ErrRepositoryIdentityInvalid) {
		t.Fatalf("error=%v, want invalid repository identity", err)
	}
	if calls != 2 {
		t.Fatalf("identity resolver calls=%d want=2", calls)
	}
	if _, err := os.Stat(filepath.Join(initial.CanonicalPath, ControlDirectoryName)); err != nil {
		t.Fatalf("control directory was not prepared before final resolution: %v", err)
	}
}

func assertNoCoordinationArtifacts(t *testing.T, prepared PreparedControlNamespace) {
	t.Helper()
	for _, path := range []string{prepared.LockArtifactPath, prepared.OwnerMetadataPath} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("preparation created artifact %q, stat err=%v", filepath.Base(path), err)
		}
	}
}

func mustDirectoryMode(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat directory %q: %v", filepath.Base(path), err)
	}
	if !info.IsDir() {
		t.Fatalf("path %q is not a directory", filepath.Base(path))
	}
	return info.Mode().Perm()
}
