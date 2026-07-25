package coordination

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveIdentityNormalizesAliasesAndTrailingSeparators(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "repository", "containers")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatalf("create target: %v", err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(filepath.Join(root, "repository"), alias); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}

	direct := mustIdentity(t, target)
	throughAlias := mustIdentity(t, filepath.Join(alias, "containers")+string(filepath.Separator))
	if direct != throughAlias {
		t.Fatalf("alias identity mismatch direct=%+v alias=%+v", direct, throughAlias)
	}
	if !filepath.IsAbs(direct.CanonicalPath) {
		t.Fatalf("canonical path is not absolute: %q", direct.CanonicalPath)
	}
}

func TestResolveIdentityNormalizesRelativeAndAbsoluteForms(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "repository", "containers")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatalf("create target: %v", err)
	}
	previous, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	if err := os.Chdir(root); err != nil {
		t.Fatalf("change working directory: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(previous); err != nil {
			t.Errorf("restore working directory: %v", err)
		}
	})

	relative := mustIdentity(t, filepath.Join("repository", "containers"))
	absolute := mustIdentity(t, target)
	if relative != absolute {
		t.Fatalf("relative identity=%+v absolute identity=%+v", relative, absolute)
	}
}

func TestResolveIdentityUsesResolvedNearestExistingAncestor(t *testing.T) {
	root := t.TempDir()
	realParent := filepath.Join(root, "real")
	if err := os.MkdirAll(realParent, 0o755); err != nil {
		t.Fatalf("create real parent: %v", err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(realParent, alias); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}

	identity := mustIdentity(t, filepath.Join(alias, "missing", "containers"))
	want := filepath.Join(realParent, "missing", "containers")
	if identity.CanonicalPath != want {
		t.Fatalf("canonical path=%q want=%q", identity.CanonicalPath, want)
	}
	if _, err := os.Stat(filepath.Join(realParent, "missing")); !os.IsNotExist(err) {
		t.Fatalf("identity resolution must not create missing directories, stat err=%v", err)
	}
}

func TestResolveIdentityRejectsInvalidAndNetworkPaths(t *testing.T) {
	for _, path := range []string{"", " \t ", "bad\x00path"} {
		_, err := ResolveIdentity(path)
		if !errors.Is(err, ErrRepositoryIdentityInvalid) {
			t.Fatalf("ResolveIdentity(%q) error=%v, want invalid identity", path, err)
		}
	}

	for _, path := range []string{"//server/share", `\\server\share`} {
		_, err := ResolveIdentity(path)
		if !errors.Is(err, ErrRepositoryLockUnsupported) {
			t.Fatalf("ResolveIdentity(%q) error=%v, want unsupported", path, err)
		}
	}
}

func TestIdentityResolutionPreservesUnderlyingFilesystemCause(t *testing.T) {
	root := t.TempDir()
	alias := filepath.Join(root, "broken-alias")
	if err := os.Symlink(filepath.Join(root, "missing-target"), alias); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}

	_, err := ResolveIdentity(filepath.Join(alias, "containers"))
	if !errors.Is(err, ErrRepositoryIdentityInvalid) {
		t.Fatalf("expected invalid identity classification, got %v", err)
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expected underlying filesystem cause, got %v", err)
	}
}

func TestIdentityHashIsStableAndPathFree(t *testing.T) {
	identity := mustIdentity(t, t.TempDir())
	again := mustIdentity(t, identity.CanonicalPath)
	if identity.Hash != again.Hash {
		t.Fatalf("identity hash changed: %q != %q", identity.Hash, again.Hash)
	}
	if len(identity.Hash) != sha256HexLength || !isLowerHex(identity.Hash) {
		t.Fatalf("identity hash is not lowercase SHA-256: %q", identity.Hash)
	}
	if strings.Contains(identity.Hash, identity.CanonicalPath) {
		t.Fatal("identity hash exposed canonical path")
	}
}

func TestResolveIdentitySeparatesDistinctRepositories(t *testing.T) {
	first := mustIdentity(t, t.TempDir())
	second := mustIdentity(t, t.TempDir())
	if first == second || first.Hash == second.Hash {
		t.Fatalf("distinct repositories shared an identity: first=%+v second=%+v", first, second)
	}
}

func TestCoordinationControlDirectoryUsesRecoverySafeSubdirectory(t *testing.T) {
	identity := mustIdentity(t, t.TempDir())
	controlDir, err := ControlDirectory(identity)
	if err != nil {
		t.Fatalf("ControlDirectory: %v", err)
	}
	if filepath.Dir(controlDir) != identity.CanonicalPath || filepath.Base(controlDir) != ControlDirectoryName {
		t.Fatalf("unexpected control directory %q", controlDir)
	}
	if _, err := os.Stat(controlDir); !os.IsNotExist(err) {
		t.Fatalf("contract helper must not create control directory, stat err=%v", err)
	}
	if strings.ContainsAny(ControlDirectoryName+LockArtifactName+OwnerMetadataName, `:*?"<>|`) {
		t.Fatal("coordination artifact names are not Windows-safe")
	}
}

func TestValidateIdentityRejectsTampering(t *testing.T) {
	identity := mustIdentity(t, t.TempDir())
	identity.Hash = strings.Repeat("0", sha256HexLength)
	if err := ValidateIdentity(identity); !errors.Is(err, ErrRepositoryIdentityInvalid) {
		t.Fatalf("expected invalid identity, got %v", err)
	}
}
