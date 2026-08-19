package coordination

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const (
	// ControlDirectoryName is deliberately a directory because startup recovery
	// treats non-directory entries in the container root as orphan containers.
	ControlDirectoryName = ".coldkeep-control"
	LockArtifactName     = "repository.lock"
	OwnerMetadataName    = "owner.json"
)

// Identity is the canonical same-host container namespace used by repository
// coordination. CanonicalPath remains internal; diagnostics must expose Hash.
type Identity struct {
	CanonicalPath string
	Hash          string
}

// ResolveIdentity derives a non-mutating identity from a container directory.
//
// Existing path components have symlinks resolved. Missing leaf components are
// appended to the resolved nearest existing ancestor; Phase 12 owns creation
// and final re-resolution of the control directory.
func ResolveIdentity(containerDir string) (Identity, error) {
	if strings.TrimSpace(containerDir) == "" || strings.ContainsRune(containerDir, '\x00') {
		return Identity{}, fmt.Errorf("%w: container directory is empty or contains NUL", ErrRepositoryIdentityInvalid)
	}
	if isUNCPath(containerDir) {
		return Identity{}, fmt.Errorf("%w: UNC and network paths are outside the local-filesystem contract", ErrRepositoryLockUnsupported)
	}

	absolute, err := filepath.Abs(filepath.Clean(containerDir))
	if err != nil {
		return Identity{}, fmt.Errorf("%w: make container directory absolute: %w", ErrRepositoryIdentityInvalid, err)
	}
	canonical, err := resolveExistingPathPrefix(absolute)
	if err != nil {
		return Identity{}, err
	}
	canonical = normalizePlatformPath(filepath.Clean(canonical))

	sum := sha256.Sum256([]byte(canonical))
	return Identity{
		CanonicalPath: canonical,
		Hash:          hex.EncodeToString(sum[:]),
	}, nil
}

// ValidateIdentity checks that an identity is internally consistent.
func ValidateIdentity(identity Identity) error {
	if strings.TrimSpace(identity.CanonicalPath) == "" || strings.ContainsRune(identity.CanonicalPath, '\x00') {
		return fmt.Errorf("%w: canonical path is empty or contains NUL", ErrRepositoryIdentityInvalid)
	}
	if !filepath.IsAbs(identity.CanonicalPath) {
		return fmt.Errorf("%w: canonical path must be absolute", ErrRepositoryIdentityInvalid)
	}
	sum := sha256.Sum256([]byte(identity.CanonicalPath))
	if identity.Hash != hex.EncodeToString(sum[:]) {
		return fmt.Errorf("%w: identity hash does not match canonical path", ErrRepositoryIdentityInvalid)
	}
	return nil
}

// ControlDirectory returns the fixed lock namespace without creating it.
func ControlDirectory(identity Identity) (string, error) {
	if err := ValidateIdentity(identity); err != nil {
		return "", err
	}
	return filepath.Join(identity.CanonicalPath, ControlDirectoryName), nil
}

func resolveExistingPathPrefix(absolute string) (string, error) {
	resolved, err := filepath.EvalSymlinks(absolute)
	if err == nil {
		return resolved, nil
	}
	if !os.IsNotExist(err) {
		return "", fmt.Errorf("%w: resolve container directory: %w", ErrRepositoryIdentityInvalid, err)
	}

	current := absolute
	missing := make([]string, 0, 4)
	for {
		if _, statErr := os.Lstat(current); statErr == nil {
			resolvedPrefix, evalErr := filepath.EvalSymlinks(current)
			if evalErr != nil {
				return "", fmt.Errorf("%w: resolve existing container ancestor: %w", ErrRepositoryIdentityInvalid, evalErr)
			}
			parts := append([]string{resolvedPrefix}, reverseStrings(missing)...)
			return filepath.Join(parts...), nil
		} else if !os.IsNotExist(statErr) {
			return "", fmt.Errorf("%w: inspect container ancestor: %w", ErrRepositoryIdentityInvalid, statErr)
		}

		parent := filepath.Dir(current)
		if parent == current {
			return "", fmt.Errorf("%w: no existing container ancestor", ErrRepositoryIdentityInvalid)
		}
		missing = append(missing, filepath.Base(current))
		current = parent
	}
}

func reverseStrings(values []string) []string {
	reversed := make([]string, len(values))
	for i := range values {
		reversed[len(values)-1-i] = values[i]
	}
	return reversed
}

func normalizePlatformPath(path string) string {
	if runtime.GOOS != "windows" {
		return path
	}
	volume := filepath.VolumeName(path)
	if len(volume) == 2 && volume[1] == ':' {
		return strings.ToUpper(volume[:1]) + path[1:]
	}
	return path
}

func isUNCPath(path string) bool {
	return strings.HasPrefix(path, `\\`) || strings.HasPrefix(path, "//")
}
