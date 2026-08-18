package coordination

import (
	"fmt"
	"os"
	"path/filepath"
)

// PreparedControlNamespace identifies the canonical repository coordination
// directory and the deterministic paths reserved for future lock artifacts.
// Preparation does not create either artifact.
type PreparedControlNamespace struct {
	Identity          Identity
	ControlDirectory  string
	LockArtifactPath  string
	OwnerMetadataPath string
}

// PrepareControlNamespace creates and validates the repository coordination
// directory, then verifies that filesystem creation did not change the
// canonical repository identity.
func PrepareControlNamespace(containerDir string) (PreparedControlNamespace, error) {
	return prepareControlNamespace(containerDir, ResolveIdentity)
}

func prepareControlNamespace(
	containerDir string,
	resolve func(string) (Identity, error),
) (PreparedControlNamespace, error) {
	identity, err := resolve(containerDir)
	if err != nil {
		return PreparedControlNamespace{}, err
	}
	if err := ValidateIdentity(identity); err != nil {
		return PreparedControlNamespace{}, err
	}
	controlDirectory, err := ensureControlDirectory(identity)
	if err != nil {
		return PreparedControlNamespace{}, err
	}

	finalIdentity, err := resolve(containerDir)
	if err != nil {
		return PreparedControlNamespace{}, err
	}
	if err := validateStableIdentity(identity, finalIdentity); err != nil {
		return PreparedControlNamespace{}, err
	}

	return PreparedControlNamespace{
		Identity:          finalIdentity,
		ControlDirectory:  controlDirectory,
		LockArtifactPath:  filepath.Join(controlDirectory, LockArtifactName),
		OwnerMetadataPath: filepath.Join(controlDirectory, OwnerMetadataName),
	}, nil
}

func ensureControlDirectory(identity Identity) (string, error) {
	if err := os.MkdirAll(identity.CanonicalPath, 0o755); err != nil {
		return "", fmt.Errorf("%w: create canonical container directory: %w", ErrRepositoryIdentityInvalid, err)
	}
	controlDirectory, err := ControlDirectory(identity)
	if err != nil {
		return "", err
	}
	if err := os.Mkdir(controlDirectory, 0o700); err != nil && !os.IsExist(err) {
		return "", fmt.Errorf("%w: create repository control directory: %w", ErrRepositoryIdentityInvalid, err)
	}
	info, err := os.Lstat(controlDirectory)
	if err != nil {
		return "", fmt.Errorf("%w: inspect repository control directory: %w", ErrRepositoryIdentityInvalid, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", fmt.Errorf("%w: repository control path must be a real directory", ErrRepositoryIdentityInvalid)
	}
	return controlDirectory, nil
}

func validateStableIdentity(initial, final Identity) error {
	if err := ValidateIdentity(final); err != nil {
		return err
	}
	if final != initial {
		return fmt.Errorf("%w: canonical container namespace changed during preparation", ErrRepositoryIdentityInvalid)
	}
	return nil
}
