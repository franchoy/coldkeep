package coordination

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	ownerMetadataTempPattern = ".owner-*.tmp"
	maxOwnerMetadataSize     = 64 * 1024
)

// publishOwnerMetadata publishes diagnostic owner metadata as a complete,
// validated record. The metadata is not repository ownership proof.
func publishOwnerMetadata(prepared PreparedControlNamespace, owner Owner) (err error) {
	if err := validatePreparedControlNamespace(prepared); err != nil {
		return err
	}
	data, err := EncodeOwner(owner)
	if err != nil {
		return err
	}
	if owner.IdentityHash != prepared.Identity.Hash {
		return fmt.Errorf("coordination: owner identity does not match prepared control namespace")
	}

	temp, err := os.CreateTemp(prepared.ControlDirectory, ownerMetadataTempPattern)
	if err != nil {
		return fmt.Errorf("coordination: create owner metadata temporary file: %w", err)
	}
	tempPath := temp.Name()
	removeTemp := true
	defer func() {
		if temp != nil {
			if closeErr := temp.Close(); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("coordination: close owner metadata temporary file: %w", closeErr))
			}
		}
		if removeTemp {
			if removeErr := os.Remove(tempPath); removeErr != nil && !os.IsNotExist(removeErr) {
				err = errors.Join(err, fmt.Errorf("coordination: remove owner metadata temporary file: %w", removeErr))
			}
		}
	}()

	closed, err := writeSyncedOwnerMetadataTemp(temp, data)
	if closed {
		temp = nil
	}
	if err != nil {
		return err
	}

	if err := replaceOwnerMetadata(tempPath, prepared.OwnerMetadataPath); err != nil {
		return err
	}
	removeTemp = false
	return nil
}

func writeSyncedOwnerMetadataTemp(temp *os.File, data []byte) (bool, error) {
	written, err := temp.Write(data)
	if err != nil {
		return false, fmt.Errorf("coordination: write owner metadata temporary file: %w", err)
	}
	if written != len(data) {
		return false, fmt.Errorf("coordination: write owner metadata temporary file: %w", io.ErrShortWrite)
	}
	if err := temp.Sync(); err != nil {
		return false, fmt.Errorf("coordination: sync owner metadata temporary file: %w", err)
	}
	if err := temp.Close(); err != nil {
		return true, fmt.Errorf("coordination: close owner metadata temporary file: %w", err)
	}
	return true, nil
}

// readOwnerMetadata reads non-authoritative diagnostics. Missing, malformed,
// and unsupported metadata remain ordinary diagnostic errors, never Busy.
func readOwnerMetadata(prepared PreparedControlNamespace) (Owner, error) {
	if err := validatePreparedControlNamespace(prepared); err != nil {
		return Owner{}, err
	}
	data, err := readBoundedRegularOwnerMetadata(prepared.OwnerMetadataPath)
	if err != nil {
		return Owner{}, err
	}
	return DecodeOwner(data)
}

func readBoundedRegularOwnerMetadata(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("coordination: inspect owner metadata: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("coordination: owner metadata must be a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("coordination: open owner metadata: %w", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, int64(maxOwnerMetadataSize)+1))
	closeErr := file.Close()
	if readErr != nil {
		return nil, fmt.Errorf("coordination: read owner metadata: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("coordination: close owner metadata: %w", closeErr)
	}
	if len(data) > maxOwnerMetadataSize {
		return nil, fmt.Errorf("coordination: owner metadata exceeds maximum size of %d bytes", maxOwnerMetadataSize)
	}
	return data, nil
}

func inspectOwnerMetadataDestination(path string) (bool, error) {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("coordination: inspect existing owner metadata: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return false, fmt.Errorf("coordination: existing owner metadata must be a regular file")
	}
	return true, nil
}

// removeOwnerMetadata removes only the diagnostic owner record. Absence is a
// successful idempotent outcome; the persistent lock artifact is untouched.
func removeOwnerMetadata(prepared PreparedControlNamespace) error {
	if err := validatePreparedControlNamespace(prepared); err != nil {
		return err
	}
	info, err := os.Lstat(prepared.OwnerMetadataPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("coordination: inspect owner metadata for removal: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("coordination: owner metadata must be a regular file")
	}
	if err := os.Remove(prepared.OwnerMetadataPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("coordination: remove owner metadata: %w", err)
	}
	return nil
}

func validatePreparedControlNamespace(prepared PreparedControlNamespace) error {
	if err := ValidateIdentity(prepared.Identity); err != nil {
		return err
	}
	controlDirectory, err := ControlDirectory(prepared.Identity)
	if err != nil {
		return err
	}
	if !preparedNamespacePathsMatch(prepared, controlDirectory) {
		return fmt.Errorf("%w: prepared control namespace paths do not match identity", ErrRepositoryIdentityInvalid)
	}
	return validateRealControlDirectory(controlDirectory)
}

func preparedNamespacePathsMatch(prepared PreparedControlNamespace, controlDirectory string) bool {
	return prepared.ControlDirectory == controlDirectory &&
		prepared.LockArtifactPath == filepath.Join(controlDirectory, LockArtifactName) &&
		prepared.OwnerMetadataPath == filepath.Join(controlDirectory, OwnerMetadataName)
}

func validateRealControlDirectory(controlDirectory string) error {
	info, err := os.Lstat(controlDirectory)
	if err != nil {
		return fmt.Errorf("%w: inspect prepared control directory: %w", ErrRepositoryIdentityInvalid, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("%w: prepared control path must be a real directory", ErrRepositoryIdentityInvalid)
	}
	return nil
}
