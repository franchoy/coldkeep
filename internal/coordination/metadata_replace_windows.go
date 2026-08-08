//go:build windows

package coordination

import (
	"fmt"
	"os"
)

func replaceOwnerMetadata(tempPath, ownerPath string) error {
	exists, err := inspectOwnerMetadataDestination(ownerPath)
	if err != nil {
		return err
	}
	if exists {
		if err := os.Remove(ownerPath); err != nil {
			return fmt.Errorf("coordination: remove existing owner metadata before publication: %w", err)
		}
	}
	if err := os.Rename(tempPath, ownerPath); err != nil {
		return fmt.Errorf("coordination: publish complete owner metadata: %w", err)
	}
	return nil
}
