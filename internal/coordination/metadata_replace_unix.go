//go:build !windows

package coordination

import (
	"fmt"
	"os"
)

func replaceOwnerMetadata(tempPath, ownerPath string) error {
	if _, err := inspectOwnerMetadataDestination(ownerPath); err != nil {
		return err
	}
	if err := os.Rename(tempPath, ownerPath); err != nil {
		return fmt.Errorf("coordination: publish complete owner metadata: %w", err)
	}
	return nil
}
