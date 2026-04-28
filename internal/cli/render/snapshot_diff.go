package render

import (
	"fmt"
	"io"

	"github.com/franchoy/coldkeep/internal/snapshot"
)

func RenderSnapshotDiffSummaryHuman(w io.Writer, baseID, targetID string, summary snapshot.SnapshotDiffSummary) error {
	if w == nil {
		return fmt.Errorf("render snapshot diff summary human: nil writer")
	}

	totalChanges := summary.Added + summary.Removed + summary.Modified
	if _, err := fmt.Fprintf(w, "Snapshot diff: %s -> %s\n\n", baseID, targetID); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Added:     %d files\n", summary.Added); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Removed:   %d files\n", summary.Removed); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Modified:  %d files\n", summary.Modified); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Total changes: %d\n", totalChanges); err != nil {
		return err
	}

	return nil
}

func RenderSnapshotDiffDetailedHuman(
	w io.Writer,
	baseID, targetID string,
	entries []snapshot.SnapshotDiffEntry,
	summary snapshot.SnapshotDiffSummary,
	matchedEntryCount, totalEntryCount int,
	durationMS int64,
	hint string,
) error {
	if w == nil {
		return fmt.Errorf("render snapshot diff detailed human: nil writer")
	}

	if _, err := fmt.Fprintln(w, "[SNAPSHOT DIFF]"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "\nBase:    %s\n", baseID); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Target:  %s\n\n", targetID); err != nil {
		return err
	}
	if len(entries) == 0 {
		if _, err := fmt.Fprintln(w, "(no changes)"); err != nil {
			return err
		}
	} else {
		for _, entry := range entries {
			prefix := "?"
			switch entry.Type {
			case snapshot.DiffAdded:
				prefix = "+"
			case snapshot.DiffRemoved:
				prefix = "-"
			case snapshot.DiffModified:
				prefix = "~"
			}
			if _, err := fmt.Fprintf(w, "%s %s\n", prefix, entry.Path); err != nil {
				return err
			}
		}
	}

	if _, err := fmt.Fprintln(w, "\nSummary:"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  entries (matched): %d\n", matchedEntryCount); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  entries (total): %d\n", totalEntryCount); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  added: %d\n", summary.Added); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  removed: %d\n", summary.Removed); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  modified: %d\n", summary.Modified); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  Duration: %dms\n", durationMS); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "  Hint: "+hint); err != nil {
		return err
	}

	return nil
}
