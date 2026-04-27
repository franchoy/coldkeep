package render

import (
	"fmt"
	"io"
	"time"

	"github.com/franchoy/coldkeep/internal/snapshot"
)

func RenderSnapshotListHuman(
	w io.Writer,
	items []snapshot.Snapshot,
	treeMode bool,
	treeLines []string,
	durationMS int64,
	hint string,
) error {
	if w == nil {
		return fmt.Errorf("render snapshot list human: nil writer")
	}

	if treeMode {
		if len(treeLines) == 0 {
			if _, err := fmt.Fprintln(w, "no snapshots found"); err != nil {
				return err
			}
		} else {
			for _, line := range treeLines {
				if _, err := fmt.Fprintln(w, line); err != nil {
					return err
				}
			}
		}
	} else {
		if _, err := fmt.Fprintln(w, "Snapshots:"); err != nil {
			return err
		}
		if len(items) == 0 {
			if _, err := fmt.Fprintln(w, "  (none)"); err != nil {
				return err
			}
		} else {
			for _, item := range items {
				label := ""
				if item.Label.Valid {
					label = "  label=" + item.Label.String
				}
				if _, err := fmt.Fprintf(w, "  %s  %s  %s%s\n", item.ID, item.Type, item.CreatedAt.UTC().Format(time.RFC3339), label); err != nil {
					return err
				}
			}
		}
	}
	if _, err := fmt.Fprintf(w, "  Duration: %dms\n", durationMS); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "  Hint: "+hint); err != nil {
		return err
	}
	return nil
}

func RenderSnapshotShowHuman(
	w io.Writer,
	item snapshot.Snapshot,
	files []snapshot.SnapshotFileEntry,
	matchedFileCount int,
	totalSnapshotFileCount int64,
	durationMS int64,
	hint string,
) error {
	if w == nil {
		return fmt.Errorf("render snapshot show human: nil writer")
	}

	if _, err := fmt.Fprintf(w, "Snapshot: %s\n", item.ID); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  Type: %s\n", item.Type); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  Created: %s\n", item.CreatedAt.UTC().Format(time.RFC3339)); err != nil {
		return err
	}
	if item.Label.Valid {
		if _, err := fmt.Fprintf(w, "  Label: %s\n", item.Label.String); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "  Files (matched): %d\n", matchedFileCount); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  Files (total): %d\n", totalSnapshotFileCount); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if len(files) == 0 {
		if _, err := fmt.Fprintln(w, "  (no files)"); err != nil {
			return err
		}
	} else {
		for _, file := range files {
			if _, err := fmt.Fprintf(w, "  %s\n", file.Path); err != nil {
				return err
			}
		}
	}
	if _, err := fmt.Fprintf(w, "\n  Duration: %dms\n", durationMS); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "  Hint: "+hint); err != nil {
		return err
	}

	return nil
}
