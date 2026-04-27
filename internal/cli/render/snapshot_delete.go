package render

import (
	"fmt"
	"strings"

	"github.com/franchoy/coldkeep/internal/snapshot"
)

func SnapshotDeleteWarnings(preview *snapshot.DeleteLineagePreview) []map[string]any {
	if preview == nil || len(preview.ChildSnapshotIDs) == 0 {
		return nil
	}
	return []map[string]any{
		{
			"type":    "lineage_breakage",
			"message": "Deleting this snapshot will break lineage visualization for its children.",
			"details": map[string]any{
				"affected_snapshots": preview.ChildSnapshotIDs,
				"note":               "Affected snapshots remain fully usable; only lineage information is affected.",
			},
		},
	}
}

func FormatSnapshotDeleteDryRunOutput(snapshotID string, preview *snapshot.DeleteLineagePreview) string {
	var buf strings.Builder

	fmt.Fprintf(&buf, "Snapshot: %s\n", snapshotID)
	buf.WriteString("\n")

	if preview != nil {
		buf.WriteString("Files:\n")
		fmt.Fprintf(&buf, "  Total:        %s\n", formatNumberWithCommas(preview.TotalFiles))
		fmt.Fprintf(&buf, "  Unique:       %s\n", formatNumberWithCommas(preview.UniqueFiles))
		fmt.Fprintf(&buf, "  Shared:       %s\n", formatNumberWithCommas(preview.SharedFiles))
		buf.WriteString("\n")

		buf.WriteString("Lineage:\n")
		if preview.ParentID.Valid {
			if preview.ParentMissing {
				buf.WriteString("  Parent: (missing)\n")
				buf.WriteString("  Parent note: parent snapshot metadata is missing; this snapshot remains usable\n")
			} else {
				fmt.Fprintf(&buf, "  Parent: %s\n", preview.ParentID.String)
			}
		} else {
			buf.WriteString("  Parent: none\n")
		}
		if len(preview.ChildSnapshotIDs) > 0 {
			buf.WriteString("  Children:\n")
			for _, childID := range preview.ChildSnapshotIDs {
				fmt.Fprintf(&buf, "    - %s\n", childID)
			}
		} else {
			buf.WriteString("  Children: none\n")
		}
		buf.WriteString("\n")

		if len(preview.ChildSnapshotIDs) > 0 {
			buf.WriteString("Warning:\n")
			buf.WriteString("  This snapshot is parent of:\n")
			for _, childID := range preview.ChildSnapshotIDs {
				fmt.Fprintf(&buf, "    - %s\n", childID)
			}
			buf.WriteString("\n")
			buf.WriteString("  Deleting it will break lineage visualization,\n")
			buf.WriteString("  but snapshots remain fully usable.\n")
			buf.WriteString("\n")
		}

		buf.WriteString("Impact:\n")
		buf.WriteString("  Deleting this snapshot will:\n")
		buf.WriteString("    - remove snapshot metadata\n")
		buf.WriteString("    - NOT delete shared data\n")
		if preview.UniqueFiles > 0 {
			fmt.Fprintf(&buf, "    - remove %s unique snapshot file reference(s) from metadata\n", formatNumberWithCommas(preview.UniqueFiles))
			buf.WriteString("      (reference impact only; does not guarantee reclaimed disk space)\n")
		}
		buf.WriteString("\n")
	}

	buf.WriteString("Dry run: no changes applied.\n")
	return buf.String()
}

func formatNumberWithCommas(n int64) string {
	if n < 0 {
		return "-" + formatNumberWithCommas(-n)
	}
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}

	var result strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result.WriteRune(',')
		}
		result.WriteRune(c)
	}
	return result.String()
}
