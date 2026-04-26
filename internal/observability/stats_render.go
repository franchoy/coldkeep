package observability

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

func RenderStatsJSON(w io.Writer, r *StatsResult) error {
	if w == nil {
		return fmt.Errorf("render stats json: nil writer")
	}
	if r == nil {
		r = &StatsResult{}
	}

	encoder := json.NewEncoder(w)
	return encoder.Encode(r)
}

func RenderStatsHuman(w io.Writer, r *StatsResult) error {
	if w == nil {
		return fmt.Errorf("render stats human: nil writer")
	}
	if r == nil {
		r = &StatsResult{}
	}

	_, err := fmt.Fprintln(w, "\n====== coldkeep Stats ======")
	if err != nil {
		return err
	}
	if strings.TrimSpace(r.Repository.ActiveWriteChunker) != "" {
		if _, err := fmt.Fprintf(w, "Active chunker (new writes):     %s\n", r.Repository.ActiveWriteChunker); err != nil {
			return err
		}
	}

	if _, err := fmt.Fprintf(w, "Generated at (UTC):              %s\n", r.GeneratedAtUTC.Format("2006-01-02T15:04:05Z")); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Logical files (total):           %d\n", r.Logical.TotalFiles); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Logical stored size (total):     %.2f MB\n", bytesToMB(r.Logical.TotalSizeBytes)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  Completed files:               %d (%.2f MB)\n", r.Logical.CompletedFiles, bytesToMB(r.Logical.CompletedSizeBytes)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  Processing files:              %d\n", r.Logical.ProcessingFiles); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  Aborted files:                 %d\n", r.Logical.AbortedFiles); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(w, "Healthy containers:              %d\n", r.Containers.HealthyContainers); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Healthy container bytes:         %.2f MB\n", bytesToMB(r.Containers.HealthyBytes)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Quarantined containers:          %d\n", r.Containers.QuarantineContainers); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Quarantined container bytes:     %.2f MB\n", bytesToMB(r.Containers.QuarantineBytes)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Total containers:                %d\n", r.Containers.TotalContainers); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Total container bytes:           %.2f MB\n", bytesToMB(r.Containers.TotalBytes)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Live block bytes (physical):     %.2f MB\n", bytesToMB(r.Containers.LiveBlockBytes)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Dead block bytes (physical):     %.2f MB\n", bytesToMB(r.Containers.DeadBlockBytes)); err != nil {
		return err
	}

	if r.Efficiency.DedupRatioPercent > 0 {
		if _, err := fmt.Fprintf(w, "Dedup ratio:                     %.2f%%\n", r.Efficiency.DedupRatioPercent); err != nil {
			return err
		}
	}
	if r.Containers.FragmentationRatioPct > 0 {
		if _, err := fmt.Fprintf(w, "Fragmentation ratio:             %.2f%%\n", r.Containers.FragmentationRatioPct); err != nil {
			return err
		}
	}

	if _, err := fmt.Fprintln(w, "Snapshot retention:"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  Current-only logical files:    %d (%.2f MB)\n", r.Retention.CurrentOnlyLogicalFiles, bytesToMB(r.Retention.CurrentOnlyBytes)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  Snapshot-referenced files:     %d (%.2f MB)\n", r.Retention.SnapshotReferencedLogicalFiles, bytesToMB(r.Retention.SnapshotReferencedBytes)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  Snapshot-only logical files:   %d (%.2f MB)\n", r.Retention.SnapshotOnlyLogicalFiles, bytesToMB(r.Retention.SnapshotOnlyBytes)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  Shared logical files:          %d (%.2f MB)\n", r.Retention.SharedLogicalFiles, bytesToMB(r.Retention.SharedBytes)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Snapshots (total):               %d\n", r.Snapshots.TotalSnapshots); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(w, "Graph snapshot reachable chunks: %d\n", r.Graph.SnapshotReachableChunks); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Graph snapshot reachable bytes:  %.2f MB\n", bytesToMB(r.Graph.SnapshotReachableBytes)); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(w, "============================"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Chunks (total):           %d\n", r.Chunks.TotalChunks); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  Completed chunks:       %d (%.2f MB)\n", r.Chunks.CompletedChunks, bytesToMB(r.Chunks.CompletedBytes)); err != nil {
		return err
	}

	versions := sortedVersionStats(r.Chunks.ChunkerVersions)
	if len(versions) > 0 {
		if _, err := fmt.Fprintln(w, "Chunker Distribution:"); err != nil {
			return err
		}
		for _, version := range versions {
			if _, err := fmt.Fprintf(w, "  %-22s %d chunks\n", version.Version+":", version.Chunks); err != nil {
				return err
			}
		}

		if _, err := fmt.Fprintln(w, "Stored Data by Chunker:"); err != nil {
			return err
		}
		for _, version := range versions {
			if _, err := fmt.Fprintf(w, "  %-22s %.2f GB\n", version.Version+":", bytesToGB(version.Bytes)); err != nil {
				return err
			}
		}
	}

	if _, err := fmt.Fprintln(w, "Dedup Signal:"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  Total chunk references:  %d\n", r.Chunks.TotalReferences); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  Unique referenced chunks:%d\n", r.Chunks.UniqueReferenced); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  Estimated dedup ratio:   %.2f%%\n", r.Logical.EstimatedDedupRatioPct); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "============================"); err != nil {
		return err
	}

	if len(r.Containers.Records) > 0 {
		if _, err := fmt.Fprintln(w, "\nPer-container breakdown:"); err != nil {
			return err
		}
		for _, c := range r.Containers.Records {
			if _, err := fmt.Fprintf(
				w,
				"Container %d (%s): quarantined=%t : total=%.2fMB live=%.2fMB dead=%.2fMB live_ratio=%.2f%%\n",
				c.ID,
				c.Filename,
				c.Quarantine,
				bytesToMB(c.TotalBytes),
				bytesToMB(c.LiveBytes),
				bytesToMB(c.DeadBytes),
				c.LiveRatioPct,
			); err != nil {
				return err
			}
		}
	}

	if len(r.Warnings) > 0 {
		if _, err := fmt.Fprintln(w, "\nWarnings:"); err != nil {
			return err
		}
		for _, warning := range r.Warnings {
			if _, err := fmt.Fprintf(w, "- [%s] %s\n", warning.Code, warning.Message); err != nil {
				return err
			}
		}
	}

	return nil
}

func sortedVersionStats(in []VersionStat) []VersionStat {
	if len(in) == 0 {
		return nil
	}
	out := append([]VersionStat(nil), in...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].Version < out[j].Version
	})
	return out
}

func bytesToMB(bytes int64) float64 {
	return float64(bytes) / (1024 * 1024)
}

func bytesToGB(bytes int64) float64 {
	return float64(bytes) / (1024 * 1024 * 1024)
}
