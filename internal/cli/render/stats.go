package render

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/franchoy/coldkeep/internal/observability"
)

type StatsResult = observability.StatsResult
type InspectResult = observability.InspectResult
type SimulationResult = observability.SimulationResult

type Renderer interface {
	RenderStats(io.Writer, *StatsResult) error
	RenderInspect(io.Writer, *InspectResult) error
	RenderSimulation(io.Writer, *SimulationResult) error
}

type HumanRenderer struct{}

type JSONRenderer struct{}

func (JSONRenderer) RenderStats(w io.Writer, r *StatsResult) error {
	if w == nil {
		return fmt.Errorf("render stats json: nil writer")
	}
	if r == nil {
		r = &StatsResult{}
	}

	data, err := toObjectMap(r)
	if err != nil {
		return fmt.Errorf("render stats json: encode data: %w", err)
	}
	delete(data, "generated_at_utc")
	delete(data, "warnings")

	envelope := jsonEnvelope{
		GeneratedAtUTC: normalizeGeneratedAt(r.GeneratedAtUTC),
		Type:           "stats",
		Data:           data,
		Warnings:       normalizeWarnings(r.Warnings),
		Meta: jsonEnvelopeMeta{
			Version: cliJSONSchemaVersion,
			Exact:   true,
		},
	}

	encoder := json.NewEncoder(w)
	return encoder.Encode(envelope)
}

func (HumanRenderer) RenderStats(w io.Writer, r *StatsResult) error {
	if w == nil {
		return fmt.Errorf("render stats human: nil writer")
	}
	if r == nil {
		r = &StatsResult{}
	}

	if _, err := fmt.Fprintln(w, "Coldkeep repository stats"); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(w, "\nRepository"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  active write chunker: %s\n", fallbackString(r.Repository.ActiveWriteChunker, "unknown")); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(w, "\nLogical data"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  files:               %s\n", formatIntGrouped(r.Logical.TotalFiles)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  completed files:     %s\n", formatIntGrouped(r.Logical.CompletedFiles)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  logical size:        %s\n", formatIECBytes(r.Logical.TotalSizeBytes)); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(w, "\nPhysical data"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  physical files:      %s\n", formatIntGrouped(r.Physical.TotalPhysicalFiles)); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(w, "\nChunks"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  total chunks:        %s\n", formatIntGrouped(r.Chunks.TotalChunks)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  completed chunks:    %s\n", formatIntGrouped(r.Chunks.CompletedChunks)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  unique chunk bytes:  %s\n", formatIECBytes(r.Chunks.CompletedBytes)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  references:          %s\n", formatIntGrouped(r.Chunks.TotalReferences)); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(w, "\nEfficiency"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  dedup ratio:         %.2fx\n", r.Efficiency.DedupRatio); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  dedup savings:       %.1f%%\n", r.Efficiency.DedupRatioPercent); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  container overhead:  %.1f%%\n", r.Efficiency.ContainerOverheadPct); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(w, "\nContainers"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  total containers:    %s\n", formatIntGrouped(r.Containers.TotalContainers)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  healthy containers:  %s\n", formatIntGrouped(r.Containers.HealthyContainers)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  quarantined:         %s\n", formatIntGrouped(r.Containers.QuarantineContainers)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  container bytes:     %s\n", formatIECBytes(r.Containers.TotalBytes)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  live block bytes:    %s\n", formatIECBytes(r.Containers.LiveBlockBytes)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  dead block bytes:    %s\n", formatIECBytes(r.Containers.DeadBlockBytes)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  fragmentation:       %.1f%%\n", r.Containers.FragmentationRatioPct); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(w, "\nSnapshots"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  snapshots:           %s\n", formatIntGrouped(r.Snapshots.TotalSnapshots)); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(w, "\nRetention"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  current-only files:  %s\n", formatIntGrouped(r.Retention.CurrentOnlyLogicalFiles)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  snapshot-only files: %s\n", formatIntGrouped(r.Retention.SnapshotOnlyLogicalFiles)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  shared files:        %s\n", formatIntGrouped(r.Retention.SharedLogicalFiles)); err != nil {
		return err
	}

	versions := sortedVersionStats(r.Chunks.ChunkerVersions)
	if len(versions) > 0 {
		if _, err := fmt.Fprintln(w, "\nChunker versions"); err != nil {
			return err
		}
		for _, version := range versions {
			if _, err := fmt.Fprintf(
				w,
				"  %-20s %s chunks / %s\n",
				version.Version+":",
				formatIntGrouped(version.Chunks),
				formatIECBytes(version.Bytes),
			); err != nil {
				return err
			}
		}
	}

	if len(r.Containers.Records) > 0 {
		records := sortedContainerRecords(r.Containers.Records)
		if _, err := fmt.Fprintln(w, "\nContainer details"); err != nil {
			return err
		}

		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		if _, err := fmt.Fprintln(tw, "  id\tfile\tsize\tlive\tdead\tstatus"); err != nil {
			return err
		}
		for _, c := range records {
			status := "healthy"
			if c.Quarantine {
				status = "quarantined"
			}
			if _, err := fmt.Fprintf(
				tw,
				"  %d\t%s\t%s\t%s\t%s\t%s\n",
				c.ID,
				c.Filename,
				formatIECBytes(c.TotalBytes),
				formatIECBytes(c.LiveBytes),
				formatIECBytes(c.DeadBytes),
				status,
			); err != nil {
				return err
			}
		}
		if err := tw.Flush(); err != nil {
			return err
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

func sortedVersionStats(in []observability.VersionStat) []observability.VersionStat {
	if len(in) == 0 {
		return nil
	}
	out := append([]observability.VersionStat(nil), in...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].Version < out[j].Version
	})
	return out
}

func sortedContainerRecords(in []observability.ContainerStatRecord) []observability.ContainerStatRecord {
	if len(in) == 0 {
		return nil
	}
	out := append([]observability.ContainerStatRecord(nil), in...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].ID == out[j].ID {
			return out[i].Filename < out[j].Filename
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func formatIECBytes(bytes int64) string {
	abs := bytes
	if abs < 0 {
		abs = -abs
	}

	const (
		ki = int64(1024)
		mi = ki * 1024
		gi = mi * 1024
		ti = gi * 1024
	)

	format := func(v float64, unit string) string {
		if unit == "B" {
			return fmt.Sprintf("%d B", bytes)
		}
		return fmt.Sprintf("%.1f %s", v, unit)
	}

	switch {
	case abs >= ti:
		return format(float64(bytes)/float64(ti), "TiB")
	case abs >= gi:
		return format(float64(bytes)/float64(gi), "GiB")
	case abs >= mi:
		return format(float64(bytes)/float64(mi), "MiB")
	case abs >= ki:
		return format(float64(bytes)/float64(ki), "KiB")
	default:
		return format(float64(bytes), "B")
	}
}

func formatIntGrouped(n int64) string {
	negative := n < 0
	if negative {
		n = -n
	}

	s := strconv.FormatInt(n, 10)
	if len(s) <= 3 {
		if negative {
			return "-" + s
		}
		return s
	}

	var b strings.Builder
	if negative {
		b.WriteByte('-')
	}

	pre := len(s) % 3
	if pre == 0 {
		pre = 3
	}
	b.WriteString(s[:pre])
	for i := pre; i < len(s); i += 3 {
		b.WriteByte(',')
		b.WriteString(s[i : i+3])
	}

	return b.String()
}

func fallbackString(v, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}
