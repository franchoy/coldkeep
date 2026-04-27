package render

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"github.com/franchoy/coldkeep/internal/observability"
)

func (HumanRenderer) RenderSimulation(w io.Writer, r *SimulationResult) error {
	if w == nil {
		return fmt.Errorf("render simulation human: nil writer")
	}
	if r == nil {
		r = &SimulationResult{}
	}

	if _, err := fmt.Fprintln(w, "GC simulation"); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}

	modeExact := r.Exact
	modeMutated := r.Mutated
	if r.GC != nil {
		modeExact = r.GC.Exact
		modeMutated = r.GC.Mutated
	}
	if _, err := fmt.Fprintln(w, "Mode"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  exact: %t\n", modeExact); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  mutated: %t\n", modeMutated); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}

	if r.GC != nil {
		if _, err := fmt.Fprintln(w, "Reachability"); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "  reachable_chunks: %d\n", r.GC.Summary.ReachableChunks); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "  unreachable_chunks: %d\n", r.GC.Summary.UnreachableChunks); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}

		if _, err := fmt.Fprintln(w, "Reclaimable"); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "  logical_bytes: %s\n", formatMiB(r.GC.Summary.LogicallyReclaimableBytes)); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "  physical_bytes_now: %s\n", formatMiB(r.GC.Summary.PhysicallyReclaimableBytes)); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}

		if _, err := fmt.Fprintln(w, "Containers"); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "  fully_reclaimable: %d\n", r.GC.Summary.FullyReclaimableContainers); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "  partially_dead: %d\n", r.GC.Summary.PartiallyDeadContainers); err != nil {
			return err
		}
	}

	if r.GC != nil && len(r.GC.Assumptions.DeletedSnapshots) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "Assumptions"); err != nil {
			return err
		}
		assumedDeleted := append([]string(nil), r.GC.Assumptions.DeletedSnapshots...)
		sort.Strings(assumedDeleted)
		for _, snapshotID := range assumedDeleted {
			if _, err := fmt.Fprintf(w, "  deleted_snapshot: %s\n", snapshotID); err != nil {
				return err
			}
		}
	}

	if r.GC != nil && len(r.GC.Containers) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "Container impact"); err != nil {
			return err
		}
		for _, c := range sortedContainerImpact(r.GC.Containers) {
			state := "requires_compaction"
			if c.FullyReclaimable {
				state = "fully_reclaimable_now"
			}
			if _, err := fmt.Fprintf(w, "  container: %d (%s)\n", c.ContainerID, c.Filename); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(w, "  reclaimable_bytes: %d\n", c.ReclaimableBytes); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(w, "  live_bytes_after_gc: %d\n", c.LiveBytesAfterGC); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(w, "  reclaimable_chunks: %d\n", c.ReclaimableChunks); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(w, "  total_chunks: %d\n", c.TotalChunks); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(w, "  status: %s\n", state); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(w); err != nil {
				return err
			}
		}
	}

	if r.GC != nil && len(r.GC.Warnings) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "Warnings"); err != nil {
			return err
		}
		for _, warning := range sortedSimulationWarnings(r.GC.Warnings) {
			if _, err := fmt.Fprintf(w, "  warning: [%s] %s\n", warning.Code, warning.Message); err != nil {
				return err
			}
		}
	}

	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "Result"); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "  changed: false"); err != nil {
		return err
	}

	return nil
}

func (JSONRenderer) RenderSimulation(w io.Writer, r *SimulationResult) error {
	if w == nil {
		return fmt.Errorf("render simulation json: nil writer")
	}
	if r == nil {
		r = &SimulationResult{}
	}
	r = normalizedSimulationForOutput(r)

	data, err := toObjectMap(r)
	if err != nil {
		return fmt.Errorf("render simulation json: encode data: %w", err)
	}
	delete(data, "generated_at_utc")
	delete(data, "warnings")

	warnings := normalizeWarnings(r.Warnings)
	if r.GC != nil {
		if gc, ok := data["gc"].(map[string]any); ok {
			delete(gc, "generated_at_utc")
			delete(gc, "warnings")
		}
		warnings = append(warnings, normalizeWarnings(r.GC.Warnings)...)
	}
	warnings = normalizeWarnings(warnings)

	exact := r.Exact
	if r.GC != nil {
		exact = r.GC.Exact
	}

	envelope := jsonEnvelope{
		GeneratedAtUTC: normalizeGeneratedAt(r.GeneratedAtUTC),
		Type:           "simulation",
		Data:           data,
		Warnings:       warnings,
		Meta: jsonEnvelopeMeta{
			Version: cliJSONSchemaVersion,
			Exact:   exact,
		},
	}

	encoder := json.NewEncoder(w)
	return encoder.Encode(envelope)
}

func formatMiB(bytes int64) string {
	mb := float64(bytes) / (1024 * 1024)
	return fmt.Sprintf("%.0f MiB", mb)
}

func CloneSimulationResult(input *observability.SimulationResult) *SimulationResult {
	if input == nil {
		return &SimulationResult{}
	}
	out := *input
	if input.GC != nil {
		gc := *input.GC
		if len(input.GC.Assumptions.DeletedSnapshots) > 0 {
			gc.Assumptions.DeletedSnapshots = append([]string(nil), input.GC.Assumptions.DeletedSnapshots...)
		}
		if len(input.GC.Containers) > 0 {
			gc.Containers = append([]observability.ContainerSimulationImpact(nil), input.GC.Containers...)
		}
		if len(input.GC.Warnings) > 0 {
			gc.Warnings = append([]observability.ObservationWarning(nil), input.GC.Warnings...)
		}
		out.GC = &gc
	}
	if len(input.Warnings) > 0 {
		out.Warnings = append([]observability.ObservationWarning(nil), input.Warnings...)
	}
	return &out
}

func sortedContainerImpact(in []observability.ContainerSimulationImpact) []observability.ContainerSimulationImpact {
	if len(in) == 0 {
		return nil
	}
	out := append([]observability.ContainerSimulationImpact(nil), in...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].ContainerID == out[j].ContainerID {
			return out[i].Filename < out[j].Filename
		}
		return out[i].ContainerID < out[j].ContainerID
	})
	return out
}

func sortedSimulationWarnings(in []observability.ObservationWarning) []observability.ObservationWarning {
	if len(in) == 0 {
		return nil
	}
	out := append([]observability.ObservationWarning(nil), in...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Code == out[j].Code {
			return out[i].Message < out[j].Message
		}
		return out[i].Code < out[j].Code
	})
	return out
}

func normalizedSimulationForOutput(input *SimulationResult) *SimulationResult {
	if input == nil {
		return &SimulationResult{}
	}
	out := CloneSimulationResult(input)
	out.Warnings = normalizeWarnings(out.Warnings)
	if out.GC != nil {
		out.GC.Assumptions.DeletedSnapshots = sortedStrings(out.GC.Assumptions.DeletedSnapshots)
		out.GC.Containers = sortedContainerImpact(out.GC.Containers)
		out.GC.Warnings = normalizeWarnings(out.GC.Warnings)
	}
	return out
}

func sortedStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}
