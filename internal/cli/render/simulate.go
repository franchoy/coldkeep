package render

import (
	"encoding/json"
	"fmt"
	"io"

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
	if _, err := fmt.Fprintf(w, "  exact:       %t\n", modeExact); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  mutated:     %t\n", modeMutated); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}

	if r.GC != nil {
		if _, err := fmt.Fprintln(w, "Reachability"); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "  reachable chunks:       %d\n", r.GC.Summary.ReachableChunks); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "  unreachable chunks:     %d\n", r.GC.Summary.UnreachableChunks); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}

		if _, err := fmt.Fprintln(w, "Reclaimable"); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "  logical bytes:          %s\n", formatMiB(r.GC.Summary.LogicallyReclaimableBytes)); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "  physical bytes now:     %s\n", formatMiB(r.GC.Summary.PhysicallyReclaimableBytes)); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}

		if _, err := fmt.Fprintln(w, "Containers"); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "  fully reclaimable:      %d\n", r.GC.Summary.FullyReclaimableContainers); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "  partially dead:         %d\n", r.GC.Summary.PartiallyDeadContainers); err != nil {
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
		for _, snapshotID := range r.GC.Assumptions.DeletedSnapshots {
			if _, err := fmt.Fprintf(w, "  snapshot treated as deleted: %s\n", snapshotID); err != nil {
				return err
			}
		}
	}

	if r.GC != nil && len(r.GC.Containers) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "  Containers with reclaimable chunks:"); err != nil {
			return err
		}
		for _, c := range r.GC.Containers {
			state := "[requires compaction]"
			if c.FullyReclaimable {
				state = "[fully reclaimable now]"
			}
			if _, err := fmt.Fprintf(
				w,
				"    container %d (%s): reclaimable=%d live_after_gc=%d chunks=%d/%d %s\n",
				c.ContainerID,
				c.Filename,
				c.ReclaimableBytes,
				c.LiveBytesAfterGC,
				c.ReclaimableChunks,
				c.TotalChunks,
				state,
			); err != nil {
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
		for _, warning := range r.GC.Warnings {
			if _, err := fmt.Fprintf(w, "  [%s] %s\n", warning.Code, warning.Message); err != nil {
				return err
			}
		}
	}

	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "No state was changed."); err != nil {
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
