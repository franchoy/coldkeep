package render

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/franchoy/coldkeep/internal/observability"
)

// SimulateStoreReport mirrors observability's read-only simulate-store report.
type SimulateStoreReport = observability.SimulateStoreReport

func RenderSimulateStoreHuman(w io.Writer, r *SimulateStoreReport) error {
	if w == nil {
		return fmt.Errorf("render simulate-store human: nil writer")
	}
	if r == nil {
		r = &SimulateStoreReport{}
	}

	if _, err := fmt.Fprintf(w, "[SIMULATE] subcommand=%s path=%s (dry run — no data written to storage)\n", r.Subcommand, r.Path); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  Files:          %d\n", r.Files); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  Chunks:         %d\n", r.Chunks); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  Containers:     %d\n", r.Containers); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  Logical size:   %d bytes (%.2f MB)\n", r.LogicalSizeBytes, float64(r.LogicalSizeBytes)/(1024*1024)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "  Physical size:  %d bytes (%.2f MB)\n", r.PhysicalSizeBytes, float64(r.PhysicalSizeBytes)/(1024*1024)); err != nil {
		return err
	}
	if r.DedupRatioPct > 0 {
		if _, err := fmt.Fprintf(w, "  Dedup savings:  %.2f%%\n", r.DedupRatioPct); err != nil {
			return err
		}
	}

	return nil
}

func RenderSimulateStoreJSON(w io.Writer, r *SimulateStoreReport) error {
	if w == nil {
		return fmt.Errorf("render simulate-store json: nil writer")
	}
	if r == nil {
		r = &SimulateStoreReport{}
	}

	payload := map[string]any{
		"status":    "ok",
		"command":   "simulate",
		"simulated": true,
		"data":      r,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("render simulate-store json: %w", err)
	}
	if _, err := fmt.Fprintln(w, string(encoded)); err != nil {
		return err
	}
	return nil
}
