package batch

import (
	"fmt"
	"strings"
)

// NewReport builds a report and computes its summary.
func NewReport(results []ItemResult) Report {
	return Report{
		Summary: Summarize(results),
		Results: results,
	}
}

// Summarize computes aggregate counts from per-item results.
func Summarize(results []ItemResult) Summary {
	summary := Summary{Total: len(results)}
	for _, result := range results {
		switch result.Status {
		case ResultSuccess:
			summary.Success++
		case ResultFailed:
			summary.Failed++
		case ResultSkipped:
			summary.Skipped++
		}
	}
	return summary
}

// FormatHuman renders a stable, human-readable report.
func FormatHuman(report Report) string {
	var b strings.Builder
	for _, item := range report.Results {
		_, _ = fmt.Fprintf(&b, "[%s] %s: %s\n", item.Status, item.Target, item.Message)
	}
	_, _ = fmt.Fprintf(
		&b,
		"Summary: total=%d success=%d failed=%d skipped=%d",
		report.Summary.Total,
		report.Summary.Success,
		report.Summary.Failed,
		report.Summary.Skipped,
	)
	return b.String()
}

// ExitCodeFromReport returns zero when all items succeeded or were skipped.
func ExitCodeFromReport(report Report) int {
	if report.Summary.Failed > 0 {
		return 1
	}
	return 0
}
