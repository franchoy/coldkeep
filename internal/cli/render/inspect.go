package render

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/franchoy/coldkeep/internal/observability"
)

func (HumanRenderer) RenderInspect(w io.Writer, r *InspectResult) error {
	if w == nil {
		return fmt.Errorf("render inspect human: nil writer")
	}
	if r == nil {
		r = &InspectResult{}
	}

	title := fmt.Sprintf("%s %s", humanizeEntityType(r.EntityType), strings.TrimSpace(r.EntityID))
	title = strings.TrimSpace(title)
	if title == "" {
		title = "Inspect"
	}
	if _, err := fmt.Fprintln(w, title); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(w, "\nSummary"); err != nil {
		return err
	}
	for _, row := range orderedSummaryRows(r.EntityType, r.Summary) {
		if _, err := fmt.Fprintf(w, "  %-18s %s\n", row.label+":", row.value); err != nil {
			return err
		}
	}

	references := filterRelationsByDirection(r.Relations, observability.RelationOutgoing)
	if len(references) > 0 {
		if _, err := fmt.Fprintln(w, "\nReferences"); err != nil {
			return err
		}
		for _, rel := range references {
			if _, err := fmt.Fprintf(w, "  %-18s %s\n", humanizeEntityType(rel.TargetType)+":", rel.TargetID); err != nil {
				return err
			}
		}
	}

	referencedBy := filterRelationsByDirection(r.Relations, observability.RelationIncoming)
	if len(referencedBy) > 0 {
		if _, err := fmt.Fprintln(w, "\nReferenced by"); err != nil {
			return err
		}
		for _, rel := range referencedBy {
			if _, err := fmt.Fprintf(w, "  %-18s %s\n", humanizeEntityType(rel.TargetType)+":", rel.TargetID); err != nil {
				return err
			}
		}
	}

	return nil
}

func (JSONRenderer) RenderInspect(w io.Writer, r *InspectResult) error {
	if w == nil {
		return fmt.Errorf("render inspect json: nil writer")
	}
	if r == nil {
		r = &InspectResult{}
	}

	encoder := json.NewEncoder(w)
	return encoder.Encode(r)
}

type inspectSummaryRow struct {
	label string
	value string
}

func orderedSummaryRows(entityType observability.EntityType, summary map[string]any) []inspectSummaryRow {
	if len(summary) == 0 {
		return nil
	}

	order := preferredSummaryOrder(entityType)
	used := make(map[string]struct{}, len(summary))
	out := make([]inspectSummaryRow, 0, len(summary))

	for _, key := range order {
		v, ok := summary[key]
		if !ok {
			continue
		}
		out = append(out, inspectSummaryRow{label: summaryLabel(key), value: summaryValueString(key, v)})
		used[key] = struct{}{}
	}

	remaining := make([]string, 0, len(summary)-len(used))
	for key := range summary {
		if _, ok := used[key]; !ok {
			remaining = append(remaining, key)
		}
	}
	sort.Strings(remaining)
	for _, key := range remaining {
		out = append(out, inspectSummaryRow{label: summaryLabel(key), value: summaryValueString(key, summary[key])})
	}

	return out
}

func preferredSummaryOrder(entityType observability.EntityType) []string {
	switch entityType {
	case observability.EntityChunk:
		return []string{"size_bytes", "chunker_version", "container_id"}
	case observability.EntityLogicalFile, observability.EntityFile:
		return []string{"original_name", "chunk_count", "chunker_version"}
	default:
		return nil
	}
}

func summaryLabel(key string) string {
	switch key {
	case "size_bytes":
		return "size"
	case "original_name":
		return "name"
	case "chunk_count":
		return "chunks"
	case "chunker_version":
		return "chunker version"
	case "container_id":
		return "container"
	default:
		return strings.ReplaceAll(key, "_", " ")
	}
}

func summaryValueString(key string, value any) string {
	if value == nil {
		return "null"
	}

	switch key {
	case "size_bytes", "total_size_bytes", "avg_chunk_size_bytes", "stored_size_bytes":
		if n, ok := toInt64(value); ok {
			return formatIECBytes(n)
		}
	}

	if n, ok := toInt64(value); ok {
		return strconv.FormatInt(n, 10)
	}
	if f, ok := toFloat64(value); ok {
		if f == float64(int64(f)) {
			return strconv.FormatInt(int64(f), 10)
		}
		return fmt.Sprintf("%.2f", f)
	}
	if s, ok := value.(string); ok {
		return s
	}
	if b, ok := value.(bool); ok {
		if b {
			return "true"
		}
		return "false"
	}

	return fmt.Sprintf("%v", value)
}

func humanizeEntityType(entityType observability.EntityType) string {
	label := strings.ReplaceAll(string(entityType), "_", " ")
	label = strings.TrimSpace(label)
	if label == "" {
		return "Entity"
	}
	return strings.ToUpper(label[:1]) + label[1:]
}

func filterRelationsByDirection(relations []observability.Relation, direction observability.RelationDirection) []observability.Relation {
	out := make([]observability.Relation, 0, len(relations))
	for _, rel := range relations {
		if rel.Direction == direction {
			out = append(out, rel)
		}
	}
	return out
}

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case uint:
		return int64(n), true
	case uint8:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint64:
		if n > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return int64(n), true
	case float64:
		return int64(n), true
	case float32:
		return int64(n), true
	default:
		return 0, false
	}
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	default:
		return 0, false
	}
}
