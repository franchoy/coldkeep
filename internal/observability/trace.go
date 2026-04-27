package observability

import (
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
)

type TraceSink interface {
	Event(event TraceEvent)
}

type TraceEvent struct {
	Step     string         `json:"step"`
	Entity   string         `json:"entity,omitempty"`
	EntityID string         `json:"entity_id,omitempty"`
	Message  string         `json:"message"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type NoopTraceSink struct{}

func (NoopTraceSink) Event(event TraceEvent) {}

type HumanTraceSink struct {
	W io.Writer
}

func (s HumanTraceSink) Event(e TraceEvent) {
	fmt.Fprintf(s.W, "TRACE %s", e.Step)

	if e.Entity != "" {
		fmt.Fprintf(s.W, " %s=%s", e.Entity, e.EntityID)
	}

	if e.Message != "" {
		fmt.Fprintf(s.W, " %s", e.Message)
	}

	fmt.Fprintln(s.W)
}

type JSONTraceSink struct {
	W   io.Writer
	enc *json.Encoder
}

func NewJSONTraceSink(w io.Writer) *JSONTraceSink {
	return &JSONTraceSink{
		W:   w,
		enc: json.NewEncoder(w),
	}
}

func (s *JSONTraceSink) Event(e TraceEvent) {
	_ = s.enc.Encode(e)
}

func emitTrace(trace TraceOptions, event TraceEvent) {
	if !trace.Enabled {
		return
	}
	if trace.Sink == nil {
		return
	}
	trace.Sink.Event(sanitizeTraceEvent(event))
}

func sanitizeTraceEvent(event TraceEvent) TraceEvent {
	sanitized := event
	sanitized.Message = sanitizeStringValue("message", event.Message)
	if len(event.Metadata) > 0 {
		sanitized.Metadata = sanitizeMetadata(event.Metadata)
	}
	return sanitized
}

func sanitizeMetadata(metadata map[string]any) map[string]any {
	if len(metadata) == 0 {
		return nil
	}
	out := make(map[string]any, len(metadata))
	for key, value := range metadata {
		if isSensitiveMetadataKey(key) {
			out[key] = "[redacted]"
			continue
		}
		out[key] = sanitizeMetadataValue(key, value)
	}
	return out
}

func sanitizeMetadataValue(key string, value any) any {
	switch v := value.(type) {
	case string:
		return sanitizeStringValue(key, v)
	case []string:
		out := make([]string, len(v))
		for i := range v {
			out[i] = sanitizeStringValue(key, v[i])
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i := range v {
			out[i] = sanitizeMetadataValue(key, v[i])
		}
		return out
	case map[string]any:
		return sanitizeMetadata(v)
	default:
		return value
	}
}

func isSensitiveMetadataKey(key string) bool {
	lower := strings.ToLower(strings.TrimSpace(key))
	if lower == "" {
		return false
	}
	sensitiveTokens := []string{
		"secret",
		"password",
		"passphrase",
		"token",
		"private",
		"encryption_key",
		"key_material",
		"env",
		"database_url",
		"db_url",
		"dsn",
		"connection_string",
		"source_path",
		"original_path",
		"absolute_path",
	}
	for _, token := range sensitiveTokens {
		if strings.Contains(lower, token) {
			return true
		}
	}
	return false
}

func sanitizeStringValue(key, value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return value
	}
	if isSensitiveMetadataKey(key) {
		return "[redacted]"
	}
	if looksLikeAbsolutePath(trimmed) {
		return "[redacted_path]"
	}
	if looksLikeEnvAssignment(trimmed) {
		return "[redacted_env]"
	}
	if looksLikeDatabaseURL(trimmed) {
		return "[redacted_db_url]"
	}
	if looksLikeKeyMaterial(trimmed) {
		return "[redacted_key_material]"
	}
	return value
}

func looksLikeAbsolutePath(value string) bool {
	if strings.HasPrefix(value, "/") || strings.HasPrefix(value, `\\`) {
		return true
	}
	if matched, _ := regexp.MatchString(`(^|\s)/(?:[^\s]+)`, value); matched {
		return true
	}
	if matched, _ := regexp.MatchString(`(^|\s)[a-zA-Z]:[\\/](?:[^\s]+)`, value); matched {
		return true
	}
	matched, _ := regexp.MatchString(`^[a-zA-Z]:[\\/]`, value)
	return matched
}

func looksLikeEnvAssignment(value string) bool {
	if !strings.Contains(value, "=") {
		return false
	}
	matched, _ := regexp.MatchString(`^[A-Z_][A-Z0-9_]*=.*$`, value)
	return matched
}

func looksLikeDatabaseURL(value string) bool {
	lower := strings.ToLower(value)
	prefixes := []string{
		"postgres://",
		"postgresql://",
		"mysql://",
		"sqlite://",
		"mongodb://",
		"redis://",
		"file://",
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}

func looksLikeKeyMaterial(value string) bool {
	if strings.HasPrefix(value, "-----BEGIN ") {
		return true
	}
	lower := strings.ToLower(value)
	if strings.Contains(lower, "private key") {
		return true
	}
	return false
}
