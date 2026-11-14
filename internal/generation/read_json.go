package generation

import (
	"bytes"
	"encoding/json"
	"io"
)

func EncodeRequest(q Request) ([]byte, error) {
	if e := q.Validate(); e != nil {
		return nil, e
	}
	return encodeBounded(q, RequestBytes)
}
func DecodeRequest(b []byte) (q Request, err error) {
	if err = strictDTO(b, RequestBytes, &q); err == nil {
		err = q.Validate()
	}
	return
}
func EncodeResponse(r Response) ([]byte, error) {
	if e := r.Validate(); e != nil {
		return nil, e
	}
	return encodeBounded(r, ResponseBytes)
}
func DecodeResponse(b []byte) (r Response, err error) {
	if err = strictDTO(b, ResponseBytes, &r); err == nil {
		err = r.Validate()
	}
	return
}
func encodeBounded(v any, limit int) ([]byte, error) {
	b, e := json.Marshal(v)
	if e != nil || len(b) > limit {
		return nil, ErrInvalid
	}
	return b, nil
}

// Tokens are bounded before constructing DTO slices. Unknown keys are refused
// by the typed decoder; Unicode is checked before encoding/json can repair it.
func strictDTO(b []byte, limit int, v any) error {
	if len(b) > limit || !losslessUnicode(b) {
		return ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if walkDTO(d, 0, "") != nil {
		return ErrInvalid
	}
	if _, e := d.Token(); e != io.EOF {
		return ErrInvalid
	}
	d = json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if d.Decode(v) != nil {
		return ErrInvalid
	}
	return nil
}

func walkDTO(d *json.Decoder, depth int, field string) error {
	if depth > 12 {
		return ErrInvalid
	}
	tok, e := d.Token()
	if e != nil {
		return ErrInvalid
	}
	if s, ok := tok.(string); ok && len(s) > RequestBytes {
		return ErrInvalid
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return nil
	}
	if walkDTOContainer(d, depth, field, delim) != nil {
		return ErrInvalid
	}
	if _, e = d.Token(); e != nil {
		return ErrInvalid
	}
	return nil
}
func walkDTOContainer(d *json.Decoder, depth int, field string, delim json.Delim) error {
	switch delim {
	case '{':
		return walkDTOObject(d, depth)
	case '[':
		return walkDTOArray(d, depth, field)
	default:
		return ErrInvalid
	}
}
func walkDTOObject(d *json.Decoder, depth int) error {
	seen := map[string]bool{}
	for d.More() {
		key, e := d.Token()
		if e != nil {
			return ErrInvalid
		}
		k, ok := key.(string)
		if !ok || seen[k] || !dtoKey(k) || len(seen) >= 32 {
			return ErrInvalid
		}
		seen[k] = true
		if e = walkDTO(d, depth+1, k); e != nil {
			return e
		}
	}
	return nil
}
func walkDTOArray(d *json.Decoder, depth int, field string) error {
	cap := PageMembers
	if field == "components" {
		cap = Depth
	}
	for n := 0; d.More(); n++ {
		if n >= cap {
			return ErrInvalid
		}
		if e := walkDTO(d, depth+1, ""); e != nil {
			return e
		}
	}
	return nil
}

func dtoKey(k string) bool {
	return oneOf(k, "version", "query", "repository_id", "source_id", "generation_id", "parent_id", "entry_id", "selector", "limit", "response_limit", "cursor",
		"kind", "components", "mode", "mtime_ns", "source_revision", "policy_id", "policy_sha256", "started_ns", "ended_ns", "scan_state", "outcome", "publication_receipt",
		"component_base64", "logical_file_id", "metadata", "generation", "root", "policy_base64", "entry", "generations", "entries", "detail", "selection", "next_cursor",
		"view", "order", "upper", "last")
}
