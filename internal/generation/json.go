package generation

import (
	"bytes"
	"encoding/json"
	"io"
	"strconv"
	"unicode/utf8"
)

// This bounded lexical pass runs before typed allocation or Unicode repair.
func decode(b []byte, limit int, value any) error {
	if len(b) > limit || !losslessUnicode(b) {
		return ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(b))
	if walkModel(d, 0, Depth) != nil {
		return ErrInvalid
	}
	if _, e := d.Token(); e != io.EOF {
		return ErrInvalid
	}
	d = json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if e := d.Decode(value); e != nil {
		return ErrInvalid
	}
	return nil
}

// The model walker deliberately keeps its depth-eight grammar separate from DTOs.
func walkModel(d *json.Decoder, depth, cap int) error {
	if depth > 8 {
		return ErrInvalid
	}
	tok, e := d.Token()
	if e != nil {
		return ErrInvalid
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return nil
	}
	if e = walkModelContainer(d, depth, cap, delim); e != nil {
		return e
	}
	_, e = d.Token()
	return e
}
func walkModelContainer(d *json.Decoder, depth, cap int, delim json.Delim) error {
	switch delim {
	case '{':
		return walkModelObject(d, depth)
	case '[':
		return walkModelArray(d, depth, cap)
	default:
		return ErrInvalid
	}
}
func walkModelObject(d *json.Decoder, depth int) error {
	seen := map[string]bool{}
	for d.More() {
		key, e := d.Token()
		if e != nil {
			return ErrInvalid
		}
		k, ok := key.(string)
		if !ok || seen[k] {
			return ErrInvalid
		}
		seen[k] = true
		childCap, e := modelFieldCap(k)
		if e != nil {
			return e
		}
		if e = walkModel(d, depth+1, childCap); e != nil {
			return e
		}
	}
	return nil
}
func modelFieldCap(k string) (int, error) {
	switch k {
	case "kind", "components", "version", "intent", "scope", "selection_version", "action", "selection":
		return Depth, nil
	case "rules":
		return PolicyRules, nil
	default:
		return 0, ErrInvalid
	}
}
func walkModelArray(d *json.Decoder, depth, cap int) error {
	for n := 0; d.More(); n++ {
		if n >= cap {
			return ErrInvalid
		}
		if e := walkModel(d, depth+1, Depth); e != nil {
			return e
		}
	}
	return nil
}

// Mirrors the accepted operator-config lossless Unicode contract. JSON syntax
// is checked separately; escaped backslash-u is literal text, not an escape.
func losslessUnicode(b []byte) bool {
	if !utf8.Valid(b) {
		return false
	}
	inString := false
	for i := 0; i < len(b); i++ {
		switch b[i] {
		case '"':
			inString = !inString
		case '\\':
			if !inString {
				continue
			}
			advance, ok := unicodeEscapeSpan(b[i:])
			if !ok {
				return false
			}
			i += advance
		}
	}
	return true
}

// Return the bytes to skip after the current backslash. JSON syntax is separate.
func unicodeEscapeSpan(b []byte) (int, bool) {
	if len(b) < 2 || b[1] != 'u' {
		return 1, true
	}
	unit, ok := unicodeEscape(b)
	if !ok || lowSurrogate(unit) {
		return 0, false
	}
	if !highSurrogate(unit) {
		return 5, true
	}
	unit, ok = unicodeEscape(b[6:])
	return 11, ok && lowSurrogate(unit)
}
func lowSurrogate(unit uint64) bool  { return unit >= 0xdc00 && unit <= 0xdfff }
func highSurrogate(unit uint64) bool { return unit >= 0xd800 && unit <= 0xdbff }
func unicodeEscape(b []byte) (uint64, bool) {
	if len(b) < 6 || b[0] != '\\' || b[1] != 'u' {
		return 0, false
	}
	n, e := strconv.ParseUint(string(b[2:6]), 16, 16)
	return n, e == nil
}
