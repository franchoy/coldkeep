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
	var walk func(int, int) error
	walk = func(depth, cap int) error {
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
		switch delim {
		case '{':
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
				childCap := Depth
				switch k {
				case "kind", "components", "version", "intent", "scope", "selection_version", "action", "selection":
				case "rules":
					childCap = PolicyRules
				default:
					return ErrInvalid
				}
				if e = walk(depth+1, childCap); e != nil {
					return e
				}
			}
		case '[':
			for n := 0; d.More(); n++ {
				if n >= cap {
					return ErrInvalid
				}
				if e = walk(depth+1, Depth); e != nil {
					return e
				}
			}
		default:
			return ErrInvalid
		}
		_, e = d.Token()
		return e
	}
	if e := walk(0, Depth); e != nil {
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
			if i+1 >= len(b) || b[i+1] != 'u' {
				i++
				continue
			}
			unit, ok := unicodeEscape(b[i:])
			if !ok || unit >= 0xdc00 && unit <= 0xdfff {
				return false
			}
			i += 5
			if unit >= 0xd800 && unit <= 0xdbff {
				unit, ok = unicodeEscape(b[i+1:])
				if !ok || unit < 0xdc00 || unit > 0xdfff {
					return false
				}
				i += 6
			}
		}
	}
	return true
}
func unicodeEscape(b []byte) (uint64, bool) {
	if len(b) < 6 || b[0] != '\\' || b[1] != 'u' {
		return 0, false
	}
	n, e := strconv.ParseUint(string(b[2:6]), 16, 16)
	return n, e == nil
}
