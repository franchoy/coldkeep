package recoveryexport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

var ErrPackage = errors.New("invalid or incomplete recovery generation")

// ImportReceiptMaxObjects is bounded by the existing 20,000 catalog rows and
// directory entries, and 20,001 package members (one is the archive).
const ImportReceiptMaxObjects = 20000

func strictJSON(ctx context.Context, b []byte, v any, membersLimit int) error {
	d := json.NewDecoder(bytes.NewReader(b))
	var walk func(int, int) error
	walk = func(depth, arrayLimit int) error {
		if e := ctx.Err(); e != nil {
			return e
		}
		if depth > 12 {
			return ErrLimit
		}
		token, e := d.Token()
		if e != nil {
			return e
		}
		if s, ok := token.(string); ok && len(s) > 4096 {
			return ErrLimit
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				key, e := d.Token()
				if e != nil {
					return e
				}
				s, ok := key.(string)
				if !ok || seen[s] || len(s) > 128 {
					return ErrPackage
				}
				// encoding/json matches struct fields case-insensitively,
				// including Unicode simple folds. Refuse aliases of a key
				// already seen so the two passes have one interpretation.
				for prior := range seen {
					if strings.EqualFold(s, prior) {
						return ErrPackage
					}
				}
				seen[s] = true
				if len(seen) > 64 {
					return ErrLimit
				}
				childLimit := -1
				if depth == 0 {
					switch {
					case strings.EqualFold(s, "members"):
						childLimit = membersLimit
					case strings.EqualFold(s, "objects"):
						childLimit = ImportReceiptMaxObjects
					case strings.EqualFold(s, "uncataloged_excluded"):
						childLimit = DefaultLimits().DirectoryEntries
					}
				}
				if e = walk(depth+1, childLimit); e != nil {
					return e
				}
			}
		case '[':
			for count := 0; d.More(); count++ {
				// Enforce slice caps before typed allocation (manifest Members,
				// Excluded, or readiness Objects), including lowered caps.
				if arrayLimit >= 0 && count >= arrayLimit {
					return ErrLimit
				}
				if e = walk(depth+1, -1); e != nil {
					return e
				}
			}
		default:
			return ErrPackage
		}
		_, e = d.Token()
		return e
	}
	if e := walk(0, -1); e != nil {
		return errors.Join(ErrPackage, e)
	}
	if _, e := d.Token(); e != io.EOF {
		return ErrPackage
	}
	if e := ctx.Err(); e != nil {
		return errors.Join(ErrPackage, e)
	}
	d = json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		return errors.Join(ErrPackage, e)
	}
	if e := ctx.Err(); e != nil {
		return errors.Join(ErrPackage, e)
	}
	return nil
}
