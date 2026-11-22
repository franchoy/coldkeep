package generation

import (
	"bytes"
	"encoding/base64"
)

// Cursor binds one fixed query order and scope to one live view. It is not an
// authorization token, and does not promise detection of every valid last-key edit.
type Cursor struct {
	Version      int     `json:"version"`
	Query        string  `json:"query"`
	View         string  `json:"view"`
	RepositoryID string  `json:"repository_id"`
	SourceID     string  `json:"source_id"`
	GenerationID string  `json:"generation_id"`
	ParentID     *string `json:"parent_id"`
	Order        string  `json:"order"`
	Upper        string  `json:"upper"`
	Last         string  `json:"last"`
	EntryID      string  `json:"entry_id"`
}

func (c Cursor) valid() bool {
	if c.Version != 1 || !identitySuffix.MatchString(c.View) || !ValidID("repo", c.RepositoryID) || !ValidID("source", c.SourceID) {
		return false
	}
	if c.Query == QueryGenerations {
		return c.validGenerationOrder()
	}
	return c.validChildOrder()
}
func (c Cursor) validGenerationOrder() bool {
	return c.Order == "opaque-id-C-v1" && c.GenerationID == "" && c.ParentID == nil && c.EntryID == "" && ValidID("generation", c.Upper) && ValidID("generation", c.Last) && c.Last <= c.Upper
}
func (c Cursor) validChildOrder() bool {
	upper, e := component(c.Upper)
	last, f := component(c.Last)
	return c.validChildScope() && e == nil && f == nil && bytes.Compare(last, upper) <= 0
}
func (c Cursor) validChildScope() bool {
	return c.Query == QueryChildren && c.Order == "raw-component-v1" && ValidID("generation", c.GenerationID) && (c.ParentID == nil || ValidID("entry", *c.ParentID)) && ValidID("entry", c.EntryID)
}

func EncodeCursor(c Cursor) (string, error) {
	if !c.valid() {
		return "", ErrInvalid
	}
	b, e := encodeBounded(c, CursorBytes*3/4)
	if e != nil {
		return "", e
	}
	s := base64.RawURLEncoding.EncodeToString(b)
	if len(s) > CursorBytes {
		return "", ErrInvalid
	}
	return s, nil
}
func DecodeCursor(s string) (c Cursor, err error) {
	if len(s) > CursorBytes {
		return c, ErrInvalid
	}
	b, e := base64.RawURLEncoding.Strict().DecodeString(s)
	if e != nil {
		return c, ErrInvalid
	}
	if strictDTO(b, CursorBytes, &c) != nil || !c.valid() {
		return c, ErrInvalid
	}
	canonical, e := EncodeCursor(c)
	if e != nil || canonical != s {
		return c, ErrInvalid
	}
	return c, nil
}
