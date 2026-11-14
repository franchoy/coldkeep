// Package generation defines portable records, not capture or publication.
package generation

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
)

const (
	ComponentBytes = 255
	PathBytes      = 3072
	Depth          = 128
	SelectorBytes  = 8192
	PolicyBytes    = 4096
	PolicyRules    = 32
)

var ErrInvalid = errors.New("invalid generation record")

type Selector struct {
	Kind       string   `json:"kind"`
	Components []string `json:"components"`
}
type Rule struct {
	Action    string   `json:"action"`
	Selection Selector `json:"selection"`
}
type Policy struct {
	Version          int    `json:"version"`
	Intent           string `json:"intent"`
	Scope            string `json:"scope"`
	SelectionVersion int    `json:"selection_version"`
	Rules            []Rule `json:"rules"`
}

func ValidComponent(b []byte) bool {
	return len(b) > 0 && len(b) <= ComponentBytes && !bytes.ContainsAny(b, "\x00/") && !bytes.Equal(b, []byte(".")) && !bytes.Equal(b, []byte(".."))
}
func (s Selector) Bytes() ([][]byte, error) {
	if s.Kind != "posix-bytes-base64" || len(s.Components) == 0 || len(s.Components) > Depth {
		return nil, ErrInvalid
	}
	result := make([][]byte, 0, len(s.Components))
	size := 0
	for _, part := range s.Components {
		b, e := component(part)
		if e != nil {
			return nil, e
		}
		if len(result) > 0 {
			size++
		}
		size += len(b)
		if size > PathBytes {
			return nil, ErrInvalid
		}
		result = append(result, b)
	}
	return result, nil
}
func DecodeSelector(b []byte) (Selector, error) {
	var s Selector
	if e := decode(b, SelectorBytes, &s); e != nil {
		return s, e
	}
	_, e := s.Bytes()
	return s, e
}
func (p Policy) Canonical() ([]byte, error) {
	if !p.validEnvelope() {
		return nil, ErrInvalid
	}
	if e := p.validateRules(); e != nil {
		return nil, e
	}
	b, e := json.Marshal(p)
	if e != nil || len(b) > PolicyBytes {
		return nil, ErrInvalid
	}
	return b, nil
}
func (p Policy) validEnvelope() bool {
	if p.Version != 1 || p.SelectionVersion != 1 {
		return false
	}
	return oneOf(p.Intent, "capture", "archival-import") && oneOf(p.Scope, "source-root", "selected") && p.Rules != nil && len(p.Rules) <= PolicyRules
}
func (p Policy) validateRules() error {
	include := false
	for _, rule := range p.Rules {
		if rule.Action != "include" && rule.Action != "exclude" {
			return ErrInvalid
		}
		if _, e := rule.Selection.Bytes(); e != nil {
			return e
		}
		include = include || rule.Action == "include"
	}
	if p.Scope == "selected" && !include {
		return ErrInvalid
	}
	return nil
}
func DecodePolicy(b []byte) (Policy, error) {
	var p Policy
	if e := decode(b, PolicyBytes, &p); e != nil {
		return p, e
	}
	canonical, e := p.Canonical()
	if e != nil || !bytes.Equal(canonical, b) {
		return p, ErrInvalid
	}
	return p, nil
}
func Digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

// IDs are opaque typed names; these records grant no publication authority.
type Record struct {
	ID                 string
	RepositoryID       string
	SourceID           string
	SourceRevision     int64
	PolicyID           string
	StartedNS, EndedNS *int64
	ScanState, Outcome string
}
type Metadata struct {
	Mode    *uint32
	MtimeNS *int64
}
type Root struct {
	GenerationID string
	Metadata     Metadata
}
type Entry struct {
	GenerationID, ID string
	ParentID         *string
	Component        []byte
	Kind, Outcome    string
	LogicalFileID    *int64
	Metadata         Metadata
}
