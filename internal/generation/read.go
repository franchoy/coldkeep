package generation

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strconv"
)

var ErrResponseLimit = errors.New("generation response exceeds caller byte limit")

const (
	PageMembers        = 100
	DefaultPageMembers = 50
	CursorBytes        = 4096
	RequestBytes       = 16384
	ResponseBytes      = 1 << 20
	ModelRecords       = "generation-model-records/v1; unpublished"
	QueryGenerations   = "generations-v1"
	QueryChildren      = "children-v1"
	QueryDetail        = "detail-v1"
	QueryEntry         = "entry-v1"
	QuerySelector      = "selector-v1"
)

// Decimal preserves every signed 64-bit value through JSON.parse consumers.
type Decimal int64

func (n Decimal) MarshalJSON() ([]byte, error) { return json.Marshal(strconv.FormatInt(int64(n), 10)) }
func (n *Decimal) UnmarshalJSON(b []byte) error {
	var s string
	if len(b) > 22 || json.Unmarshal(b, &s) != nil {
		return ErrInvalid
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || strconv.FormatInt(v, 10) != s || string(b) != `"`+s+`"` {
		return ErrInvalid
	}
	*n = Decimal(v)
	return nil
}

var identitySuffix = regexp.MustCompile(`^[0-9a-f]{32}$`)

func ValidID(kind, id string) bool {
	return len(id) == len(kind)+33 && id[:len(kind)+1] == kind+"_" && identitySuffix.MatchString(id[len(kind)+1:])
}

// Reader is valid only inside the owning callback; it exposes no SQL/resource handle.
type Reader interface {
	Read(context.Context, Request) (Response, error)
}

type Request struct {
	Version       int       `json:"version"`
	Query         string    `json:"query"`
	RepositoryID  string    `json:"repository_id"`
	SourceID      string    `json:"source_id"`
	GenerationID  string    `json:"generation_id"`
	ParentID      *string   `json:"parent_id"`
	EntryID       string    `json:"entry_id"`
	Selector      *Selector `json:"selector"`
	Limit         int       `json:"limit"`
	ResponseLimit int       `json:"response_limit"`
	Cursor        string    `json:"cursor"`
}

func (q Request) Validate() error {
	if !q.validEnvelope() || !q.validScope() {
		return ErrInvalid
	}
	switch q.Query {
	case QueryGenerations:
		return q.validateGenerations()
	case QueryChildren:
		return q.validateChildren()
	case QueryDetail, QueryEntry, QuerySelector:
		return q.validateSingle()
	default:
		return ErrInvalid
	}
}

func (q Request) validateGenerations() error {
	if q.GenerationID != "" || q.ParentID != nil || q.EntryID != "" || q.Selector != nil {
		return ErrInvalid
	}
	return nil
}
func (q Request) validateChildren() error {
	if q.EntryID != "" || q.Selector != nil {
		return ErrInvalid
	}
	return nil
}

func (q Request) validEnvelope() bool {
	if q.Version != 1 || !ValidID("repo", q.RepositoryID) || !ValidID("source", q.SourceID) {
		return false
	}
	return q.validLimits()
}
func (q Request) validLimits() bool {
	return q.Limit >= 0 && q.Limit <= PageMembers && q.ResponseLimit >= 0 && q.ResponseLimit <= ResponseBytes && len(q.Cursor) <= CursorBytes
}
func (q Request) validScope() bool {
	if q.Query != QueryGenerations && !ValidID("generation", q.GenerationID) {
		return false
	}
	return q.ParentID == nil || ValidID("entry", *q.ParentID)
}
func (q Request) validateSingle() error {
	if q.Limit != 0 || q.Cursor != "" || q.ParentID != nil {
		return ErrInvalid
	}
	if q.Query == QueryEntry {
		if !ValidID("entry", q.EntryID) || q.Selector != nil {
			return ErrInvalid
		}
	} else if q.EntryID != "" {
		return ErrInvalid
	}
	return q.validateSelector()
}
func (q Request) validateSelector() error {
	if q.Query != QuerySelector {
		if q.Selector != nil {
			return ErrInvalid
		}
		return nil
	}
	if q.Selector == nil {
		return ErrInvalid
	}
	if _, err := q.Selector.Bytes(); err != nil {
		return err
	}
	b, err := json.Marshal(q.Selector)
	if err != nil || len(b) > SelectorBytes {
		return ErrInvalid
	}
	return nil
}

func (q Request) PageLimit() int {
	if q.Limit == 0 {
		return DefaultPageMembers
	}
	return q.Limit
}

func (q Request) ResponseByteLimit() int {
	if q.ResponseLimit == 0 {
		return ResponseBytes
	}
	return q.ResponseLimit
}

type MetadataDTO struct {
	Mode    *uint32  `json:"mode"`
	MtimeNS *Decimal `json:"mtime_ns"`
}
type GenerationDTO struct {
	ID                 string   `json:"generation_id"`
	RepositoryID       string   `json:"repository_id"`
	SourceID           string   `json:"source_id"`
	SourceRevision     Decimal  `json:"source_revision"`
	PolicyID           string   `json:"policy_id"`
	PolicySHA256       string   `json:"policy_sha256"`
	StartedNS          *Decimal `json:"started_ns"`
	EndedNS            *Decimal `json:"ended_ns"`
	ScanState          string   `json:"scan_state"`
	Outcome            string   `json:"outcome"`
	PublicationReceipt *string  `json:"publication_receipt"`
}
type EntryDTO struct {
	GenerationID    string      `json:"generation_id"`
	ID              string      `json:"entry_id"`
	ParentID        *string     `json:"parent_id"`
	ComponentBase64 string      `json:"component_base64"`
	Kind            string      `json:"kind"`
	Outcome         string      `json:"outcome"`
	LogicalFileID   *Decimal    `json:"logical_file_id"`
	Metadata        MetadataDTO `json:"metadata"`
}
type DetailDTO struct {
	Generation   GenerationDTO `json:"generation"`
	Root         MetadataDTO   `json:"root"`
	PolicyBase64 string        `json:"policy_base64"`
}
type SelectionDTO struct {
	Entry    EntryDTO `json:"entry"`
	Selector Selector `json:"selector"`
}
type Response struct {
	Kind         string          `json:"kind"`
	Query        string          `json:"query"`
	RepositoryID string          `json:"repository_id"`
	SourceID     string          `json:"source_id"`
	GenerationID string          `json:"generation_id"`
	ParentID     *string         `json:"parent_id"`
	Generations  []GenerationDTO `json:"generations"`
	Entries      []EntryDTO      `json:"entries"`
	Detail       *DetailDTO      `json:"detail"`
	Selection    *SelectionDTO   `json:"selection"`
	NextCursor   string          `json:"next_cursor"`
}

func validMetadata(m MetadataDTO) bool { return m.Mode == nil || *m.Mode <= 4095 }
func validGeneration(g GenerationDTO, repo, source string) bool {
	return validGenerationIdentity(g, repo, source) && validGenerationTimes(g) && g.PublicationReceipt == nil &&
		oneOf(g.ScanState, "unknown", "pending", "partial", "enumerated") && oneOf(g.Outcome, "unknown", "incomplete", "failed")
}
func validGenerationIdentity(g GenerationDTO, repo, source string) bool {
	return ValidID("generation", g.ID) && g.RepositoryID == repo && g.SourceID == source && g.SourceRevision > 0 && ValidID("policy", g.PolicyID) && validDigest(g.PolicySHA256)
}
func validGenerationTimes(g GenerationDTO) bool {
	return (g.StartedNS == nil || *g.StartedNS >= 0) && (g.EndedNS == nil || g.StartedNS != nil && *g.EndedNS >= *g.StartedNS)
}

func validDigest(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
func oneOf(s string, values ...string) bool {
	for _, v := range values {
		if s == v {
			return true
		}
	}
	return false
}
func component(s string) ([]byte, error) {
	if len(s) > base64.StdEncoding.EncodedLen(ComponentBytes) {
		return nil, ErrInvalid
	}
	b, e := base64.StdEncoding.Strict().DecodeString(s)
	if e != nil || !ValidComponent(b) || base64.StdEncoding.EncodeToString(b) != s {
		return nil, ErrInvalid
	}
	return b, nil
}
func validEntry(e EntryDTO, gid string) bool {
	_, err := component(e.ComponentBase64)
	return err == nil && e.GenerationID == gid && ValidID("entry", e.ID) && (e.ParentID == nil || ValidID("entry", *e.ParentID)) && validMetadata(e.Metadata) && validEntryOutcome(e)
}
func validEntryOutcome(e EntryDTO) bool {
	return oneOf(e.Kind, "file", "directory") && oneOf(e.Outcome, "stored", "excluded", "missing", "unreadable", "unstable", "unsupported") &&
		((e.LogicalFileID != nil) == (e.Kind == "file" && e.Outcome == "stored")) && (e.LogicalFileID == nil || *e.LogicalFileID > 0)
}

func (r Response) Validate() error {
	if !r.validEnvelope() || !r.validCursor() {
		return ErrInvalid
	}
	switch r.Query {
	case QueryGenerations:
		return r.validateGenerations()
	case QueryChildren:
		return r.validateChildren()
	case QueryDetail:
		return r.validateDetail()
	case QueryEntry, QuerySelector:
		return r.validateSelection()
	}
	return nil
}
func (r Response) validEnvelope() bool {
	q := Request{Version: 1, Query: r.Query, RepositoryID: r.RepositoryID, SourceID: r.SourceID, GenerationID: r.GenerationID, ParentID: r.ParentID}
	if r.Query == QueryEntry && r.Selection != nil {
		q.EntryID = r.Selection.Entry.ID
	}
	if r.Query == QuerySelector && r.Selection != nil {
		q.Selector = &r.Selection.Selector
	}
	return q.Validate() == nil && r.validBounds()
}
func (r Response) validBounds() bool {
	return r.Kind == ModelRecords && len(r.Generations) <= PageMembers && len(r.Entries) <= PageMembers && len(r.NextCursor) <= CursorBytes
}
func sameParent(a, b *string) bool {
	return (a == nil) == (b == nil) && (a == nil || *a == *b)
}
func (r Response) validCursor() bool {
	if r.NextCursor == "" {
		return true
	}
	c, err := DecodeCursor(r.NextCursor)
	return err == nil && c.Query == r.Query && c.RepositoryID == r.RepositoryID && c.SourceID == r.SourceID && c.GenerationID == r.GenerationID && sameParent(c.ParentID, r.ParentID)
}
func (r Response) validateGenerations() error {
	if r.Generations == nil || r.Entries != nil || r.Detail != nil || r.Selection != nil {
		return ErrInvalid
	}
	for _, g := range r.Generations {
		if !validGeneration(g, r.RepositoryID, r.SourceID) {
			return ErrInvalid
		}
	}
	return nil
}
func (r Response) validateChildren() error {
	if r.Entries == nil || r.Generations != nil || r.Detail != nil || r.Selection != nil {
		return ErrInvalid
	}
	for _, e := range r.Entries {
		if !validEntry(e, r.GenerationID) || !sameParent(e.ParentID, r.ParentID) {
			return ErrInvalid
		}
	}
	return nil
}
func (r Response) validateDetail() error {
	if r.Detail == nil || r.Selection != nil || r.Generations != nil || r.Entries != nil || r.NextCursor != "" {
		return ErrInvalid
	}
	return r.Detail.validate(r.RepositoryID, r.SourceID, r.GenerationID)
}
func (d DetailDTO) validate(repo, source, gid string) error {
	if !validGeneration(d.Generation, repo, source) || d.Generation.ID != gid || !validMetadata(d.Root) || len(d.PolicyBase64) > base64.StdEncoding.EncodedLen(PolicyBytes) {
		return ErrInvalid
	}
	return d.validatePolicy()
}
func (d DetailDTO) validatePolicy() error {
	b, e := base64.StdEncoding.Strict().DecodeString(d.PolicyBase64)
	if e != nil || base64.StdEncoding.EncodeToString(b) != d.PolicyBase64 || Digest(b) != d.Generation.PolicySHA256 {
		return ErrInvalid
	}
	_, e = DecodePolicy(b)
	return e
}
func (r Response) validateSelection() error {
	if r.Selection == nil || r.Detail != nil || r.Generations != nil || r.Entries != nil || r.NextCursor != "" || !validEntry(r.Selection.Entry, r.GenerationID) {
		return ErrInvalid
	}
	return r.Selection.validateSelector()
}
func (s SelectionDTO) validateSelector() error {
	b, e := s.Selector.Bytes()
	if e != nil || base64.StdEncoding.EncodeToString(b[len(b)-1]) != s.Entry.ComponentBase64 {
		return ErrInvalid
	}
	return nil
}

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
