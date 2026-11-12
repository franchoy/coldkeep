package recoveryexport

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/franchoy/coldkeep/v2/internal/generation"
)

var (
	ErrReadViewClosed     = errors.New("generation read view closed")
	ErrReadViewBusy       = errors.New("simultaneous generation reader calls refused")
	ErrGenerationNotFound = errors.New("generation scope or record not found")
)

type ReadViewResult struct {
	Complete             bool
	Opening              time.Duration
	AdmittedRows         int
	AdmittedCatalogBytes int64
	AdmittedObjects      int
}
type generationReadHooks struct {
	binding *bindingHooks
	query   func(string, []any)
}
type generationReadView struct {
	mu               sync.Mutex
	open             bool
	done             chan struct{}
	ctx              context.Context
	cancel           context.CancelFunc
	tx               *sql.Tx
	s                *bindingSession
	descriptor       BindingDescriptor
	view, repository string
	h                *generationReadHooks
}

// WithGenerationReadView holds one admitted read-only snapshot and continuous
// installed/root/database ownership. The callback must cooperate with ctx.
// Opening deliberately includes the accepted full catalog and payload admission.
func WithGenerationReadView(ctx context.Context, c Config, q ReadViewRequest, l Limits, consume func(context.Context, generation.Reader) error) (ReadViewResult, error) {
	return withGenerationReadView(ctx, c, q, l, consume, nil)
}
func withGenerationReadView(ctx context.Context, c Config, q ReadViewRequest, l Limits, consume func(context.Context, generation.Reader) error, h *generationReadHooks) (result ReadViewResult, err error) {
	if ctx == nil || !q.valid() || !l.valid() || consume == nil {
		return result, ErrConfiguration
	}
	started := time.Now()
	ctx, cancel := context.WithTimeout(ctx, l.Duration)
	defer cancel()
	var bh *bindingHooks
	var ch *hooks
	if h != nil {
		bh = h.binding
		if bh != nil {
			ch = bh.cut
		}
	}
	s, e := openMaintenanceSession(ctx, c, q.InstallationDirectory, "", "", bh)
	if e != nil {
		return result, e
	}
	defer func() {
		err = errors.Join(err, s.close())
		if err != nil {
			result.Complete = false
		}
	}()
	if e = s.readIntentAbsent(); e != nil {
		return result, e
	}
	var descriptor BindingDescriptor
	cut, e := inspectOwnedCut(ctx, s.c, s.r, l, func(ctx context.Context, _ resolved, tx *sql.Tx, i *Inventory) error {
		var e error
		descriptor, e = s.readDescriptor(q, i)
		if e != nil {
			return e
		}
		present, e := readIndexState(ctx, tx)
		if e != nil {
			return e
		}
		if !present {
			return ErrReadPreparationRequired
		}
		id, e := bindingID("view")
		if e != nil {
			return e
		}
		viewctx, viewcancel := context.WithCancel(ctx)
		v := &generationReadView{ctx: viewctx, cancel: viewcancel, open: true, tx: tx, s: s, descriptor: descriptor, view: id[len("view_"):], repository: q.RepositoryID, h: h}
		result.Opening = time.Since(started)
		result.AdmittedRows = i.Rows
		result.AdmittedCatalogBytes = i.CatalogBytes
		result.AdmittedObjects = len(i.Objects)
		// No inventory or payload graph is retained by reader methods.
		e = func() error { defer v.close(); return consume(viewctx, v) }()
		return errors.Join(e, ctx.Err(), s.checkReadDescriptor(descriptor))
	}, ch, ownedCut{s.r.identity, s.lease})
	if e != nil || !cut.Complete {
		return result, errors.Join(e, ErrCorrupt)
	}
	if e = s.checkReadDescriptor(descriptor); e != nil {
		return result, e
	}
	result.Complete = true
	return result, nil
}

func (v *generationReadView) close() {
	v.mu.Lock()
	v.open = false
	v.cancel()
	done := v.done
	v.mu.Unlock()
	if done != nil {
		<-done
	}
}
func (v *generationReadView) Read(caller context.Context, q generation.Request) (out generation.Response, err error) {
	if caller == nil || q.Validate() != nil {
		return out, generation.ErrInvalid
	}
	var cursor generation.Cursor
	if q.Cursor != "" {
		cursor, err = generation.DecodeCursor(q.Cursor)
		if err != nil {
			return out, err
		}
		if cursor.View != v.view || cursor.Query != q.Query || cursor.RepositoryID != q.RepositoryID || cursor.SourceID != q.SourceID || cursor.GenerationID != q.GenerationID || !sameParent(cursor.ParentID, q.ParentID) {
			return out, generation.ErrInvalid
		}
	}
	v.mu.Lock()
	if !v.open || v.ctx.Err() != nil {
		v.mu.Unlock()
		return out, ErrReadViewClosed
	}
	if v.done != nil {
		v.mu.Unlock()
		return out, ErrReadViewBusy
	}
	v.done = make(chan struct{})
	done := v.done
	v.mu.Unlock()
	defer func() { v.mu.Lock(); v.done = nil; close(done); v.mu.Unlock() }()
	ctx, cancel := context.WithCancel(v.ctx)
	stop := context.AfterFunc(caller, cancel)
	defer func() { stop(); cancel() }()
	if e := errors.Join(caller.Err(), ctx.Err()); e != nil {
		return out, e
	}
	if q.RepositoryID != v.repository {
		return out, ErrGenerationNotFound
	}
	if err = v.s.checkReadDescriptor(v.descriptor); err != nil {
		return out, err
	}
	out = generation.Response{Kind: generation.ModelRecords, Query: q.Query, RepositoryID: q.RepositoryID, SourceID: q.SourceID, GenerationID: q.GenerationID, ParentID: q.ParentID}
	switch q.Query {
	case generation.QueryGenerations:
		err = v.generations(ctx, q, cursor, &out)
	case generation.QueryChildren:
		err = v.children(ctx, q, cursor, &out)
	case generation.QueryDetail:
		err = v.detail(ctx, q, &out)
	case generation.QueryEntry, generation.QuerySelector:
		err = v.selectEntry(ctx, q, &out)
	}
	if err == nil {
		err = errors.Join(ctx.Err(), caller.Err(), v.s.checkReadDescriptor(v.descriptor))
	}
	if err == nil {
		var encoded []byte
		encoded, err = generation.EncodeResponse(out)
		if err == nil && len(encoded) > q.ResponseByteLimit() {
			err = generation.ErrResponseLimit
		}
	}
	if err != nil {
		return generation.Response{}, err
	}
	return out, nil
}
func sameParent(a, b *string) bool { return a == nil && b == nil || a != nil && b != nil && *a == *b }
func (v *generationReadView) observe(query string, args []any) {
	if v.h != nil && v.h.query != nil {
		v.h.query(query, args)
	}
}
func (v *generationReadView) row(ctx context.Context, query string, args ...any) *sql.Row {
	v.observe(query, args)
	return v.tx.QueryRowContext(ctx, query, args...)
}
func (v *generationReadView) rows(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	v.observe(query, args)
	return v.tx.QueryContext(ctx, query, args...)
}

// Every text/blob result is guarded on the server before the driver receives it.
// NULL on a non-null field fails Scan. Admission and all pages share the snapshot.
const readGenerationFields = `CASE WHEN octet_length(generation_id)=43 THEN generation_id END,
 CASE WHEN octet_length(repository_id)=37 THEN repository_id END,CASE WHEN octet_length(source_id)=39 THEN source_id END,
 source_revision,CASE WHEN octet_length(policy_id)=39 THEN policy_id END,CASE WHEN octet_length(policy_sha256)=64 THEN policy_sha256 END,
 started_ns,ended_ns,CASE WHEN octet_length(scan_state)<=10 THEN scan_state END,CASE WHEN octet_length(outcome)<=10 THEN outcome END`
const readGenerationScope = ` FROM public.capture_generation WHERE repository_id COLLATE "C"=$1 COLLATE "C" AND source_id COLLATE "C"=$2 COLLATE "C"`
const readGenerationPage = `SELECT ` + readGenerationFields + readGenerationScope + ` AND generation_id COLLATE "C">$3 COLLATE "C" AND generation_id COLLATE "C"<=$4 COLLATE "C" ORDER BY generation_id COLLATE "C" LIMIT $5`
const readGenerationUpper = `SELECT CASE WHEN octet_length(generation_id)=43 THEN generation_id END` + readGenerationScope + ` ORDER BY generation_id COLLATE "C" DESC LIMIT 1`

type readScanner interface{ Scan(...any) error }

func scanGeneration(row readScanner) (g generation.GenerationDTO, err error) {
	var start, end sql.NullInt64
	err = row.Scan(&g.ID, &g.RepositoryID, &g.SourceID, &g.SourceRevision, &g.PolicyID, &g.PolicySHA256, &start, &end, &g.ScanState, &g.Outcome)
	g.StartedNS = readDecimal(start)
	g.EndedNS = readDecimal(end)
	return
}
func readDecimal(n sql.NullInt64) *generation.Decimal {
	if !n.Valid {
		return nil
	}
	v := generation.Decimal(n.Int64)
	return &v
}
func readMetadata(mode, mtime sql.NullInt64) (generation.MetadataDTO, error) {
	m := generation.MetadataDTO{MtimeNS: readDecimal(mtime)}
	if mode.Valid {
		if mode.Int64 < 0 || mode.Int64 > 4095 {
			return m, ErrCorrupt
		}
		v := uint32(mode.Int64)
		m.Mode = &v
	}
	return m, nil
}
func noRecord(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return ErrGenerationNotFound
	}
	return err
}
func (v *generationReadView) source(ctx context.Context, q generation.Request) error {
	var ok bool
	err := v.row(ctx, `SELECT true FROM public.configured_source WHERE source_id=$1`, q.SourceID).Scan(&ok)
	return noRecord(err)
}
func (v *generationReadView) scopedGeneration(ctx context.Context, q generation.Request) (generation.GenerationDTO, error) {
	g, e := scanGeneration(v.row(ctx, `SELECT `+readGenerationFields+readGenerationScope+` AND generation_id COLLATE "C"=$3 COLLATE "C"`, q.RepositoryID, q.SourceID, q.GenerationID))
	return g, noRecord(e)
}
func (v *generationReadView) generations(ctx context.Context, q generation.Request, c generation.Cursor, out *generation.Response) (err error) {
	if err = v.source(ctx, q); err != nil {
		return err
	}
	out.Generations = []generation.GenerationDTO{}
	var upper string
	err = v.row(ctx, readGenerationUpper, q.RepositoryID, q.SourceID).Scan(&upper)
	if errors.Is(err, sql.ErrNoRows) {
		if q.Cursor != "" {
			return generation.ErrInvalid
		}
		return nil
	}
	if err != nil {
		return err
	}
	if c.Upper != "" && c.Upper != upper {
		return generation.ErrInvalid
	}
	last := ""
	if q.Cursor != "" {
		last = c.Last
		probe := q
		probe.GenerationID = last
		if _, err = v.scopedGeneration(ctx, probe); err != nil {
			return err
		}
	}
	rows, err := v.rows(ctx, readGenerationPage, q.RepositoryID, q.SourceID, last, upper, q.PageLimit()+1)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	for rows.Next() {
		g, e := scanGeneration(rows)
		if e != nil {
			return e
		}
		if len(out.Generations) == q.PageLimit() {
			tail := out.Generations[len(out.Generations)-1]
			out.NextCursor, err = generation.EncodeCursor(generation.Cursor{Version: 1, Query: q.Query, View: v.view, RepositoryID: q.RepositoryID, SourceID: q.SourceID, Order: "opaque-id-C-v1", Upper: upper, Last: tail.ID})
			break
		}
		out.Generations = append(out.Generations, g)
	}
	return errors.Join(err, rows.Err(), ctx.Err())
}

const readEntryFields = `CASE WHEN octet_length(generation_id)=43 THEN generation_id END,CASE WHEN octet_length(entry_id)=38 THEN entry_id END,
 CASE WHEN parent_id IS NULL OR octet_length(parent_id)=38 THEN parent_id ELSE 'invalid' END,
 CASE WHEN octet_length(component) BETWEEN 1 AND 255 THEN component END,
 CASE WHEN octet_length(kind)<=9 THEN kind END,CASE WHEN octet_length(outcome)<=11 THEN outcome END,logical_file_id,mode,mtime_ns`

func scanEntry(row readScanner) (g generation.EntryDTO, err error) {
	var parent sql.NullString
	var raw []byte
	var file, mode, mtime sql.NullInt64
	err = row.Scan(&g.GenerationID, &g.ID, &parent, &raw, &g.Kind, &g.Outcome, &file, &mode, &mtime)
	if err != nil {
		return
	}
	if !generation.ValidComponent(raw) {
		return g, ErrCorrupt
	}
	g.ComponentBase64 = base64.StdEncoding.EncodeToString(raw)
	if parent.Valid {
		g.ParentID = &parent.String
	}
	g.LogicalFileID = readDecimal(file)
	g.Metadata, err = readMetadata(mode, mtime)
	return
}
func (v *generationReadView) entry(ctx context.Context, gid, id string) (generation.EntryDTO, error) {
	e, err := scanEntry(v.row(ctx, `SELECT `+readEntryFields+` FROM public.generation_entry WHERE generation_id=$1 AND entry_id=$2`, gid, id))
	return e, noRecord(err)
}
func childScope(parent *string) (string, []any) {
	if parent == nil {
		return `parent_id IS NULL`, nil
	}
	return `parent_id=$2`, []any{*parent}
}
func (v *generationReadView) children(ctx context.Context, q generation.Request, c generation.Cursor, out *generation.Response) (err error) {
	if _, err = v.scopedGeneration(ctx, q); err != nil {
		return err
	}
	if q.ParentID != nil {
		parent, e := v.entry(ctx, q.GenerationID, *q.ParentID)
		if e != nil {
			return e
		}
		if parent.Kind != "directory" {
			return ErrGenerationNotFound
		}
	}
	predicate, parent := childScope(q.ParentID)
	args := append([]any{q.GenerationID}, parent...)
	from := ` FROM public.generation_entry WHERE generation_id=$1 AND ` + predicate
	var upper []byte
	// Include the fixed parent in the ordering: PostgreSQL does not treat
	// IS NULL as an equality pathkey. This uses the existing composite index
	// for root children too, without scanning and sorting the entire scope.
	err = v.row(ctx, `SELECT CASE WHEN octet_length(component) BETWEEN 1 AND 255 THEN component END`+from+` ORDER BY parent_id DESC,component DESC LIMIT 1`, args...).Scan(&upper)
	out.Entries = []generation.EntryDTO{}
	if errors.Is(err, sql.ErrNoRows) {
		if q.Cursor != "" {
			return generation.ErrInvalid
		}
		return nil
	}
	if err != nil {
		return err
	}
	if !generation.ValidComponent(upper) {
		return ErrCorrupt
	}
	if q.Cursor != "" && c.Upper != base64.StdEncoding.EncodeToString(upper) {
		return generation.ErrInvalid
	}
	last := []byte{}
	if q.Cursor != "" {
		last, err = base64.StdEncoding.Strict().DecodeString(c.Last)
		if err != nil {
			return err
		}
		prior, e := v.entry(ctx, q.GenerationID, c.EntryID)
		if e != nil {
			return e
		}
		if prior.ComponentBase64 != c.Last || !sameParent(prior.ParentID, q.ParentID) {
			return generation.ErrInvalid
		}
	}
	n := len(args)
	args = append(args, last, upper, q.PageLimit()+1)
	query := `SELECT ` + readEntryFields + from + fmt.Sprintf(` AND component>$%d AND component<=$%d ORDER BY parent_id,component LIMIT $%d`, n+1, n+2, n+3)
	rows, err := v.rows(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	for rows.Next() {
		e, x := scanEntry(rows)
		if x != nil {
			return x
		}
		if len(out.Entries) == q.PageLimit() {
			tail := out.Entries[len(out.Entries)-1]
			out.NextCursor, err = generation.EncodeCursor(generation.Cursor{Version: 1, Query: q.Query, View: v.view, RepositoryID: q.RepositoryID, SourceID: q.SourceID, GenerationID: q.GenerationID, ParentID: q.ParentID, Order: "raw-component-v1", Upper: base64.StdEncoding.EncodeToString(upper), Last: tail.ComponentBase64, EntryID: tail.ID})
			break
		}
		out.Entries = append(out.Entries, e)
	}
	return errors.Join(err, rows.Err(), ctx.Err())
}
func (v *generationReadView) detail(ctx context.Context, q generation.Request, out *generation.Response) error {
	g, e := v.scopedGeneration(ctx, q)
	if e != nil {
		return e
	}
	d := &generation.DetailDTO{Generation: g}
	var mode, mtime sql.NullInt64
	e = v.row(ctx, `SELECT mode,mtime_ns FROM public.generation_root WHERE generation_id=$1`, q.GenerationID).Scan(&mode, &mtime)
	if e != nil {
		return noRecord(e)
	}
	d.Root, e = readMetadata(mode, mtime)
	if e != nil {
		return e
	}
	var raw []byte
	e = v.row(ctx, `SELECT CASE WHEN octet_length(descriptor) BETWEEN 2 AND 4096 THEN descriptor END FROM public.generation_policy WHERE policy_id=$1 AND repository_id=$2 AND source_id=$3 AND descriptor_sha256=$4`, g.PolicyID, q.RepositoryID, q.SourceID, g.PolicySHA256).Scan(&raw)
	if e != nil {
		return noRecord(e)
	}
	if _, e = generation.DecodePolicy(raw); e != nil {
		return e
	}
	d.PolicyBase64 = base64.StdEncoding.EncodeToString(raw)
	out.Detail = d
	return nil
}
func (v *generationReadView) selectEntry(ctx context.Context, q generation.Request, out *generation.Response) error {
	if _, e := v.scopedGeneration(ctx, q); e != nil {
		return e
	}
	var selected generation.EntryDTO
	var err error
	if q.Query == generation.QuerySelector {
		parts, e := q.Selector.Bytes()
		if e != nil {
			return e
		}
		var parent *string
		for _, part := range parts {
			predicate, scope := childScope(parent)
			args := append([]any{q.GenerationID}, scope...)
			args = append(args, part)
			selected, err = scanEntry(v.row(ctx, `SELECT `+readEntryFields+` FROM public.generation_entry WHERE generation_id=$1 AND `+predicate+fmt.Sprintf(` AND component=$%d`, len(args)), args...))
			if err != nil {
				return noRecord(err)
			}
			id := selected.ID
			parent = &id
		}
	} else {
		selected, err = v.entry(ctx, q.GenerationID, q.EntryID)
		if err != nil {
			return err
		}
	}
	parts := []string{selected.ComponentBase64}
	current := selected
	size := 0
	for {
		b, e := base64.StdEncoding.Strict().DecodeString(current.ComponentBase64)
		if e != nil {
			return e
		}
		size += len(b)
		if len(parts) > generation.Depth || size > generation.PathBytes {
			return ErrLimit
		}
		if current.ParentID == nil {
			break
		}
		current, err = v.entry(ctx, q.GenerationID, *current.ParentID)
		if err != nil {
			return err
		}
		if current.Kind != "directory" {
			return ErrCorrupt
		}
		parts = append(parts, current.ComponentBase64)
		size++
	}
	slices.Reverse(parts)
	selector := generation.Selector{Kind: "posix-bytes-base64", Components: parts}
	if q.Query == generation.QuerySelector {
		a, _ := q.Selector.Bytes()
		b, _ := selector.Bytes()
		if len(a) != len(b) {
			return ErrCorrupt
		}
		for i := range a {
			if !bytes.Equal(a[i], b[i]) {
				return ErrCorrupt
			}
		}
	}
	out.Selection = &generation.SelectionDTO{Entry: selected, Selector: selector}
	return nil
}
