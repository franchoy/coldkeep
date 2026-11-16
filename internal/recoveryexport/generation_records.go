package recoveryexport

import (
	"context"
	"database/sql"

	"github.com/franchoy/coldkeep/v2/internal/db"
	"github.com/franchoy/coldkeep/v2/internal/generation"
)

// This private record seam supports controlled model fixtures. It has no
// operator, capture or publication entry point. Existing identities are never
// upserted. The caller owns the transaction and must roll it back on error.
func insertGenerationPolicy(ctx context.Context, tx *sql.Tx, id, repository, source string, p generation.Policy) error {
	if !db.ValidIdentity("policy", id) || !db.ValidIdentity("repo", repository) || !db.ValidIdentity("source", source) {
		return ErrConfiguration
	}
	raw, e := p.Canonical()
	if e != nil {
		return e
	}
	_, e = tx.ExecContext(ctx, `INSERT INTO public.generation_policy(policy_id,repository_id,source_id,descriptor,descriptor_sha256) VALUES($1,$2,$3,$4,$5)`, id, repository, source, raw, generation.Digest(raw))
	return e
}
func insertGenerationRecords(ctx context.Context, tx *sql.Tx, g generation.Record, root generation.Root, entries []generation.Entry, l Limits) error {
	if ctx == nil || !l.valid() || !db.ValidIdentity("generation", g.ID) || root.GenerationID != g.ID || len(entries) > l.Rows {
		return ErrConfiguration
	}
	_, e := tx.ExecContext(ctx, `INSERT INTO public.capture_generation(generation_id,repository_id,source_id,source_revision,policy_id,policy_sha256,started_ns,ended_ns,scan_state,outcome,publication_receipt) SELECT $1,$2,$3,$4,$5,descriptor_sha256,$6,$7,$8,$9,NULL FROM public.generation_policy WHERE policy_id=$5 AND source_id=$3 AND repository_id=$2`, g.ID, g.RepositoryID, g.SourceID, g.SourceRevision, g.PolicyID, g.StartedNS, g.EndedNS, g.ScanState, g.Outcome)
	if e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, `INSERT INTO public.generation_root VALUES($1,$2,$3)`, g.ID, root.Metadata.Mode, root.Metadata.MtimeNS); e != nil {
		return e
	}
	for _, r := range entries {
		if r.GenerationID != g.ID || !db.ValidIdentity("entry", r.ID) || !generation.ValidComponent(r.Component) {
			return ErrConfiguration
		}
		if _, e = tx.ExecContext(ctx, `INSERT INTO public.generation_entry VALUES($1,$2,$3,'directory',$4,$5,$6,$7,$8,$9)`, g.ID, r.ID, r.ParentID, r.Component, r.Kind, r.Outcome, r.LogicalFileID, r.Metadata.Mode, r.Metadata.MtimeNS); e != nil {
			return e
		}
	}
	inv := &Inventory{Limits: l, Catalog: map[string][]Row{}}
	if e = readCatalog(ctx, tx, inv, nil); e != nil {
		return e
	}
	return validateGraph(ctx, inv)
}
