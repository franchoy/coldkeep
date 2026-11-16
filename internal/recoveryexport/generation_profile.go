package recoveryexport

import (
	"context"
	"database/sql"
	"strings"

	"github.com/franchoy/coldkeep/v2/internal/db"
	"github.com/franchoy/coldkeep/v2/internal/generation"
)

const generationReader = "Coldkeep schema-20 generation-model-v1/recovery-only, container-major-0/1, legacy-v1, CKBL-v1, none/zstd, plain/AES-GCM"

var generationTables = []tableSpec{
	{"schema_upgrade_operation", "singleton operation_id repository_id root_id binding_revision plan_sha256 request_sha256 prior_plan generation manifest_sha256 from_version to_version", "operation_id repository_id root_id plan_sha256 request_sha256 prior_plan generation manifest_sha256", "int4 text text text int8 text text text text text int4 int4", "singleton operation_id repository_id root_id binding_revision plan_sha256 request_sha256 prior_plan generation manifest_sha256 from_version to_version"},
	{"generation_policy", "policy_id repository_id source_id descriptor descriptor_sha256", "policy_id repository_id source_id descriptor descriptor_sha256", "text text text bytea text", "policy_id repository_id source_id descriptor descriptor_sha256"},
	{"capture_generation", "generation_id repository_id source_id source_revision policy_id policy_sha256 started_ns ended_ns scan_state outcome publication_receipt", "generation_id repository_id source_id policy_id policy_sha256 scan_state outcome publication_receipt", "text text text int8 text text int8 int8 text text text", "generation_id repository_id source_id source_revision policy_id policy_sha256 scan_state outcome"},
	{"generation_root", "generation_id mode mtime_ns", "generation_id", "text int4 int8", "generation_id"},
	{"generation_entry", "generation_id entry_id parent_id parent_kind component kind outcome logical_file_id mode mtime_ns", "generation_id entry_id parent_id parent_kind component kind outcome", "text text text text bytea text text int8 int4 int8", "generation_id entry_id parent_kind component kind outcome"},
}

func validateGenerationConstraints(ctx context.Context, tx *sql.Tx) error {
	all := map[identityConstraintKey]identityConstraintSpec{}
	for _, m := range []map[identityConstraintKey]identityConstraintSpec{identityConstraints, bindingConstraints, generationConstraints} {
		for k, v := range m {
			all[k] = v
		}
	}
	names := []string{}
	for _, t := range profileTables(20)[len(catalogTables):] {
		names = append(names, "'"+t.name+"'")
	}
	if e := validateGenerationFence(ctx, tx); e != nil {
		return e
	}
	return validateExactConstraints(ctx, tx, all, ` FROM pg_catalog.pg_constraint c JOIN pg_catalog.pg_class t ON t.oid=c.conrelid JOIN pg_catalog.pg_namespace n ON n.oid=t.relnamespace WHERE n.nspname='public' AND t.relname IN (`+strings.Join(names, ",")+`)`)
}
func validateGenerationGraph(ctx context.Context, i *Inventory) error {
	if i.CatalogVersion != 20 {
		return nil
	}
	bad := func() error { return graphError("generation lineage or membership") }
	c := i.Catalog
	repo := c["repository_identity"][0].s("repository_id")
	root := c["storage_root_identity"][0].s("root_id")
	if len(c["schema_upgrade_operation"]) != 1 {
		return bad()
	}
	op := c["schema_upgrade_operation"][0]
	if op.n("singleton") != 1 || !db.ValidIdentity("upgrade", op.s("operation_id")) || op.s("repository_id") != repo || op.s("root_id") != root || op.n("binding_revision") != c["root_binding_head"][0].n("revision") || op.n("from_version") != 19 || op.n("to_version") != 20 || !safeGeneration.MatchString(op.s("generation")) {
		return bad()
	}
	for _, k := range []string{"plan_sha256", "request_sha256", "prior_plan", "manifest_sha256"} {
		if !safeDigest.MatchString(op.s(k)) {
			return bad()
		}
	}
	sources, e := strIndex(c["configured_source"], "source_id")
	if e != nil {
		return e
	}
	policies, e := strIndex(c["generation_policy"], "policy_id")
	if e != nil {
		return e
	}
	for id, p := range policies {
		if !db.ValidIdentity("policy", id) || p.s("repository_id") != repo || sources[p.s("source_id")] == nil {
			return bad()
		}
		b := p.blob("descriptor")
		if _, e = generation.DecodePolicy(b); e != nil || generation.Digest(b) != p.s("descriptor_sha256") {
			return bad()
		}
	}
	revisions := map[string]map[int64]bool{}
	for _, r := range c["source_binding_revision"] {
		id := r.s("source_id")
		if revisions[id] == nil {
			revisions[id] = map[int64]bool{}
		}
		revisions[id][r.n("revision")] = true
	}
	gens, e := strIndex(c["capture_generation"], "generation_id")
	if e != nil {
		return e
	}
	for id, g := range gens {
		p := policies[g.s("policy_id")]
		if !db.ValidIdentity("generation", id) || g.s("repository_id") != repo || p == nil || p.s("source_id") != g.s("source_id") || p.s("descriptor_sha256") != g.s("policy_sha256") || !revisions[g.s("source_id")][g.n("source_revision")] || g["publication_receipt"] != nil {
			return bad()
		}
		if !oneOf(g.s("scan_state"), "unknown", "pending", "partial", "enumerated") || !oneOf(g.s("outcome"), "unknown", "incomplete", "failed") {
			return bad()
		}
		if (g["started_ns"] != nil && g.n("started_ns") < 0) || (g["ended_ns"] != nil && (g["started_ns"] == nil || g.n("ended_ns") < g.n("started_ns"))) {
			return bad()
		}
	}
	roots, e := strIndex(c["generation_root"], "generation_id")
	if e != nil {
		return e
	}
	if len(roots) != len(gens) {
		return bad()
	}
	for id, r := range roots {
		if gens[id] == nil || !validGenerationMetadata(r) {
			return bad()
		}
	}
	entries := map[string]map[string]Row{}
	siblings := map[string]bool{}
	files, e := index(c["logical_file"], "id")
	if e != nil {
		return e
	}
	for _, r := range c["generation_entry"] {
		if e = ctx.Err(); e != nil {
			return e
		}
		g, id := r.s("generation_id"), r.s("entry_id")
		b := r.blob("component")
		if gens[g] == nil || !db.ValidIdentity("entry", id) || !generation.ValidComponent(b) || r.s("parent_kind") != "directory" || !validGenerationMetadata(r) {
			return bad()
		}
		if entries[g] == nil {
			entries[g] = map[string]Row{}
		}
		if entries[g][id] != nil {
			return bad()
		}
		entries[g][id] = r
		if r["parent_id"] != nil && !db.ValidIdentity("entry", r.s("parent_id")) {
			return bad()
		}
		key := g + "/" + r.s("parent_id") + "/" + string(b)
		if siblings[key] {
			return bad()
		}
		siblings[key] = true
		if !oneOf(r.s("kind"), "file", "directory") || !oneOf(r.s("outcome"), "stored", "excluded", "missing", "unreadable", "unstable", "unsupported") {
			return bad()
		}
		storedFile := r.s("kind") == "file" && r.s("outcome") == "stored"
		if storedFile {
			if r["logical_file_id"] == nil || files[r.n("logical_file_id")] == nil || files[r.n("logical_file_id")].s("status") != "COMPLETED" {
				return bad()
			}
		} else if r["logical_file_id"] != nil {
			return bad()
		}
	}
	// Bounded walks: at most 128 ancestors per admitted row, never recursive.
	for _, members := range entries {
		for _, r := range members {
			size := 0
			for depth := 0; ; depth++ {
				if depth >= generation.Depth {
					return bad()
				}
				b := r.blob("component")
				size += len(b)
				if depth > 0 {
					size++
				}
				if size > generation.PathBytes {
					return bad()
				}
				parent := r.s("parent_id")
				if parent == "" {
					break
				}
				r = members[parent]
				if r == nil || r.s("kind") != "directory" {
					return bad()
				}
			}
		}
	}
	return nil
}
func oneOf(value string, values ...string) bool {
	for _, v := range values {
		if value == v {
			return true
		}
	}
	return false
}
func validGenerationMetadata(r Row) bool {
	return r["mode"] == nil || (r.n("mode") >= 0 && r.n("mode") <= 4095)
}

// Count relations independently of columns, including zero-column tables.
func validateGenerationRelations(ctx context.Context, tx *sql.Tx) error {
	expected := map[string]bool{}
	for _, t := range profileTables(20) {
		expected[t.name] = true
	}
	rows, e := tx.QueryContext(ctx, `SELECT c.relname,c.relkind FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','f') LIMIT $1`, len(expected)+1)
	if e != nil {
		return e
	}
	for rows.Next() {
		var name, kind string
		if e = rows.Scan(&name, &kind); e != nil {
			break
		}
		if !expected[name] || kind != "r" {
			e = ErrUnsupported
			break
		}
		delete(expected, name)
	}
	e = joinRows(e, rows)
	if e != nil {
		return e
	}
	if len(expected) != 0 {
		return ErrUnsupported
	}
	return nil
}
