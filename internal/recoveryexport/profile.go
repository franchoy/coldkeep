package recoveryexport

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/franchoy/coldkeep/v2/internal/db"
)

var safeGeneration = regexp.MustCompile(`^[0-9a-f]{32}$`)
var safeDigest = regexp.MustCompile(`^[0-9a-f]{64}$`)

const importReader = "Coldkeep schema-17, container-major-0/1, legacy-v1, CKBL-v1, none/zstd, plain/AES-GCM"

const identityReader = "Coldkeep schema-18 identity-lineage-v1/unbound, container-major-0/1, legacy-v1, CKBL-v1, none/zstd, plain/AES-GCM"

// Profiles are exact independent contracts. Callers never infer support by a
// version range or by the development application version string.
func profileTables(version int) []tableSpec {
	switch version {
	case 17:
		return catalogTables
	case 18:
		result := append([]tableSpec(nil), catalogTables...)
		return append(result, identityTables...)
	default:
		return nil
	}
}
func profileReader(version int) string {
	switch version {
	case 17:
		return importReader
	case 18:
		return identityReader
	default:
		return ""
	}
}
func profilePackage(version int) int {
	switch version {
	case 17:
		return 1
	case 18:
		return 2
	default:
		return 0
	}
}
func profileReceipt(version int) string {
	switch version {
	case 17:
		return "coldkeep-import-ready/v1"
	case 18:
		return "coldkeep-import-ready/v2"
	default:
		return ""
	}
}

var identityTables = []tableSpec{
	{"repository_identity", "singleton repository_id capability origin", "repository_id capability origin", "int text text text", "singleton repository_id capability origin"},
	{"storage_root_identity", "singleton root_id repository_id binding", "root_id repository_id binding", "int text text text", "singleton root_id repository_id binding"},
	{"device_identity", "singleton device_id repository_id provenance binding", "device_id repository_id provenance binding", "int text text text text", "singleton device_id repository_id provenance binding"},
	{"source_identity", "singleton source_id repository_id provenance enabled binding", "source_id repository_id provenance binding", "int text text text bool text", "singleton source_id repository_id provenance enabled binding"},
	{"adoption_operation", "singleton operation_id plan_sha256 repository_id root_id device_id source_id from_version to_version generation manifest_sha256 legacy_root_hash status", "operation_id plan_sha256 repository_id root_id device_id source_id generation manifest_sha256 legacy_root_hash status", "int text text text text text text int int text text text text", "singleton operation_id plan_sha256 repository_id root_id device_id source_id from_version to_version generation manifest_sha256 legacy_root_hash status"},
	{"legacy_snapshot_origin", "snapshot_id source_id operation_id", "snapshot_id source_id operation_id", "text text text", "snapshot_id source_id operation_id"},
}

func validateIdentity(i *Inventory) error {
	if i.CatalogVersion == 17 {
		return nil
	}
	if i.CatalogVersion != 18 {
		return ErrUnsupported
	}
	for _, t := range identityTables[:5] {
		if len(i.Catalog[t.name]) != 1 || i.Catalog[t.name][0].n("singleton") != 1 {
			return graphError(t.name + " singleton")
		}
	}
	repo := i.Catalog["repository_identity"][0]
	root := i.Catalog["storage_root_identity"][0]
	device := i.Catalog["device_identity"][0]
	source := i.Catalog["source_identity"][0]
	op := i.Catalog["adoption_operation"][0]
	for _, item := range []struct {
		kind, field string
		row         Row
	}{{"repo", "repository_id", repo}, {"root", "root_id", root}, {"device", "device_id", device}, {"source", "source_id", source}, {"adopt", "operation_id", op}} {
		if !db.ValidIdentity(item.kind, item.row.s(item.field)) {
			return graphError("identity encoding")
		}
	}
	for _, r := range []Row{root, device, source, op} {
		if r.s("repository_id") != repo.s("repository_id") {
			return graphError("repository lineage")
		}
	}
	if repo.s("capability") != "identity-lineage-v1/unbound" || repo.s("origin") != "legacy-schema17" {
		return ErrUnsupported
	}
	if root.s("binding") != "unbound" || device.s("binding") != "unbound" || source.s("binding") != "unbound" || source["enabled"] != false || device.s("provenance") != "adopter-not-historical-capturer" || source.s("provenance") != "synthetic-unknown-legacy-scope" {
		return graphError("unbound provenance")
	}
	if op.s("root_id") != root.s("root_id") || op.s("device_id") != device.s("device_id") || op.s("source_id") != source.s("source_id") || op.n("from_version") != 17 || op.n("to_version") != 18 || op.s("status") != "committed" || !safeGeneration.MatchString(op.s("generation")) || !safeDigest.MatchString(op.s("manifest_sha256")) || !safeDigest.MatchString(op.s("plan_sha256")) || !safeDigest.MatchString(op.s("legacy_root_hash")) {
		return graphError("adoption checkpoint")
	}
	snaps, e := strIndex(i.Catalog["snapshot"], "id")
	if e != nil {
		return e
	}
	mapping, e := strIndex(i.Catalog["legacy_snapshot_origin"], "snapshot_id")
	if e != nil {
		return e
	}
	if len(snaps) != len(mapping) {
		return graphError("legacy snapshot coverage")
	}
	for id, r := range mapping {
		if snaps[id] == nil || r.s("source_id") != source.s("source_id") || r.s("operation_id") != op.s("operation_id") {
			return graphError("legacy snapshot lineage")
		}
	}
	return nil
}

// Identity constraints must be validated and non-deferrable. Exact definitions
// are checked against the version-specific PostgreSQL profile below.
func validateIdentityConstraints(ctx context.Context, tx *sql.Tx) error {
	rows, e := tx.QueryContext(ctx, `SELECT c.conname,c.contype::text,c.convalidated,c.condeferrable,pg_get_constraintdef(c.oid) FROM pg_constraint c JOIN pg_class t ON t.oid=c.conrelid JOIN pg_namespace n ON n.oid=t.relnamespace WHERE n.nspname='public' AND t.relname IN ('repository_identity','storage_root_identity','device_identity','source_identity','adoption_operation','legacy_snapshot_origin') ORDER BY c.conname LIMIT 101`)
	if e != nil {
		return ErrUnsupported
	}
	count := 0
	for rows.Next() {
		var name, kind, def string
		var valid, deferred bool
		if e = rows.Scan(&name, &kind, &valid, &deferred, &def); e != nil {
			break
		}
		count++
		if !valid || deferred || identityConstraints[name] != strings.ReplaceAll(def, "REFERENCES public.", "REFERENCES ") {
			e = fmt.Errorf("%w: identity constraint %s", ErrUnsupported, name)
			break
		}
	}
	e = joinRows(e, rows)
	if e == nil && count != len(identityConstraints) {
		return fmt.Errorf("%w: identity constraint set", ErrUnsupported)
	}
	return e
}
