package recoveryexport

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type identityConstraintKey struct{ table, name string }
type identityConstraintSpec struct{ kind, definition string }

// Separate from catalog-row serialization limits: <=53 admitted records, each
// with <=4096 bytes of uncompressed variable input (<=217088 bytes in total).
// Names are PostgreSQL name values (<=63 bytes); definitions never cross the
// driver boundary. The overflow observation is a bounded 54th metadata row.
const identityConstraintMetadataBytes int64 = 4096

const identityConstraintRelations = `
 FROM pg_catalog.pg_constraint c
 JOIN pg_catalog.pg_class t ON t.oid=c.conrelid
 JOIN pg_catalog.pg_namespace n ON n.oid=t.relnamespace
 WHERE n.nspname='public' AND t.relname IN
 ('repository_identity','storage_root_identity','device_identity',
  'source_identity','adoption_operation','legacy_snapshot_origin')`

const identityConstraintStatus = `c.convalidated AND NOT c.condeferrable
 AND NOT c.condeferred AND c.conislocal AND c.coninhcount=0 AND c.conparentid=0
 AND c.connoinherit=(c.contype<>'c') AND (c.conbin IS NOT NULL)=(c.contype='c')`

// Apply these functions to the original catalog Datums, before any cast or
// deparser. PostgreSQL 16 toast_datum_size reads an external value's stored
// length, not its pointer length; +4 conservatively covers varlena headers.
// Compression must be rejected separately, since stored size alone is unsafe.
// Bound all variable pg_constraint inputs used by the supported deparser.
var identityConstraintRawBytes, identityConstraintUncompressed = func() (string, string) {
	var sizes, plain []string
	for _, field := range []string{"conbin", "conkey", "confkey", "conpfeqop", "conppeqop", "conffeqop", "confdelsetcols", "conexclop"} {
		sizes = append(sizes, "COALESCE(pg_catalog.pg_column_size(c."+field+")::bigint+4,0)")
		plain = append(plain, "pg_catalog.pg_column_compression(c."+field+") IS NULL")
	}
	return "(" + strings.Join(sizes, "+") + ")", "(" + strings.Join(plain, " AND ") + ")"
}()

func validateIdentityConstraints(ctx context.Context, tx *sql.Tx) error {
	return validateExactConstraints(ctx, tx, identityConstraints, identityConstraintRelations)
}

func validateExactConstraints(ctx context.Context, tx *sql.Tx, constraints map[identityConstraintKey]identityConstraintSpec, relations string) error {
	rows, err := tx.QueryContext(ctx, `SELECT c.oid::bigint,c.conrelid::bigint,
 t.relname,c.conname,c.contype::text,(`+identityConstraintStatus+`),
 `+identityConstraintUncompressed+`,`+identityConstraintRawBytes+
		relations+` LIMIT $1`, len(constraints)+1)
	if err != nil {
		return errors.Join(ErrUnsupported, ctx.Err())
	}
	type admittedConstraint struct {
		key           identityConstraintKey
		oid, relation int64
	}
	admitted := make([]admittedConstraint, 0, len(constraints))
	seen := make(map[identityConstraintKey]bool, len(constraints))
	remaining := int64(len(constraints)) * identityConstraintMetadataBytes
	for rows.Next() {
		var item admittedConstraint
		var kind string
		var supported, uncompressed bool
		var size int64
		if err = rows.Scan(&item.oid, &item.relation, &item.key.table, &item.key.name, &kind, &supported, &uncompressed, &size); err != nil {
			break
		}
		spec, exists := constraints[item.key]
		if !exists || seen[item.key] || spec.kind != kind || !supported || len(admitted) >= len(constraints) {
			err = fmt.Errorf("%w: identity constraint set/status", ErrUnsupported)
			break
		}
		if !uncompressed || size < 0 || size > identityConstraintMetadataBytes || size > remaining {
			err = fmt.Errorf("%w: identity constraint metadata", ErrUnsupported)
			break
		}
		seen[item.key] = true
		remaining -= size
		admitted = append(admitted, item)
	}
	// Finish the physical rows/statement before starting another query. No
	// deparsing occurs until the entire exact set has passed this first stage.
	if err = joinRows(err, rows); err != nil {
		return errors.Join(err, ctx.Err())
	}
	for key := range constraints {
		if !seen[key] {
			return fmt.Errorf("%w: identity constraint set", ErrUnsupported)
		}
	}
	query := `SELECT CASE WHEN c.conrelid=$2::oid AND c.conname=$3::name
 AND c.contype::text=$4 AND (` + identityConstraintStatus + `)
 AND ` + identityConstraintUncompressed + ` AND ` + identityConstraintRawBytes + `<=$6
 THEN replace(pg_catalog.pg_get_constraintdef(c.oid,false),'REFERENCES public.','REFERENCES ')=$5
 ELSE false END FROM pg_catalog.pg_constraint c WHERE c.oid=$1::oid`
	for _, item := range admitted {
		spec := constraints[item.key]
		var matches bool
		// CASE attaches the raw-input guard to the actual row being deparsed.
		// Return only a boolean, never arbitrary definition text or diagnostics.
		if err = tx.QueryRowContext(ctx, query, item.oid, item.relation, item.key.name, spec.kind, spec.definition, identityConstraintMetadataBytes).Scan(&matches); err != nil {
			return errors.Join(ErrUnsupported, ctx.Err())
		}
		if !matches {
			return fmt.Errorf("%w: identity constraint definition", ErrUnsupported)
		}
	}
	return ctx.Err()
}

// Canonical PostgreSQL 16 definitions from db/adoption_postgres.sql. Ownership
// is explicit: a matching name on another relation cannot replace a missing FK.
var identityConstraints = map[identityConstraintKey]identityConstraintSpec{
	{"adoption_operation", "adoption_operation_device_id_fkey"}:            {"f", "FOREIGN KEY (device_id) REFERENCES device_identity(device_id)"},
	{"adoption_operation", "adoption_operation_device_id_key"}:             {"u", "UNIQUE (device_id)"},
	{"adoption_operation", "adoption_operation_from_version_check"}:        {"c", "CHECK ((from_version = 17))"},
	{"adoption_operation", "adoption_operation_generation_check"}:          {"c", "CHECK ((generation ~ '^[0-9a-f]{32}$'::text))"},
	{"adoption_operation", "adoption_operation_legacy_root_hash_check"}:    {"c", "CHECK ((legacy_root_hash ~ '^[0-9a-f]{64}$'::text))"},
	{"adoption_operation", "adoption_operation_manifest_sha256_check"}:     {"c", "CHECK ((manifest_sha256 ~ '^[0-9a-f]{64}$'::text))"},
	{"adoption_operation", "adoption_operation_operation_id_check"}:        {"c", "CHECK ((operation_id ~ '^adopt_[0-9a-f]{32}$'::text))"},
	{"adoption_operation", "adoption_operation_operation_id_key"}:          {"u", "UNIQUE (operation_id)"},
	{"adoption_operation", "adoption_operation_pkey"}:                      {"p", "PRIMARY KEY (singleton)"},
	{"adoption_operation", "adoption_operation_plan_sha256_check"}:         {"c", "CHECK ((plan_sha256 ~ '^[0-9a-f]{64}$'::text))"},
	{"adoption_operation", "adoption_operation_repository_id_fkey"}:        {"f", "FOREIGN KEY (repository_id) REFERENCES repository_identity(repository_id)"},
	{"adoption_operation", "adoption_operation_repository_id_key"}:         {"u", "UNIQUE (repository_id)"},
	{"adoption_operation", "adoption_operation_root_id_fkey"}:              {"f", "FOREIGN KEY (root_id) REFERENCES storage_root_identity(root_id)"},
	{"adoption_operation", "adoption_operation_root_id_key"}:               {"u", "UNIQUE (root_id)"},
	{"adoption_operation", "adoption_operation_singleton_check"}:           {"c", "CHECK ((singleton = 1))"},
	{"adoption_operation", "adoption_operation_source_id_fkey"}:            {"f", "FOREIGN KEY (source_id) REFERENCES source_identity(source_id)"},
	{"adoption_operation", "adoption_operation_source_id_key"}:             {"u", "UNIQUE (source_id)"},
	{"adoption_operation", "adoption_operation_status_check"}:              {"c", "CHECK ((status = 'committed'::text))"},
	{"adoption_operation", "adoption_operation_to_version_check"}:          {"c", "CHECK ((to_version = 18))"},
	{"device_identity", "device_identity_binding_check"}:                   {"c", "CHECK ((binding = 'unbound'::text))"},
	{"device_identity", "device_identity_device_id_check"}:                 {"c", "CHECK ((device_id ~ '^device_[0-9a-f]{32}$'::text))"},
	{"device_identity", "device_identity_device_id_key"}:                   {"u", "UNIQUE (device_id)"},
	{"device_identity", "device_identity_pkey"}:                            {"p", "PRIMARY KEY (singleton)"},
	{"device_identity", "device_identity_provenance_check"}:                {"c", "CHECK ((provenance = 'adopter-not-historical-capturer'::text))"},
	{"device_identity", "device_identity_repository_id_fkey"}:              {"f", "FOREIGN KEY (repository_id) REFERENCES repository_identity(repository_id)"},
	{"device_identity", "device_identity_repository_id_key"}:               {"u", "UNIQUE (repository_id)"},
	{"device_identity", "device_identity_singleton_check"}:                 {"c", "CHECK ((singleton = 1))"},
	{"legacy_snapshot_origin", "legacy_snapshot_origin_operation_id_fkey"}: {"f", "FOREIGN KEY (operation_id) REFERENCES adoption_operation(operation_id)"},
	{"legacy_snapshot_origin", "legacy_snapshot_origin_pkey"}:              {"p", "PRIMARY KEY (snapshot_id)"},
	{"legacy_snapshot_origin", "legacy_snapshot_origin_snapshot_id_fkey"}:  {"f", "FOREIGN KEY (snapshot_id) REFERENCES snapshot(id)"},
	{"legacy_snapshot_origin", "legacy_snapshot_origin_source_id_fkey"}:    {"f", "FOREIGN KEY (source_id) REFERENCES source_identity(source_id)"},
	{"repository_identity", "repository_identity_capability_check"}:        {"c", "CHECK ((capability = 'identity-lineage-v1/unbound'::text))"},
	{"repository_identity", "repository_identity_origin_check"}:            {"c", "CHECK ((origin = 'legacy-schema17'::text))"},
	{"repository_identity", "repository_identity_pkey"}:                    {"p", "PRIMARY KEY (singleton)"},
	{"repository_identity", "repository_identity_repository_id_check"}:     {"c", "CHECK ((repository_id ~ '^repo_[0-9a-f]{32}$'::text))"},
	{"repository_identity", "repository_identity_repository_id_key"}:       {"u", "UNIQUE (repository_id)"},
	{"repository_identity", "repository_identity_singleton_check"}:         {"c", "CHECK ((singleton = 1))"},
	{"source_identity", "source_identity_binding_check"}:                   {"c", "CHECK ((binding = 'unbound'::text))"},
	{"source_identity", "source_identity_enabled_check"}:                   {"c", "CHECK ((enabled = false))"},
	{"source_identity", "source_identity_pkey"}:                            {"p", "PRIMARY KEY (singleton)"},
	{"source_identity", "source_identity_provenance_check"}:                {"c", "CHECK ((provenance = 'synthetic-unknown-legacy-scope'::text))"},
	{"source_identity", "source_identity_repository_id_fkey"}:              {"f", "FOREIGN KEY (repository_id) REFERENCES repository_identity(repository_id)"},
	{"source_identity", "source_identity_repository_id_key"}:               {"u", "UNIQUE (repository_id)"},
	{"source_identity", "source_identity_singleton_check"}:                 {"c", "CHECK ((singleton = 1))"},
	{"source_identity", "source_identity_source_id_check"}:                 {"c", "CHECK ((source_id ~ '^source_[0-9a-f]{32}$'::text))"},
	{"source_identity", "source_identity_source_id_key"}:                   {"u", "UNIQUE (source_id)"},
	{"storage_root_identity", "storage_root_identity_binding_check"}:       {"c", "CHECK ((binding = 'unbound'::text))"},
	{"storage_root_identity", "storage_root_identity_pkey"}:                {"p", "PRIMARY KEY (singleton)"},
	{"storage_root_identity", "storage_root_identity_repository_id_fkey"}:  {"f", "FOREIGN KEY (repository_id) REFERENCES repository_identity(repository_id)"},
	{"storage_root_identity", "storage_root_identity_repository_id_key"}:   {"u", "UNIQUE (repository_id)"},
	{"storage_root_identity", "storage_root_identity_root_id_check"}:       {"c", "CHECK ((root_id ~ '^root_[0-9a-f]{32}$'::text))"},
	{"storage_root_identity", "storage_root_identity_root_id_key"}:         {"u", "UNIQUE (root_id)"},
	{"storage_root_identity", "storage_root_identity_singleton_check"}:     {"c", "CHECK ((singleton = 1))"},
}
