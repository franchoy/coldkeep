package recoveryexport

import (
	"context"
	"errors"
	"os"
	"testing"
)

// Exercise the repository's real v7 snapshot-path migration before establishing
// the passive baseline. It appends path_id and removes path; PostgreSQL keeps
// that harmless physical order when later migrations advance to schema 17.
func TestSupportedPostgresMigrationColumnOrder(t *testing.T) {
	f := newFixture(t)
	execSQL(t, f.db, `
ALTER TABLE schema_version RENAME COLUMN catalog_version TO version;
UPDATE schema_version SET version=7;
DROP TABLE snapshot_file;
ALTER TABLE snapshot DROP COLUMN parent_id;
CREATE TABLE snapshot_file (
 id BIGSERIAL PRIMARY KEY,
 snapshot_id TEXT NOT NULL REFERENCES snapshot(id),
 path TEXT NOT NULL CHECK (path <> ''),
 logical_file_id BIGINT NOT NULL REFERENCES logical_file(id),
 size BIGINT,
 mode BIGINT,
 mtime TIMESTAMPTZ
);
INSERT INTO snapshot(id,created_at,type) VALUES('migration-history',NOW(),'full');
INSERT INTO snapshot_file(snapshot_id,path,logical_file_id,size)
 VALUES('migration-history','history/migrated.txt',2,4);`)
	schema, err := os.ReadFile("../../db/schema_postgres.sql")
	must(t, err)
	execSQL(t, f.db, string(schema))
	execSQL(t, f.db, "GRANT SELECT ON ALL TABLES IN SCHEMA public TO "+pqIdent(f.config.User))
	var version int
	must(t, f.db.QueryRow("SELECT catalog_version FROM schema_version").Scan(&version))
	if version != 17 {
		t.Fatalf("fixture migration ended at %d", version)
	}
	var physicalColumns string
	must(t, f.db.QueryRow(`SELECT string_agg(a.attname::text,' ' ORDER BY a.attnum)
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 JOIN pg_attribute a ON a.attrelid=c.oid
 WHERE n.nspname='public' AND c.relname='snapshot_file'
 AND a.attnum>0 AND NOT a.attisdropped`).Scan(&physicalColumns))
	t.Logf("SCENARIO supported migration catalogVersion=%d physicalColumns=%s", version, physicalColumns)
	if physicalColumns != "id snapshot_id logical_file_id size mode mtime path_id" {
		t.Fatalf("fixture did not exercise historical physical order: %s", physicalColumns)
	}
	result, err := runPreserved(t, f, context.Background(), DefaultLimits(), nil, func(_ context.Context, inventory *Inventory) error {
		members := inventory.Catalog["snapshot_file"]
		paths := inventory.Catalog["snapshot_path"]
		if len(members) != 1 || members[0].s("snapshot_id") != "migration-history" || members[0].n("logical_file_id") != 2 || members[0].n("size") != 4 {
			t.Fatalf("migrated history missing: %+v", members)
		}
		if len(paths) != 1 || paths[0].s("path") != "history/migrated.txt" || members[0].n("path_id") != paths[0].n("id") || len(inventory.Objects) != 1 {
			t.Fatalf("migrated path/physical closure missing: %+v", inventory)
		}
		return nil
	})
	must(t, err)
	if !result.Complete {
		t.Fatal(result)
	}
}

// Relaxing physical order must not relax the exact named-field contract.
// Existing refusal cases separately retain extra-column and wrong-type checks.
func TestCatalogMissingOrRenamedColumn(t *testing.T) {
	for _, query := range []string{
		"ALTER TABLE logical_file DROP COLUMN original_name",
		"ALTER TABLE logical_file RENAME COLUMN original_name TO future_name",
	} {
		t.Run(query, func(t *testing.T) {
			f := newFixture(t)
			execSQL(t, f.db, query)
			consumed := false
			result, err := runPreserved(t, f, context.Background(), DefaultLimits(), nil, func(context.Context, *Inventory) error {
				consumed = true
				return nil
			})
			if !errors.Is(err, ErrUnsupported) || result.Complete || consumed {
				t.Fatalf("unsupported named fields admitted: result=%+v error=%v consumed=%v", result, err, consumed)
			}
		})
	}
}
