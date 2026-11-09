//go:build linux

package recoveryexport

// Adapted from the reviewer proposal and reproduced on unchanged submitted source.
// All destructive fixture changes target test-owned PostgreSQL data.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

const phase3aFollowupIdentityRelations = `
 FROM pg_catalog.pg_constraint c
 JOIN pg_catalog.pg_class r ON r.oid=c.conrelid
 JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
 WHERE n.nspname='public' AND r.relname IN
 ('repository_identity','storage_root_identity','device_identity',
  'source_identity','adoption_operation','legacy_snapshot_origin')`

func phase3aFollowupAdopted(t *testing.T) *fixture {
	t.Helper()
	f, options := adoptionFixture(t)
	_, err := ApplyAdoption(context.Background(), f.config, options)
	must(t, err)
	return f
}

func phase3aFollowupConstraintCounts(t *testing.T, conn *sql.DB) (rows, names int) {
	t.Helper()
	must(t, conn.QueryRow(`SELECT count(*),count(DISTINCT c.conname)`+
		phase3aFollowupIdentityRelations).Scan(&rows, &names))
	return
}

// Include the relation: the existing catalogFingerprint's constraint section
// does not include conrelid. Hash definitions on the server so this test oracle
// does not fetch the deliberately oversized A2 definition into the client.
func phase3aFollowupConstraintState(t *testing.T, conn *sql.DB) string {
	t.Helper()
	rows, err := conn.Query(`SELECT r.relname,c.conname,c.contype::text,
 c.convalidated,c.condeferrable,md5(pg_get_constraintdef(c.oid))` +
		phase3aFollowupIdentityRelations + ` ORDER BY r.relname,c.conname`)
	must(t, err)
	defer func() { must(t, rows.Close()) }()
	var result strings.Builder
	for rows.Next() {
		var relation, name, kind, digest string
		var valid, deferrable bool
		must(t, rows.Scan(&relation, &name, &kind, &valid, &deferrable, &digest))
		fmt.Fprintln(&result, relation, name, kind, valid, deferrable, digest)
	}
	must(t, rows.Err())
	return result.String()
}

func phase3aFollowupMisplaceConstraint(t *testing.T, conn *sql.DB, kind string) {
	t.Helper()
	beforeRows, beforeNames := phase3aFollowupConstraintCounts(t, conn)
	execSQL(t, conn, `ALTER TABLE public.source_identity
 DROP CONSTRAINT source_identity_repository_id_fkey`)
	switch kind {
	case "foreign-key-relocated":
		// Every expected name still appears exactly once. A seen-name set without
		// relation identity is insufficient to detect this malformed profile.
		execSQL(t, conn, `ALTER TABLE public.device_identity
 ADD CONSTRAINT source_identity_repository_id_fkey
 FOREIGN KEY (repository_id) REFERENCES public.repository_identity(repository_id)`)
	case "duplicate-check-padding":
		// CHECK names are relation-local and create no backing-index name clash.
		execSQL(t, conn, `ALTER TABLE public.device_identity
 ADD CONSTRAINT repository_identity_singleton_check CHECK (singleton=1)`)
	default:
		t.Fatalf("unknown reviewer fixture mutation %q", kind)
	}
	afterRows, afterNames := phase3aFollowupConstraintCounts(t, conn)
	t.Logf("A1 actual catalog: count=%d distinct names=%d", afterRows, afterNames)
	if afterRows != beforeRows {
		t.Fatalf("fixture must preserve constraint count: %d -> %d", beforeRows, afterRows)
	}
	wantNames := beforeNames
	if kind == "duplicate-check-padding" {
		wantNames--
	}
	if afterNames != wantNames {
		t.Fatalf("fixture distinct-name count: got %d, want %d", afterNames, wantNames)
	}
	var sourceStillConstrained bool
	must(t, conn.QueryRow(`SELECT EXISTS(SELECT FROM pg_catalog.pg_constraint
 WHERE conrelid='public.source_identity'::regclass
 AND conname='source_identity_repository_id_fkey')`).Scan(&sourceStillConstrained))
	if sourceStillConstrained {
		t.Fatal("fixture did not remove the source relation's FK")
	}
}

func TestPhase3AFollowupA1IdentityConstraintSet(t *testing.T) {
	for _, kind := range []string{"valid", "foreign-key-relocated", "duplicate-check-padding"} {
		t.Run(kind, func(t *testing.T) {
			f := phase3aFollowupAdopted(t)
			positive, positiveErr := Inspect(context.Background(), f.config, DefaultLimits(), noConsume)
			must(t, positiveErr)
			if !positive.Complete {
				t.Fatal("valid before-mutation control incomplete")
			}
			if kind != "valid" {
				phase3aFollowupMisplaceConstraint(t, f.db, kind)
			}
			catalog, tree := catalogFingerprint(t, f.db), treeFingerprint(t, f.root)
			constraints := phase3aFollowupConstraintState(t, f.db)
			t.Logf("A1 relation/name/kind/status/definition digest:\n%s", constraints)
			consumed := false
			result, err := Inspect(context.Background(), f.config, DefaultLimits(), func(_ context.Context, inv *Inventory) error {
				consumed = true
				if inv.CatalogVersion != 18 {
					t.Errorf("fixture catalog version = %d, want 18", inv.CatalogVersion)
				}
				return nil
			})
			if catalog != catalogFingerprint(t, f.db) || tree != treeFingerprint(t, f.root) ||
				constraints != phase3aFollowupConstraintState(t, f.db) {
				t.Fatal("inspection changed the logical catalog, constraints, or payload/control tree")
			}
			assertNoInventorySessions(t, f.db)
			if kind == "valid" {
				must(t, err)
				if !result.Complete || !consumed {
					t.Fatalf("valid schema18 was not admitted: %+v", result)
				}
				return
			}
			if !errors.Is(err, ErrUnsupported) || result.Complete || consumed {
				t.Fatalf("A1 malformed constraint ownership/set admitted: result=%+v consumed=%v err=%v", result, consumed, err)
			}
		})
	}
}

// The direct Inspect test covers both attacks. This companion demonstrates the
// table-identity attack at real export and post-restore import boundaries.
func TestPhase3AFollowupA1AdmissionBoundaries(t *testing.T) {
	t.Run("export-no-complete", func(t *testing.T) {
		f := phase3aFollowupAdopted(t)
		phase3aFollowupMisplaceConstraint(t, f.db, "foreign-key-relocated")
		constraints := phase3aFollowupConstraintState(t, f.db)
		options := exportOptions(t)
		result, err := exportPreserved(t, f, options, nil)
		if !errors.Is(err, ErrUnsupported) {
			t.Fatalf("A1 export must reject malformed constraints: %+v %v", result, err)
		}
		assertIncomplete(t, options, result, err)
		noMarker(t, options.Destination)
		if constraints != phase3aFollowupConstraintState(t, f.db) {
			t.Fatal("export changed constraint ownership")
		}
	})
	t.Run("import-no-ready", func(t *testing.T) {
		f := phase3aFollowupAdopted(t)
		pkg := exportOptions(t)
		_, err := Export(context.Background(), f.config, pkg)
		must(t, err)
		destination, open := importTarget(t, f)
		var restored, beforeReady bool
		h := &importHooks{event: func(stage string) error {
			if stage == "restored" {
				restored = true
				phase3aFollowupMisplaceConstraint(t, open(), "foreign-key-relocated")
			}
			if stage == "before-ready" {
				beforeReady = true
			}
			return nil
		}}
		result, err := importPreserved(t, f, destination, importOptionsFor(t, pkg), h)
		if !restored {
			t.Fatalf("test never reached actual restored-catalog admission: %v", err)
		}
		if !errors.Is(err, ErrUnsupported) || result.Ready || result.Published || beforeReady {
			t.Fatalf("A1 malformed restored catalog reached readiness: result=%+v beforeReady=%v err=%v", result, beforeReady, err)
		}
		for _, name := range []string{"IMPORT_READY", "import-ready.pending"} {
			if _, err := os.Lstat(filepath.Join(destination.PayloadRoot, ".coldkeep-control", name)); !os.IsNotExist(err) {
				t.Fatalf("malformed catalog left %s: %v", name, err)
			}
		}
	})
}

// Independent driver observation ceiling; catalog-row budgets are unchanged.
const phase3aFollowupMetadataValueCeiling = 4096
const phase3aFollowupLargeLiteralBytes = 1 << 20

type phase3aFollowupMetadataStats struct {
	queries, rows, maxValueBytes int
	definitions                  int
	maxColumn                    string
}

type phase3aFollowupMetadataProbe struct {
	mu       sync.Mutex
	stats    phase3aFollowupMetadataStats
	afterRow func(bool) // nil for pure transfer observations
}

func (p *phase3aFollowupMetadataProbe) snapshot() phase3aFollowupMetadataStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}

type phase3aFollowupProbeConnector struct {
	driver.Connector
	probe *phase3aFollowupMetadataProbe
}

func (c phase3aFollowupProbeConnector) Connect(ctx context.Context) (driver.Conn, error) {
	raw, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	conn, ok := raw.(catalogDriverConn)
	if !ok {
		return nil, errors.Join(ErrUnsupported, raw.Close())
	}
	return &phase3aFollowupProbeConn{conn, c.probe}, nil
}

type phase3aFollowupProbeConn struct {
	catalogDriverConn
	probe *phase3aFollowupMetadataProbe
}

// catalogConn.QueryContext uses Prepare + StmtQueryContext. Overriding only a
// raw connector/connection QueryContext would miss this production path.
func (c *phase3aFollowupProbeConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.catalogDriverConn.Prepare(query)
	if err != nil {
		return nil, err
	}
	lower := strings.ToLower(query)
	if !strings.Contains(lower, "pg_constraint") && !strings.Contains(lower, "pg_get_constraintdef") {
		return stmt, nil
	}
	queryContext, ok := stmt.(driver.StmtQueryContext)
	if !ok {
		return nil, errors.Join(ErrUnsupported, stmt.Close())
	}
	return &phase3aFollowupProbeStmt{stmt, queryContext, c.probe, strings.Contains(lower, "pg_get_constraintdef")}, nil
}

type phase3aFollowupProbeStmt struct {
	driver.Stmt
	queryContext driver.StmtQueryContext
	probe        *phase3aFollowupMetadataProbe
	definition   bool
}

func (s *phase3aFollowupProbeStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := s.queryContext.QueryContext(ctx, args)
	if err != nil {
		return nil, err
	}
	s.probe.mu.Lock()
	s.probe.stats.queries++
	if s.definition {
		s.probe.stats.definitions++
	}
	s.probe.mu.Unlock()
	return &phase3aFollowupProbeRows{rows, rows.Columns(), s.probe, s.definition}, nil
}

type phase3aFollowupProbeRows struct {
	driver.Rows
	columns    []string
	probe      *phase3aFollowupMetadataProbe
	definition bool
}

func (r *phase3aFollowupProbeRows) Next(dest []driver.Value) error {
	err := r.Rows.Next(dest)
	if err != nil {
		return err
	}
	if r.probe.afterRow != nil {
		r.probe.afterRow(r.definition)
	}
	r.probe.mu.Lock()
	defer r.probe.mu.Unlock()
	r.probe.stats.rows++
	for index, value := range dest {
		size := 0
		switch value := value.(type) {
		case string:
			size = len(value)
		case []byte:
			size = len(value)
		}
		if size > r.probe.stats.maxValueBytes {
			r.probe.stats.maxValueBytes = size
			r.probe.stats.maxColumn = r.columns[index]
		}
	}
	// Observe only: never truncate values, substitute rows, or inject an error.
	return nil
}

func TestPhase3AFollowupA2ConstraintMetadataBound(t *testing.T) {
	for _, kind := range []string{"valid", "oversized-existing-name", "oversized-unexpected-name", "compressed-small"} {
		t.Run(kind, func(t *testing.T) {
			f := phase3aFollowupAdopted(t)
			if kind != "valid" {
				name := "source_identity_unexpected_long_check"
				if kind == "oversized-existing-name" || kind == "compressed-small" {
					name = "source_identity_binding_check"
					execSQL(t, f.db, `ALTER TABLE public.source_identity DROP CONSTRAINT source_identity_binding_check`)
				}
				// Embed the literal in the stored CHECK. CHECK(repeat(...)) alone
				// would leave a small expression and would not exercise this bug.
				literalBytes := phase3aFollowupLargeLiteralBytes
				if kind == "compressed-small" {
					literalBytes = 16384
				}
				literal := strings.Repeat("x", literalBytes)
				execSQL(t, f.db, `ALTER TABLE public.source_identity ADD CONSTRAINT `+pqIdent(name)+
					` CHECK (binding='unbound' OR binding='`+literal+`')`)
				var definitionBytes int
				// Independent fixture oracle returns a scalar, not the large text.
				must(t, f.db.QueryRow(`SELECT octet_length(pg_get_constraintdef(oid))
 FROM pg_catalog.pg_constraint WHERE conrelid='public.source_identity'::regclass
 AND conname=$1`, name).Scan(&definitionBytes))
				if definitionBytes < literalBytes {
					t.Fatalf("fixture definition only %d bytes", definitionBytes)
				}
				var stored, logical int
				var compression sql.NullString
				must(t, f.db.QueryRow(`SELECT pg_column_size(conbin),octet_length(conbin::text),pg_column_compression(conbin) FROM pg_catalog.pg_constraint WHERE conrelid='public.source_identity'::regclass AND conname=$1`, name).Scan(&stored, &logical, &compression))
				t.Logf("A2 fixture name=%s definition bytes=%d stored=%d logical=%d compression=%v", name, definitionBytes, stored, logical, compression)
				if kind == "compressed-small" && (!compression.Valid || stored+4 > phase3aFollowupMetadataValueCeiling || logical <= phase3aFollowupMetadataValueCeiling) {
					t.Fatal("compressed-small witness did not straddle representation ceiling")
				}
			}
			var maxStored, compressed int
			must(t, f.db.QueryRow(`SELECT COALESCE(max(pg_column_size(c.conbin)),0),
 count(*) FILTER (WHERE pg_column_compression(c.conbin) IS NOT NULL)`+
				phase3aFollowupIdentityRelations).Scan(&maxStored, &compressed))
			t.Logf("A2 fixture max stored CHECK bytes=%d compressed CHECKs=%d", maxStored, compressed)
			constraints, tree := phase3aFollowupConstraintState(t, f.db), treeFingerprint(t, f.root)
			legacy := legacyRowsFingerprint(t, f.db)
			var lineage string
			must(t, f.db.QueryRow(identityLineageQuery).Scan(&lineage))
			probe := &phase3aFollowupMetadataProbe{}
			h := &hooks{wrapConnector: func(connector driver.Connector) driver.Connector {
				return phase3aFollowupProbeConnector{connector, probe}
			}}
			consumed := false
			result, err := inspect(context.Background(), f.config, DefaultLimits(), func(context.Context, *Inventory) error {
				consumed = true
				return nil
			}, h)
			stats := probe.snapshot()
			t.Logf("A2 metadata queries=%d rows=%d max returned value bytes=%d column=%q", stats.queries, stats.rows, stats.maxValueBytes, stats.maxColumn)
			if constraints != phase3aFollowupConstraintState(t, f.db) || tree != treeFingerprint(t, f.root) {
				t.Fatal("metadata inspection changed constraints or payload/control tree")
			}
			var afterLineage string
			must(t, f.db.QueryRow(identityLineageQuery).Scan(&afterLineage))
			if legacy != legacyRowsFingerprint(t, f.db) || lineage != afterLineage {
				t.Fatal("metadata inspection changed legacy or identity rows")
			}
			assertNoInventorySessions(t, f.db)
			if stats.queries == 0 || (kind == "valid" && stats.rows == 0) {
				t.Fatal("observer saw no constraint queries; review hook coverage before claiming a bound")
			}
			if stats.maxValueBytes > phase3aFollowupMetadataValueCeiling {
				t.Fatalf("A2 oversized metadata reached driver Rows.Next before Scan: got %d bytes in %q, ceiling %d", stats.maxValueBytes, stats.maxColumn, phase3aFollowupMetadataValueCeiling)
			}
			if kind == "valid" {
				must(t, err)
				if !result.Complete || !consumed {
					t.Fatalf("valid schema18 was not admitted: %+v", result)
				}
				return
			}
			if stats.definitions != 0 {
				t.Fatalf("unsafe metadata reached definition stage: %+v", stats)
			}
			if (!errors.Is(err, ErrUnsupported) && !errors.Is(err, ErrLimit)) || result.Complete || consumed {
				t.Fatalf("A2 oversized definition must refuse before consumption: result=%+v consumed=%v err=%v", result, consumed, err)
			}
		})
	}
}

var _ catalogDriverConn = (*phase3aFollowupProbeConn)(nil)
var _ driver.StmtQueryContext = (*phase3aFollowupProbeStmt)(nil)
var _ driver.Rows = (*phase3aFollowupProbeRows)(nil)
