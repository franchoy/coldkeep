package recoveryexport

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	dbschema "github.com/franchoy/coldkeep/v2/db"
)

// Exact fixed-function/trigger admission uses bounded catalog Datums before
// comparing body bytes. No function or trigger deparser output crosses the driver.
func validateGenerationFence(ctx context.Context, tx *sql.Tx) error {
	functions := map[string]string{
		"coldkeep_schema20_writer_fence": strings.Split(dbschema.GenerationFencePostgres, "$coldkeep$")[1],
		"set_updated_at":                 strings.Split(strings.Split(dbschema.PostgresSchema, "CREATE OR REPLACE FUNCTION set_updated_at()")[1], "$$")[1],
	}
	for name, body := range functions {
		var count int
		e := tx.QueryRowContext(ctx, `SELECT count(*) FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.proname=$1`, name).Scan(&count)
		if e != nil || count != 1 {
			return errors.Join(e, fmt.Errorf("%w: schema20 trigger function %s", ErrUnsupported, name))
		}
		var matches bool
		e = tx.QueryRowContext(ctx, `SELECT CASE WHEN pg_catalog.pg_column_compression(p.prosrc) IS NULL AND pg_catalog.pg_column_size(p.prosrc)+4 <= 4096 AND p.proconfig IS NULL AND p.probin IS NULL AND p.prosqlbody IS NULL THEN p.prosrc=$1 AND p.pronargs=0 AND p.prorettype='pg_catalog.trigger'::regtype AND p.prolang=(SELECT oid FROM pg_catalog.pg_language WHERE lanname='plpgsql') AND NOT p.prosecdef AND p.provolatile='v' AND p.prokind='f' ELSE false END FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.proname=$2`, body, name).Scan(&matches)
		if e != nil || !matches {
			return errors.Join(e, fmt.Errorf("%w: schema20 trigger function %s", ErrUnsupported, name))
		}
	}
	expected := map[string]int{}
	for _, t := range profileTables(20) {
		switch t.name {
		case "generation_root", "generation_entry":
			continue
		case "generation_policy", "capture_generation":
			expected[t.name+":aaaa_coldkeep_schema20_fence"] = 58
		default:
			expected[t.name+":aaaa_coldkeep_schema20_fence"] = 62
		}
	}
	expected["chunk:aaaa_coldkeep_schema20_pin_rows"] = 19
	for _, table := range []string{"container", "chunk", "logical_file", "blocks"} {
		expected[table+":trg_"+table+"_updated_at"] = 19
	}
	rows, e := tx.QueryContext(ctx, `SELECT t.relname,g.tgname,g.tgtype,g.tgenabled, g.tgisinternal, (g.tgconstraint=0 AND g.tgparentid=0 AND g.tgattr=''::int2vector AND g.tgnargs=0 AND pg_catalog.octet_length(g.tgargs)=0 AND g.tgqual IS NULL AND g.tgoldtable IS NULL AND g.tgnewtable IS NULL AND g.tgfoid=CASE WHEN g.tgname='trg_'||t.relname||'_updated_at' THEN 'public.set_updated_at()'::regprocedure ELSE 'public.coldkeep_schema20_writer_fence()'::regprocedure END) FROM pg_catalog.pg_trigger g JOIN pg_catalog.pg_class t ON t.oid=g.tgrelid JOIN pg_catalog.pg_namespace n ON n.oid=t.relnamespace WHERE n.nspname='public' AND NOT g.tgisinternal LIMIT $1`, len(expected)+1)
	if e != nil {
		return e
	}
	for rows.Next() {
		var table, name, enabled string
		var typ int
		var internal, exact bool
		if e = rows.Scan(&table, &name, &typ, &enabled, &internal, &exact); e != nil {
			break
		}
		if expected[table+":"+name] != typ || enabled != "O" || internal || !exact {
			e = fmt.Errorf("%w: schema20 trigger %s/%s type %d expected %d enabled %s exact %v", ErrUnsupported, table, name, typ, expected[table+":"+name], enabled, exact)
			break
		}
		delete(expected, table+":"+name)
	}
	if e = joinRows(e, rows); e != nil {
		return e
	}
	if len(expected) > 0 {
		return fmt.Errorf("%w: schema20 missing triggers %v", ErrUnsupported, expected)
	}
	return nil
}
