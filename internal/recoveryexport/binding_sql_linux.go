package recoveryexport

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"

	dbschema "github.com/franchoy/coldkeep/v2/db"
	"github.com/lib/pq"
)

func (s *bindingSession) commit(o BindingOptions, intent bindingIntent) (committed bool, err error) {
	cfg := s.r.pg
	cfg.Runtime = make(map[string]string, len(s.r.pg.Runtime))
	for k, v := range s.r.pg.Runtime {
		cfg.Runtime[k] = v
	}
	cfg.Runtime["default_transaction_read_only"] = "off"
	cfg.ApplicationName = "coldkeep-binding-maintenance"
	connector, e := pq.NewConnectorConfig(cfg)
	if e != nil {
		return false, ErrConfiguration
	}
	dialer := &catalogDialer{ctx: s.ctx}
	connector.Dialer(dialer)
	defer func() { err = errors.Join(err, dialer.join()) }()
	var dc driver.Connector = connector
	if s.h != nil && s.h.migration != nil && s.h.migration.wrapConnector != nil {
		dc = s.h.migration.wrapConnector(dc)
	}
	session, e := openCatalogSession(s.ctx, dc)
	if e != nil {
		return false, e
	}
	conn := sql.OpenDB(adoptionConnector{session})
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)
	defer func() {
		err = errors.Join(err, s.h.at("migration-draining", nil), session.close(conn), s.h.at("migration-closed", nil))
	}()
	tx, e := conn.BeginTx(s.ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if e != nil {
		return false, e
	}
	defer func() {
		e := tx.Rollback()
		if errors.Is(e, sql.ErrTxDone) {
			e = nil
		}
		err = errors.Join(err, e, s.h.at("migration-rollback-joined", nil))
	}()
	if _, e = tx.ExecContext(s.ctx, `LOCK TABLE public.schema_version IN ACCESS EXCLUSIVE MODE`); e != nil {
		return false, e
	}
	var count, v int
	if e = tx.QueryRowContext(s.ctx, `SELECT count(*),min(catalog_version) FROM public.schema_version`).Scan(&count, &v); e != nil || count != 1 || v != o.Request.ExpectedSchema {
		return false, errors.Join(e, ErrBindingMismatch)
	}
	if e = s.check(); e != nil {
		return false, e
	}
	if v == 18 {
		if e = s.h.at("before-ddl", tx); e != nil {
			return false, e
		}
		for _, statement := range strings.Split(dbschema.BindingPostgres, ";") {
			if strings.TrimSpace(statement) == "" {
				continue
			}
			if _, e = tx.ExecContext(s.ctx, statement); e != nil {
				return false, e
			}
			if e = s.h.at("during-ddl", tx); e != nil {
				return false, e
			}
		}
	} else {
		var revision int64
		if e = tx.QueryRowContext(s.ctx, `SELECT revision FROM public.root_binding_head WHERE root_id=$1 FOR UPDATE`, intent.Result.Descriptor.Marker.RootID).Scan(&revision); e != nil || revision != o.Request.ExpectedRevision {
			return false, errors.Join(e, ErrBindingMismatch)
		}
	}
	d := intent.Result.Descriptor
	m := d.Marker
	q := o.Request
	queries := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO public.binding_operation(operation_id,plan_sha256,request_sha256,root_id,action,from_revision,to_revision,generation,manifest_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, []any{q.OperationID, o.PlanSHA256, bindingDigest(q), m.RootID, string(q.Action), q.ExpectedRevision, m.Revision, intent.Result.Generation, intent.Result.ManifestSHA256}},
		{`INSERT INTO public.root_binding_revision VALUES($1,$2,$3,$4,$5,$6,$7)`, []any{m.RootID, m.Revision, m.InstallationID, m.Incarnation, d.RootPathBase64, bindingJSON(d.Evidence), m.OperationID}},
	}
	if v == 18 {
		queries = append(queries, struct {
			sql  string
			args []any
		}{`INSERT INTO public.root_binding_head VALUES($1,$2,$3)`, []any{m.RootID, m.Revision, bindingCapability}}, struct {
			sql  string
			args []any
		}{`UPDATE public.schema_version SET catalog_version=19`, nil})
	} else {
		queries = append(queries, struct {
			sql  string
			args []any
		}{`UPDATE public.root_binding_head SET revision=$1 WHERE root_id=$2 AND revision=$3`, []any{m.Revision, m.RootID, q.ExpectedRevision}})
	}
	for _, query := range queries {
		if _, e = tx.ExecContext(s.ctx, query.sql, query.args...); e != nil {
			return false, e
		}
		if e = s.h.at("during-data", tx); e != nil {
			return false, e
		}
	}
	if q.SourceID != "" {
		if q.Action == BindingSourceRegister {
			if _, e = tx.ExecContext(s.ctx, `INSERT INTO public.configured_source SELECT $1,$2,COALESCE(max(slot),0)+1,false,false FROM public.configured_source`, q.SourceID, m.RepositoryID); e != nil {
				return false, e
			}
		} else {
			var revision int64
			if e = tx.QueryRowContext(s.ctx, `SELECT revision FROM public.source_binding_head WHERE source_id=$1 FOR UPDATE`, q.SourceID).Scan(&revision); e != nil || revision != q.ExpectedSourceRevision {
				return false, errors.Join(e, ErrBindingMismatch)
			}
		}
		path, evidence := q.SourcePathBase64, bindingJSON(intent.SourceEvidence)
		if q.Action == BindingSourceDisable {
			if e = tx.QueryRowContext(s.ctx, `SELECT path_base64,evidence FROM public.source_binding_revision WHERE source_id=$1 AND revision=$2`, q.SourceID, q.ExpectedSourceRevision).Scan(&path, &evidence); e != nil {
				return false, e
			}
			if _, e = tx.ExecContext(s.ctx, `UPDATE public.configured_source SET disabled=true WHERE source_id=$1`, q.SourceID); e != nil {
				return false, e
			}
		}
		if _, e = tx.ExecContext(s.ctx, `INSERT INTO public.source_binding_revision VALUES($1,$2,$3,$4,$5,$6,$7)`, q.SourceID, intent.Result.SourceRevision, path, evidence, intent.Result.Continuity, intent.Result.Continuity != "same-object", q.OperationID); e != nil {
			return false, e
		}
		if q.Action == BindingSourceRegister {
			_, e = tx.ExecContext(s.ctx, `INSERT INTO public.source_binding_head VALUES($1,$2)`, q.SourceID, intent.Result.SourceRevision)
		} else {
			_, e = tx.ExecContext(s.ctx, `UPDATE public.source_binding_head SET revision=$1 WHERE source_id=$2 AND revision=$3`, intent.Result.SourceRevision, q.SourceID, q.ExpectedSourceRevision)
		}
		if e != nil {
			return false, e
		}
	}
	inv := &Inventory{Limits: o.Export.InventoryLimits, Catalog: map[string][]Row{}}
	if e = readCatalog(s.ctx, tx, inv, nil); e != nil {
		return false, e
	}
	if e = validateGraph(s.ctx, inv); e != nil {
		return false, e
	}
	if e = s.h.at("before-commit", tx); e != nil {
		return false, e
	}
	if e = s.check(); e != nil {
		return false, e
	}
	// An uncertain response is conservatively committed/uncertain until a new
	// owned reconciliation checks the operation row. It is never called rollback.
	if e = tx.Commit(); e != nil {
		return true, errors.Join(ErrBindingReconciliation, e)
	}
	return true, nil
}
