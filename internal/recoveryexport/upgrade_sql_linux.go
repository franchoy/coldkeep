package recoveryexport

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"

	dbschema "github.com/franchoy/coldkeep/v2/db"
	"github.com/lib/pq"
)

func (s *bindingSession) commitUpgrade(o UpgradeOptions, intent upgradeIntent) (committed bool, err error) {
	cfg := s.r.pg
	cfg.Runtime = make(map[string]string, len(s.r.pg.Runtime))
	for k, v := range s.r.pg.Runtime {
		cfg.Runtime[k] = v
	}
	cfg.Runtime["default_transaction_read_only"] = "off"
	cfg.ApplicationName = "coldkeep-schema-upgrade"
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
	var revision int64
	if e = tx.QueryRowContext(s.ctx, `SELECT revision FROM public.root_binding_head WHERE root_id=$1 FOR UPDATE`, o.Request.RootID).Scan(&revision); e != nil || revision != o.Request.ExpectedRevision {
		return false, errors.Join(e, ErrBindingMismatch)
	}
	prior := &Inventory{Limits: o.Export.InventoryLimits, Catalog: map[string][]Row{}}
	if e = readCatalog(s.ctx, tx, prior, nil); e != nil {
		return false, e
	}
	if e = validateGraph(s.ctx, prior); e != nil {
		return false, e
	}
	if upgradeCatalogDigest(prior) != intent.CatalogSHA256 {
		return false, ErrBindingMismatch
	}
	if e = s.h.at("before-ddl", tx); e != nil {
		return false, e
	}
	for _, statement := range strings.Split(dbschema.GenerationPostgres, ";") {
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
	q := o.Request
	if _, e = tx.ExecContext(s.ctx, `INSERT INTO public.schema_upgrade_operation VALUES(1,$1,$2,$3,$4,$5,$6,$7,$8,$9,19,20)`, q.OperationID, q.RepositoryID, q.RootID, q.ExpectedRevision, o.PlanSHA256, bindingDigest(q), intent.PriorPlan, intent.Result.Generation, intent.Result.ManifestSHA256); e != nil {
		return false, e
	}
	if e = s.h.at("during-data", tx); e != nil {
		return false, e
	}
	if _, e = tx.ExecContext(s.ctx, `UPDATE public.schema_version SET catalog_version=20`); e != nil {
		return false, e
	}
	// Keep the dollar-quoted function body intact; the owned driver prepares
	// exactly one SQL statement per execution.
	fence := strings.SplitN(dbschema.GenerationFencePostgres, "$coldkeep$;", 2)
	statements := append([]string{fence[0] + "$coldkeep$;"}, strings.Split(fence[1], ";")...)
	for _, statement := range statements {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, e = tx.ExecContext(s.ctx, statement); e != nil {
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
