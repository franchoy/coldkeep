package recoveryexport

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"

	"github.com/franchoy/coldkeep/v2/internal/generation"
	"github.com/lib/pq"
)

const generationReadIndex = "coldkeep_generation_source_read_v1"
const generationReadIndexDDL = `CREATE INDEX coldkeep_generation_source_read_v1 ON public.capture_generation USING btree
 (repository_id COLLATE "C" pg_catalog.text_ops ASC NULLS LAST,
  source_id COLLATE "C" pg_catalog.text_ops ASC NULLS LAST,
  generation_id COLLATE "C" pg_catalog.text_ops ASC NULLS LAST)`

var ErrReadPreparationRequired = errors.New("generation read index preparation required")

// ReadViewRequest selects an already installed physical binding, never an import.
type ReadViewRequest struct {
	InstallationDirectory string
	RepositoryID, RootID  string
	ExpectedRevision      int64
}

func (q ReadViewRequest) valid() bool {
	return generation.ValidID("repo", q.RepositoryID) && generation.ValidID("root", q.RootID) && q.ExpectedRevision > 0 && q.ExpectedRevision < 20000
}

func (s *bindingSession) readDescriptor(q ReadViewRequest, i *Inventory) (BindingDescriptor, error) {
	if i.CatalogVersion != 20 {
		return BindingDescriptor{}, ErrUnsupported
	}
	if err := s.readIntentAbsent(); err != nil {
		return BindingDescriptor{}, err
	}
	return s.upgradeDescriptor(UpgradeRequest{RepositoryID: q.RepositoryID, RootID: q.RootID, ExpectedRevision: q.ExpectedRevision}, i)
}
func (s *bindingSession) readIntentAbsent() error {
	return errors.Join(bindingFileAbsent(s.installation, bindingIntentName), bindingFileAbsent(s.installation, bindingIntentName+".pending"),
		bindingFileAbsent(s.installation, bindingDescriptorName+".pending"), bindingFileAbsent(s.control, bindingMarkerName+".pending"))
}
func (s *bindingSession) checkReadDescriptor(d BindingDescriptor) error {
	if err := errors.Join(s.check(), s.readIntentAbsent()); err != nil {
		return err
	}
	var stored BindingDescriptor
	var marker BindingMarker
	if err := errors.Join(readBindingFile(s.ctx, s.installation, bindingDescriptorName, BindingDescriptorMaxBytes, &stored), readBindingFile(s.ctx, s.control, bindingMarkerName, BindingMarkerMaxBytes, &marker)); err != nil {
		return err
	}
	if stored != d || marker != d.Marker {
		return ErrBindingMismatch
	}
	return nil
}

// Only booleans return from bounded metadata. No index-expression deparser or
// caller-controlled definition is transferred. A same-name relation conflicts.
func readIndexState(ctx context.Context, tx *sql.Tx) (bool, error) {
	const query = `SELECT coalesce(c.relkind='i' AND i.indrelid='public.capture_generation'::regclass
 AND am.amname='btree' AND i.indnatts=3 AND i.indnkeyatts=3
 AND i.indisvalid AND i.indisready AND i.indislive AND NOT i.indisunique AND NOT i.indisprimary AND NOT i.indisexclusion AND NOT i.indnullsnotdistinct
 AND i.indexprs IS NULL AND i.indpred IS NULL
 AND i.indkey[0]=(SELECT attnum FROM pg_catalog.pg_attribute WHERE attrelid=i.indrelid AND attname='repository_id' AND NOT attisdropped)
 AND i.indkey[1]=(SELECT attnum FROM pg_catalog.pg_attribute WHERE attrelid=i.indrelid AND attname='source_id' AND NOT attisdropped)
 AND i.indkey[2]=(SELECT attnum FROM pg_catalog.pg_attribute WHERE attrelid=i.indrelid AND attname='generation_id' AND NOT attisdropped)
 AND i.indoption[0]=0 AND i.indoption[1]=0 AND i.indoption[2]=0
 AND i.indcollation[0]='pg_catalog."C"'::regcollation AND i.indcollation[1]='pg_catalog."C"'::regcollation AND i.indcollation[2]='pg_catalog."C"'::regcollation
 AND i.indclass[0]=op.oid AND i.indclass[1]=op.oid AND i.indclass[2]=op.oid
 AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_constraint WHERE conindid=c.oid),false)
 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 LEFT JOIN pg_catalog.pg_index i ON i.indexrelid=c.oid LEFT JOIN pg_catalog.pg_am am ON am.oid=c.relam
 LEFT JOIN pg_catalog.pg_opclass op ON op.opcname='text_ops' AND op.opcnamespace='pg_catalog'::regnamespace AND op.opcmethod=am.oid
 WHERE n.nspname='public' AND c.relname=$1`
	var valid bool
	err := tx.QueryRowContext(ctx, query, generationReadIndex).Scan(&valid)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !valid {
		return false, ErrUnsupported
	}
	return true, ctx.Err()
}

type ReadIndexResult struct {
	Complete                               bool   `json:"complete"`
	Status                                 string `json:"status"`
	Index                                  string `json:"index"`
	DefinitionSHA256                       string `json:"definition_sha256"`
	Checkpoint, Generation, ManifestSHA256 string
	Reconciled                             bool `json:"reconciled"`
}

// PrepareGenerationReadIndex makes a fresh verified checkpoint before first DDL.
// Exact-present retries reconcile current state; no durable operation history is claimed.
func PrepareGenerationReadIndex(ctx context.Context, c Config, q ReadViewRequest, o ExportOptions) (ReadIndexResult, error) {
	return prepareGenerationReadIndex(ctx, c, q, o, nil)
}
func prepareGenerationReadIndex(ctx context.Context, c Config, q ReadViewRequest, o ExportOptions, h *bindingHooks) (result ReadIndexResult, err error) {
	result = ReadIndexResult{Status: "incomplete; read index not qualified", Index: generationReadIndex, DefinitionSHA256: generation.Digest([]byte(generationReadIndexDDL))}
	if ctx == nil || !q.valid() || !o.Limits.valid() || !o.InventoryLimits.valid() {
		return result, ErrConfiguration
	}
	ctx, cancel := context.WithTimeout(ctx, min(o.Limits.Duration, o.InventoryLimits.Duration))
	defer cancel()
	s, e := openMaintenanceSession(ctx, c, q.InstallationDirectory, "", o.Destination, h)
	if e != nil {
		return result, e
	}
	defer func() {
		err = errors.Join(err, s.close())
		if err != nil {
			result.Complete = false
			result.Status = "incomplete; inspect current index state before retry"
		}
	}()
	var inv *Inventory
	var descriptor BindingDescriptor
	var present bool
	var cutHooks *hooks
	if h != nil {
		cutHooks = h.cut
	}
	cut, e := inspectOwnedCut(ctx, s.c, s.r, o.InventoryLimits, func(ctx context.Context, _ resolved, tx *sql.Tx, i *Inventory) error {
		inv = i
		descriptor, e = s.readDescriptor(q, i)
		if e != nil {
			return e
		}
		present, e = readIndexState(ctx, tx)
		return e
	}, cutHooks, ownedCut{s.r.identity, s.lease})
	if e != nil || !cut.Complete {
		return result, errors.Join(e, ErrCorrupt)
	}
	if present {
		result.Reconciled = true
		result.Complete = true
		result.Status = "complete; exact read index already present"
		return result, s.checkReadDescriptor(descriptor)
	}
	if e = h.at("before-checkpoint", nil); e != nil {
		return result, e
	}
	var eh *exportHooks
	if h != nil {
		eh = h.export
	}
	owned := ownedCut{s.r.identity, s.lease}
	checkpoint, e := exportOwned(ctx, s.c, o, eh, &s.r, &owned)
	if e != nil || !checkpoint.Complete {
		return result, errors.Join(e, ErrPackage)
	}
	result.Checkpoint = o.Destination
	result.Generation = checkpoint.Generation
	result.ManifestSHA256, e = checkBindingCheckpoint(ctx, o, 20, checkpoint.Generation, s.r.identity.Hash)
	if e != nil {
		return result, e
	}
	if e = s.checkReadDescriptor(descriptor); e != nil {
		return result, e
	}
	if e = h.at("after-checkpoint", nil); e != nil {
		return result, e
	}
	if e = s.commitReadIndex(q, o.InventoryLimits, inv, descriptor); e != nil {
		return result, e
	}
	if e = h.at("after-commit", nil); e != nil {
		return result, e
	}
	cut, e = inspectOwnedCut(ctx, s.c, s.r, o.InventoryLimits, func(ctx context.Context, _ resolved, tx *sql.Tx, after *Inventory) error {
		if upgradeCatalogDigest(after) != upgradeCatalogDigest(inv) {
			return ErrBindingMismatch
		}
		present, e = readIndexState(ctx, tx)
		if e != nil {
			return e
		}
		if !present {
			return ErrReadPreparationRequired
		}
		return s.checkReadDescriptor(descriptor)
	}, cutHooks, owned)
	if e != nil || !cut.Complete {
		return result, errors.Join(e, ErrCorrupt)
	}
	result.Complete = true
	result.Status = "complete; optional schema20 read index prepared"
	return result, s.checkReadDescriptor(descriptor)
}

func (s *bindingSession) commitReadIndex(q ReadViewRequest, l Limits, before *Inventory, d BindingDescriptor) (err error) {
	cfg := s.r.pg
	cfg.Runtime = make(map[string]string, len(s.r.pg.Runtime))
	for k, v := range s.r.pg.Runtime {
		cfg.Runtime[k] = v
	}
	cfg.Runtime["default_transaction_read_only"] = "off"
	cfg.ApplicationName = "coldkeep-generation-read-index"
	connector, e := pq.NewConnectorConfig(cfg)
	if e != nil {
		return ErrConfiguration
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
		return e
	}
	conn := sql.OpenDB(adoptionConnector{session})
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)
	defer func() {
		err = errors.Join(err, s.h.at("migration-draining", nil), session.close(conn), s.h.at("migration-closed", nil))
	}()
	tx, e := conn.BeginTx(s.ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if e != nil {
		return e
	}
	defer func() {
		e := tx.Rollback()
		if errors.Is(e, sql.ErrTxDone) {
			e = nil
		}
		err = errors.Join(err, e, s.h.at("migration-rollback-joined", nil))
	}()
	if _, e = tx.ExecContext(s.ctx, `LOCK TABLE public.capture_generation IN SHARE MODE`); e != nil {
		return e
	}
	prior := &Inventory{Limits: l, Catalog: map[string][]Row{}}
	if e = readCatalog(s.ctx, tx, prior, nil); e != nil {
		return e
	}
	if e = validateGraph(s.ctx, prior); e != nil {
		return e
	}
	if upgradeCatalogDigest(prior) != upgradeCatalogDigest(before) {
		return ErrBindingMismatch
	}
	if _, e = s.readDescriptor(q, prior); e != nil {
		return e
	}
	present, e := readIndexState(s.ctx, tx)
	if e != nil {
		return e
	}
	if present {
		return ErrBindingMismatch
	}
	if e = s.h.at("before-ddl", tx); e != nil {
		return e
	}
	if _, e = tx.ExecContext(s.ctx, generationReadIndexDDL); e != nil {
		return e
	}
	if e = s.h.at("after-ddl", tx); e != nil {
		return e
	}
	present, e = readIndexState(s.ctx, tx)
	if e != nil {
		return e
	}
	if !present {
		return ErrUnsupported
	}
	if e = s.h.at("before-commit", tx); e != nil {
		return e
	}
	if e = s.checkReadDescriptor(d); e != nil {
		return e
	}
	if e = tx.Commit(); e != nil {
		return errors.Join(ErrBindingReconciliation, e)
	}
	return nil
}
