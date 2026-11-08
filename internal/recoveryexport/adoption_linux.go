package recoveryexport

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	dbschema "github.com/franchoy/coldkeep/v2/db"
	"github.com/franchoy/coldkeep/v2/internal/coordination"
	"github.com/franchoy/coldkeep/v2/internal/db"
	"github.com/lib/pq"
)

var ErrAdoptionConflict = errors.New("adoption request or reviewed source plan conflicts")
var ErrRetainedAuthority = errors.New("adoption refuses retained IMPORT_READY authority; recover into a fresh schema-17 source without outstanding reader authority")
var ErrAdoptionUncertain = errors.New("adoption commit outcome uncertain; retain checkpoint and reconcile the same request")

type AdoptionOptions struct {
	ExpectedSchema int           `json:"expected_schema"`
	OperationID    string        `json:"operation_id"`
	PlanSHA256     string        `json:"plan_sha256"`
	Export         ExportOptions `json:"export"`
}
type AdoptionResult struct {
	Status         string          `json:"status"`
	PlanSHA256     string          `json:"plan_sha256"`
	RepositoryID   db.RepositoryID `json:"repository_id,omitempty"`
	RootID         db.RootID       `json:"root_id,omitempty"`
	DeviceID       db.DeviceID     `json:"device_id,omitempty"`
	SourceID       db.SourceID     `json:"source_id,omitempty"`
	OperationID    db.AdoptionID   `json:"operation_id,omitempty"`
	Generation     string          `json:"generation,omitempty"`
	ManifestSHA256 string          `json:"manifest_sha256,omitempty"`
}
type adoptionHooks struct {
	event     func(string, *sql.Tx) error
	export    *exportHooks
	cut       *hooks
	migration *hooks
}

func (h *adoptionHooks) at(s string, tx *sql.Tx) error {
	if h != nil && h.event != nil {
		return h.event(s, tx)
	}
	return nil
}

// PreviewAdoption is passive; its digest binds explicit destination selection and
// all supported logical catalog rows/payload identities. It grants no authority.
func PreviewAdoption(ctx context.Context, c Config, l Limits) (result AdoptionResult, err error) {
	_, err = inspectCut(ctx, c, l, func(ctx context.Context, _ resolved, tx *sql.Tx, i *Inventory) error {
		if _, e := databaseRequirements(ctx, tx); e != nil {
			return e
		}
		if i.CatalogVersion != 17 {
			return ErrAdoptionConflict
		}
		if e := noRetainedAuthority(c.PayloadRoot); e != nil {
			return e
		}
		result.PlanSHA256 = adoptionPlan(c, i)
		result.Status = "preview; schema17 unchanged; identity binding remains unqualified"
		return nil
	}, nil)
	return
}
func noRetainedAuthority(root string) error {
	_, e := os.Lstat(filepath.Join(root, ".coldkeep-control", "IMPORT_READY"))
	if os.IsNotExist(e) {
		return nil
	}
	if e != nil {
		return e
	}
	return ErrRetainedAuthority
}
func adoptionPlan(c Config, i *Inventory) string {
	catalog := make(map[string][]string)
	for name, rows := range i.Catalog {
		for _, row := range rows {
			b, _ := json.Marshal(row)
			catalog[name] = append(catalog[name], string(b))
		}
		sort.Strings(catalog[name])
	}
	objects := append([]Object(nil), i.Objects...)
	sort.Slice(objects, func(a, b int) bool { return objects[a].ID < objects[b].ID })
	b, _ := json.Marshal(struct {
		Host                               string
		Port                               uint16
		Database, User, Root, KeyReference string
		Catalog                            map[string][]string
		Objects                            []Object
	}{c.Host, c.Port, c.Database, c.User, i.RootHash, c.KeyReference, catalog, objects})
	hash := sha256.Sum256(b)
	return hex.EncodeToString(hash[:])
}
func adoptionResult(i *Inventory) AdoptionResult {
	op := i.Catalog["adoption_operation"][0]
	return AdoptionResult{Status: "committed; physical binding required", PlanSHA256: op.s("plan_sha256"), RepositoryID: db.RepositoryID(op.s("repository_id")), RootID: db.RootID(op.s("root_id")), DeviceID: db.DeviceID(op.s("device_id")), SourceID: db.SourceID(op.s("source_id")), OperationID: db.AdoptionID(op.s("operation_id")), Generation: op.s("generation"), ManifestSHA256: op.s("manifest_sha256")}
}
func ApplyAdoption(ctx context.Context, c Config, o AdoptionOptions) (AdoptionResult, error) {
	return applyAdoption(ctx, c, o, nil)
}
func applyAdoption(ctx context.Context, c Config, o AdoptionOptions, h *adoptionHooks) (result AdoptionResult, err error) {
	result.Status = "incomplete; source not adopted"
	if ctx == nil || c.Backend != "postgres" || o.ExpectedSchema != 17 || !db.ValidIdentity("adopt", o.OperationID) || !safeDigest.MatchString(o.PlanSHA256) || !o.Export.Limits.valid() || !o.Export.InventoryLimits.valid() {
		return result, ErrConfiguration
	}
	ctx, cancel := context.WithTimeout(ctx, o.Export.Limits.Duration)
	defer cancel()
	r, e := resolve(ctx, c)
	if e != nil {
		return result, e
	}
	defer clear(r.key)
	lease, e := coordination.AcquireExisting(ctx, r.identity)
	if e != nil {
		return result, e
	}
	defer func() {
		err = errors.Join(err, lease.Release())
		if err != nil && result.Status == "committed; physical binding required" {
			result.Status = "committed; cleanup/acknowledgement failed; reconcile"
		}
	}()
	if e = noRetainedAuthority(r.identity.CanonicalPath); e != nil {
		return result, e
	}
	// Same database reservation as the fresh importer, retained through all cleanup.
	reservation, e := reserveImport(ctx, r)
	if e != nil {
		return result, e
	}
	defer func() { err = errors.Join(err, reservation.close()) }()
	owned := ownedCut{r.identity, lease}
	var cut *hooks
	if h != nil {
		cut = h.cut
	}
	inspect := func(consume func(context.Context, resolved, *sql.Tx, *Inventory) error) error {
		result, e := inspectOwnedCut(ctx, c, r, o.Export.InventoryLimits, consume, cut, owned)
		if e != nil {
			return e
		}
		if !result.Complete {
			return ErrCorrupt
		}
		return nil
	}
	var prior *Inventory
	if e = inspect(func(_ context.Context, _ resolved, _ *sql.Tx, i *Inventory) error { prior = i; return nil }); e != nil {
		return result, e
	}
	if prior.CatalogVersion == 18 {
		result = adoptionResult(prior)
		if string(result.OperationID) != o.OperationID || result.PlanSHA256 != o.PlanSHA256 {
			return result, ErrAdoptionConflict
		}
		return result, reservation.check(ctx)
	}
	if adoptionPlan(c, prior) != o.PlanSHA256 {
		return result, ErrAdoptionConflict
	}
	if e = h.at("before-checkpoint", nil); e != nil {
		return result, e
	}
	var eh *exportHooks
	if h != nil {
		eh = h.export
	}
	checkpoint, e := exportOwned(ctx, c, o.Export, eh, &r, &owned)
	result.Generation = checkpoint.Generation
	if e != nil || !checkpoint.Complete {
		return result, errors.Join(e, ErrPackage)
	}
	if e = h.at("checkpoint-published", nil); e != nil {
		return result, e
	}
	packageRoot, e := os.OpenRoot(o.Export.Destination)
	if e != nil {
		return result, e
	}
	manifest, raw, e := checkGeneration(ctx, packageRoot, o.Export.Limits, true)
	e = errors.Join(e, packageRoot.Close())
	if e != nil {
		return result, e
	}
	if manifest.CatalogVersion != 17 || manifest.Generation != checkpoint.Generation || manifest.LegacyRootHash != r.identity.Hash {
		return result, ErrPackage
	}
	sum := sha256.Sum256(raw)
	result.ManifestSHA256 = hex.EncodeToString(sum[:])
	if e = reservation.check(ctx); e != nil {
		return result, e
	}
	if e = h.at("checkpoint-checked", nil); e != nil {
		return result, e
	}
	// Recheck the exact current logical cut while retaining both ownership domains.
	if e = inspect(func(_ context.Context, _ resolved, _ *sql.Tx, i *Inventory) error {
		if i.CatalogVersion != 17 || adoptionPlan(c, i) != o.PlanSHA256 {
			return ErrAdoptionConflict
		}
		return nil
	}); e != nil {
		return result, e
	}
	if e = noRetainedAuthority(r.identity.CanonicalPath); e != nil {
		return result, e
	}
	result, e = migrateAdoption(ctx, r, o, result, h)
	if e != nil {
		return result, e
	}
	if e = h.at("after-commit", nil); e != nil {
		return result, errors.Join(ErrAdoptionUncertain, e)
	}
	if e = inspect(func(_ context.Context, _ resolved, _ *sql.Tx, i *Inventory) error {
		if i.CatalogVersion != 18 {
			return ErrCorrupt
		}
		result = adoptionResult(i)
		return nil
	}); e != nil {
		return result, e
	}
	return result, errors.Join(reservation.check(ctx), lease.Check())
}

// Only this explicit adapter supplies ExecContext. Passive catalog connections
// keep their existing direct-query-only contract and physical-close join.
type adoptionConnector struct{ *catalogSession }

func (c adoptionConnector) Connect(ctx context.Context) (driver.Conn, error) {
	raw, e := c.catalogSession.Connect(ctx)
	if e != nil {
		return nil, e
	}
	conn, ok := raw.(*catalogConn)
	if !ok {
		return nil, errors.Join(ErrUnsupported, raw.Close())
	}
	return &adoptionDriver{conn}, nil
}

type adoptionDriver struct{ *catalogConn }

func (c *adoptionDriver) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	stmt, e := c.catalogDriverConn.Prepare(q)
	if e != nil {
		return nil, e
	}
	exec, ok := stmt.(driver.StmtExecContext)
	if !ok {
		return nil, errors.Join(ErrUnsupported, stmt.Close())
	}
	result, e := exec.ExecContext(ctx, args)
	return result, errors.Join(e, stmt.Close())
}
func migrateAdoption(ctx context.Context, r resolved, o AdoptionOptions, result AdoptionResult, h *adoptionHooks) (_ AdoptionResult, err error) {
	cfg := r.pg
	cfg.Runtime = make(map[string]string, len(r.pg.Runtime))
	for k, v := range r.pg.Runtime {
		cfg.Runtime[k] = v
	}
	cfg.Runtime["default_transaction_read_only"] = "off"
	cfg.ApplicationName = "coldkeep-identity-adoption"
	connector, e := pq.NewConnectorConfig(cfg)
	if e != nil {
		return result, ErrConfiguration
	}
	dialer := &catalogDialer{ctx: ctx}
	connector.Dialer(dialer)
	defer func() { err = errors.Join(err, dialer.join()) }()
	var dc driver.Connector = connector
	if h != nil && h.migration != nil && h.migration.wrapConnector != nil {
		dc = h.migration.wrapConnector(dc)
	}
	session, e := openCatalogSession(ctx, dc)
	if e != nil {
		return result, e
	}
	conn := sql.OpenDB(adoptionConnector{session})
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)
	defer func() {
		err = errors.Join(err, h.at("migration-draining", nil), session.close(conn), h.at("migration-closed", nil))
	}()
	tx, e := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if e != nil {
		return result, e
	}
	defer func() {
		e := tx.Rollback()
		if errors.Is(e, sql.ErrTxDone) {
			e = nil
		}
		err = errors.Join(err, e, h.at("migration-rollback-joined", nil))
	}()
	// The legacy metadata table lock and exact version check serialize commit even
	// across different root configurations. The reservation covers the earlier cut.
	if _, e = tx.ExecContext(ctx, `LOCK TABLE public.schema_version IN ACCESS EXCLUSIVE MODE`); e != nil {
		return result, e
	}
	var v, count int
	if e = tx.QueryRowContext(ctx, `SELECT count(*),min(catalog_version) FROM public.schema_version`).Scan(&count, &v); e != nil || count != 1 || v != 17 {
		return result, errors.Join(e, ErrAdoptionConflict)
	}
	if e = h.at("before-ddl", tx); e != nil {
		return result, e
	}
	for _, statement := range strings.Split(dbschema.AdoptionPostgres, ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, e = tx.ExecContext(ctx, statement); e != nil {
			return result, e
		}
		if e = h.at("during-ddl", tx); e != nil {
			return result, e
		}
	}
	ids := make([]string, 4)
	for k, kind := range []string{"repo", "root", "device", "source"} {
		ids[k], e = db.NewIdentity(kind)
		if e != nil {
			return result, e
		}
	}
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO public.repository_identity VALUES(1,$1,'identity-lineage-v1/unbound','legacy-schema17')`, []any{ids[0]}},
		{`INSERT INTO public.storage_root_identity VALUES(1,$1,$2,'unbound')`, []any{ids[1], ids[0]}},
		{`INSERT INTO public.device_identity VALUES(1,$1,$2,'adopter-not-historical-capturer','unbound')`, []any{ids[2], ids[0]}},
		{`INSERT INTO public.source_identity VALUES(1,$1,$2,'synthetic-unknown-legacy-scope',false,'unbound')`, []any{ids[3], ids[0]}},
		{`INSERT INTO public.adoption_operation VALUES(1,$1,$2,$3,$4,$5,$6,17,18,$7,$8,$9,'committed')`, []any{o.OperationID, o.PlanSHA256, ids[0], ids[1], ids[2], ids[3], result.Generation, result.ManifestSHA256, r.identity.Hash}},
		{`INSERT INTO public.legacy_snapshot_origin SELECT id,$1,$2 FROM public.snapshot`, []any{ids[3], o.OperationID}},
		{`UPDATE public.schema_version SET catalog_version=18`, nil},
	} {
		if _, e = tx.ExecContext(ctx, q.sql, q.args...); e != nil {
			return result, e
		}
		if e = h.at("during-data", tx); e != nil {
			return result, e
		}
	}
	inv := &Inventory{Limits: o.Export.InventoryLimits, Catalog: make(map[string][]Row)}
	if e = readCatalog(ctx, tx, inv, nil); e != nil {
		return result, e
	}
	if e = validateGraph(ctx, inv); e != nil {
		return result, e
	}
	if e = h.at("before-commit", tx); e != nil {
		return result, e
	}
	result = adoptionResult(inv)
	if e = tx.Commit(); e != nil {
		result.Status = "commit uncertain; reconcile same request"
		return result, errors.Join(ErrAdoptionUncertain, e)
	}
	return result, nil
}

// ValidateRestoreOwned validates the actual full catalog/payload tuple through
// joined passive sessions while retaining the caller's existing root lease.
// No newly opened SQL transaction can outlive this admission boundary.
func ValidateRestoreOwned(ctx context.Context, c Config, lease coordination.ExistingLease, receipt ImportReceipt) (err error) {
	r, e := resolve(ctx, c)
	if e != nil {
		return e
	}
	defer clear(r.key)
	if receipt.Profile != c.Profile || !safeGeneration.MatchString(receipt.Generation) || !safeDigest.MatchString(receipt.ManifestSHA256) || !safeDigest.MatchString(receipt.LegacyRootObservation) || receipt.TargetRootHash != r.identity.Hash || receipt.Database != c.Database || receipt.Role != c.User || receipt.Host != c.Host || receipt.Port != c.Port {
		return ErrConfiguration
	}
	result, e := inspectOwnedCut(ctx, c, r, DefaultLimits(), func(_ context.Context, _ resolved, _ *sql.Tx, inv *Inventory) error {
		if receipt.Kind != profileReceipt(inv.CatalogVersion) || receipt.Reader != profileReader(inv.CatalogVersion) {
			return fmt.Errorf("%w: receipt/catalog tuple", ErrUnsupported)
		}
		if inv.CatalogVersion == 17 && receipt.RepositoryID != "" {
			return ErrCorrupt
		}
		if inv.CatalogVersion == 18 && receipt.RepositoryID != inv.Catalog["repository_identity"][0].s("repository_id") {
			return ErrCorrupt
		}
		return nil
	}, nil, ownedCut{r.identity, lease})
	if e != nil {
		return e
	}
	if !result.Complete {
		return ErrCorrupt
	}
	return nil
}
