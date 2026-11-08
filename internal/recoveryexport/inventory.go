// Package recoveryexport establishes owned full-catalog cuts and independent
// recovery copies. Inspect remains diagnostic-only; Export publishes after cleanup.
package recoveryexport

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"github.com/franchoy/coldkeep/v2/internal/coordination"
	"github.com/lib/pq"
)

var (
	ErrConfiguration  = errors.New("explicit compatible configuration required")
	ErrUnsupported    = errors.New("unsupported catalog or payload requirement")
	ErrCorrupt        = errors.New("inconsistent catalog or payload")
	ErrKeyUnavailable = errors.New("required key unavailable; recover the existing key")
	ErrLimit          = errors.New("inventory capacity exceeded")
)

// Config is an explicit operation input, never an ambient database selection.
// Password and Key resolve once before adapter construction. A nil Key is valid
// for plaintext data only. No secrets, DSNs or persistent origin IDs are reported.
// This unit supports a local PostgreSQL 16 catalog with explicit plaintext
// transport on loopback or a local Unix socket; SQLite opening is not included.
type Config struct {
	Backend, Profile, PayloadRoot string
	Host                          string
	Port                          uint16
	Database, User, Transport     string
	Password                      func(context.Context) (string, error)
	KeyReference                  string
	Key                           func(context.Context) ([]byte, error)
}

// Limits are hard ceilings; callers may lower them, but cannot raise them.
// The row and byte ceilings apply during database iteration, not after loading.
type Limits struct {
	Rows                                                                     int
	CatalogBytes, BlockBytes, ObjectBytes, TotalObjectBytes, DiagnosticBytes int64
	DirectoryEntries                                                         int
	Duration                                                                 time.Duration
}

func DefaultLimits() Limits {
	return Limits{Rows: 20000, CatalogBytes: 16 << 20, BlockBytes: 8 << 20,
		ObjectBytes: 1 << 30, TotalObjectBytes: 4 << 30, DiagnosticBytes: 32 << 20,
		DirectoryEntries: 20000, Duration: 2 * time.Minute}
}
func (l Limits) valid() bool {
	d := DefaultLimits()
	return l.Rows > 0 && l.Rows <= d.Rows && l.CatalogBytes > 0 && l.CatalogBytes <= d.CatalogBytes &&
		l.BlockBytes > 0 && l.BlockBytes <= d.BlockBytes && l.ObjectBytes > 0 && l.ObjectBytes <= d.ObjectBytes &&
		l.TotalObjectBytes > 0 && l.TotalObjectBytes <= d.TotalObjectBytes &&
		l.DiagnosticBytes > 0 && l.DiagnosticBytes <= d.DiagnosticBytes &&
		l.DirectoryEntries > 0 && l.DirectoryEntries <= d.DirectoryEntries && l.Duration > 0 && l.Duration <= d.Duration
}

// Row contains bounded catalog metadata, including relationships outside the
// current-file/GC view. Bytea fields use PostgreSQL's hex representation.
type Row map[string]any

type Object struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	Size        int64  `json:"size"`
	RequiredEnd int64  `json:"required_end"`
	SHA256      string `json:"sha256"`
	FormatMajor uint16 `json:"format_major"`
}

type Inventory struct {
	Kind           string           `json:"kind"`
	Backend        string           `json:"backend"`
	Profile        string           `json:"profile"`
	CatalogVersion int              `json:"catalog_version"`
	RootHash       string           `json:"canonical_root_hash"`
	KeyReference   string           `json:"key_reference,omitempty"`
	KeyRequired    bool             `json:"key_required"`
	Reader         string           `json:"reader"`
	Checks         []string         `json:"checks"`
	Catalog        map[string][]Row `json:"catalog"`
	Objects        []Object         `json:"whole_objects"`
	Uncataloged    []string         `json:"uncataloged_excluded"`
	Rows           int              `json:"rows"`
	CatalogBytes   int64            `json:"catalog_bytes"`
	Limits         Limits           `json:"limits"`
}

type Result struct {
	Complete       bool  `json:"complete"`
	DiagnosticOnly bool  `json:"diagnostic_only"`
	Rows           int   `json:"rows"`
	Objects        int   `json:"objects"`
	CatalogBytes   int64 `json:"catalog_bytes"`
}

type resolved struct {
	identity        coordination.Identity
	connector       *pq.Connector
	pg              pq.Config
	key             []byte
	keyRef, profile string
}

func resolve(ctx context.Context, c Config) (resolved, error) {
	var r resolved
	if c.Backend != "postgres" || c.Profile != "manual-postgresql-docker" || c.Transport != "local-plaintext" ||
		c.Port == 0 || c.Database == "" || c.User == "" || c.Password == nil || len(c.KeyReference) > 128 {
		return r, ErrConfiguration
	}
	ip := net.ParseIP(c.Host)
	if (ip == nil || !ip.IsLoopback()) && !strings.HasPrefix(c.Host, "/") {
		return r, ErrConfiguration
	}
	for _, s := range []string{c.Host, c.Database, c.User, c.PayloadRoot, c.KeyReference} {
		if len(s) > 4096 || strings.ContainsRune(s, 0) {
			return r, ErrConfiguration
		}
	}
	id, err := coordination.ResolveIdentity(c.PayloadRoot)
	if err != nil {
		return r, ErrConfiguration
	}
	info, err := os.Stat(id.CanonicalPath)
	if err != nil || !info.IsDir() {
		return r, ErrConfiguration
	}
	password, err := c.Password(ctx)
	if err != nil || len(password) > 4096 {
		return r, ErrConfiguration
	}
	var key []byte
	if c.Key != nil {
		k, e := c.Key(ctx)
		if e != nil {
			return r, ErrKeyUnavailable
		}
		key = append([]byte(nil), k...)
		if (len(key) != 16 && len(key) != 24 && len(key) != 32) || c.KeyReference == "" {
			clear(key)
			return r, ErrKeyUnavailable
		}
	}
	// Resolve scoped secrets before checking the environment: a resolver must
	// not introduce an alternate host, options or credential file either.
	for _, e := range os.Environ() {
		if strings.HasPrefix(e, "PG") || strings.HasPrefix(e, "PQGO_DEBUG=") {
			clear(key)
			return r, ErrConfiguration
		}
	}
	quote := func(s string) string { return "'" + strings.NewReplacer("\\", "\\\\", "'", "\\'").Replace(s) + "'" }
	dsn := "host=" + quote(c.Host) + " port=" + fmt.Sprint(c.Port) + " dbname=" + quote(c.Database) + " user=" + quote(c.User) +
		" password=" + quote(password) + " sslmode=disable connect_timeout=5 application_name=coldkeep-passive-inventory" +
		" default_transaction_read_only=on row_security=off search_path=pg_catalog statement_timeout=10000 lock_timeout=1000 bytea_output=hex"
	config, err := pq.NewConfig(dsn)
	// Validate the parsed destination too, so an environment change racing
	// resolution cannot redirect the connection via hostaddr or multiple hosts.
	// Retain pq's explicit-password flag, preventing a later .pgpass lookup.
	if err != nil || config.Host != c.Host || config.Port != c.Port || config.Database != c.Database ||
		config.User != c.User || config.Password != password || config.Hostaddr.IsValid() || len(config.Multi) != 0 ||
		config.Options != "" || config.Passfile != "" || config.SSLMode != pq.SSLModeDisable {
		clear(key)
		return r, ErrConfiguration
	}
	connector, err := pq.NewConnectorConfig(config)
	if err != nil {
		clear(key)
		return r, ErrConfiguration
	}
	return resolved{identity: id, connector: connector, pg: config, key: key, keyRef: c.KeyReference, profile: c.Profile}, nil
}

// Inspect holds the legacy lease and one REPEATABLE READ READ ONLY snapshot
// through synchronous consumption. It opens no writers, pins, migration or
// recovery runtime. consume must return on context cancellation and must finish
// its work before returning. No DB/transaction/file handles or producers escape.
// Retained Inventory values are diagnostics only; exporting later requires a new
// owned cut. On any failure Result.Complete is false, including cleanup failure.
func Inspect(ctx context.Context, c Config, limits Limits, consume func(context.Context, *Inventory) error) (result Result, err error) {
	return inspect(ctx, c, limits, consume, nil)
}

// hooks are package-private failure/lifetime instrumentation, never configuration.
type hooks struct {
	event         func(string, *sql.Tx) error
	connector     driver.Connector
	wrapConnector func(driver.Connector) driver.Connector
}

func (h *hooks) at(s string, tx *sql.Tx) error {
	if h != nil && h.event != nil {
		return h.event(s, tx)
	}
	return nil
}

func inspect(ctx context.Context, c Config, l Limits, consume func(context.Context, *Inventory) error, h *hooks) (Result, error) {
	if consume == nil {
		return Result{DiagnosticOnly: true}, ErrConfiguration
	}
	return inspectCut(ctx, c, l, func(ctx context.Context, _ resolved, _ *sql.Tx, inv *Inventory) error { return consume(ctx, inv) }, h)
}

// inspectCut is private: no transaction, configuration, or lease capability escapes.
func inspectCut(ctx context.Context, c Config, l Limits, consume func(context.Context, resolved, *sql.Tx, *Inventory) error, h *hooks) (result Result, err error) {
	result.DiagnosticOnly = true
	if ctx == nil || consume == nil || !l.valid() {
		return result, ErrConfiguration
	}
	ctx, cancel := context.WithTimeout(ctx, l.Duration)
	defer cancel()
	if err = ctx.Err(); err != nil {
		return
	}
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
		if err != nil {
			result.Complete = false
		}
	}()
	if err = h.at("acquired", nil); err != nil {
		return
	}
	return inspectOwnedCut(ctx, c, r, l, consume, h, ownedCut{r.identity, lease})
}

// ownedCut binds continuous ownership to the exact root. Active sessions join
// inside inspectOwnedCut; callers retain the lease through their handoff.
type ownedCut struct {
	identity coordination.Identity
	lease    coordination.ExistingLease
}

func inspectOwnedCut(ctx context.Context, c Config, r resolved, l Limits, consume func(context.Context, resolved, *sql.Tx, *Inventory) error, h *hooks, owned ownedCut) (result Result, err error) {
	result.DiagnosticOnly = true
	if owned.lease == nil || owned.identity != r.identity || !l.valid() || consume == nil {
		return result, ErrConfiguration
	}
	lease := owned.lease
	if err = lease.Check(); err != nil {
		return
	}
	defer func() {
		if err != nil {
			result.Complete = false
		}
	}()
	// The dialer owns cancellation of all physical I/O, including startup.
	dialer := &catalogDialer{ctx: ctx}
	r.connector.Dialer(dialer)
	defer func() { err = errors.Join(err, dialer.join(), ctx.Err()) }()
	var connector driver.Connector = r.connector
	if h != nil && h.connector != nil {
		connector = h.connector
	}
	if h != nil && h.wrapConnector != nil {
		connector = h.wrapConnector(connector)
	}
	session, e := openCatalogSession(ctx, connector)
	if e != nil {
		return result, errors.Join(e, ctx.Err())
	}
	conn := sql.OpenDB(session)
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)
	defer func() {
		err = errors.Join(err, h.at("database-draining", nil))
		err = errors.Join(err, session.close(conn), h.at("database-closed", nil))
	}()
	if err = h.at("database-opened", nil); err != nil {
		return
	}
	tx, e := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if e != nil {
		return result, fmt.Errorf("catalog snapshot unavailable")
	}
	defer func() {
		e := tx.Rollback()
		if errors.Is(e, sql.ErrTxDone) {
			e = nil
		}
		err = errors.Join(err, e, h.at("snapshot-closed", nil))
	}()
	if err = h.at("snapshot-opened", tx); err != nil {
		return
	}
	var ro, isolation, database, rowSecurity string
	var server int
	if e = tx.QueryRowContext(ctx, `SELECT current_setting('transaction_read_only'),current_setting('transaction_isolation'),current_database(),current_setting('server_version_num')::integer,current_setting('row_security')`).Scan(&ro, &isolation, &database, &server, &rowSecurity); e != nil {
		return result, errors.New("catalog settings unavailable")
	}
	// row_security=off rejects policy-filtered reads; it does not grant a bypass.
	// Full-catalog inventory must never silently accept a role-filtered projection.
	if ro != "on" || isolation != "repeatable read" || database != c.Database || rowSecurity != "off" || server < 160000 || server >= 170000 {
		return result, ErrUnsupported
	}
	inv := &Inventory{Kind: "diagnostic-full-catalog-inventory/v1", Backend: "postgres16", Profile: r.profile, CatalogVersion: 17,
		RootHash: r.identity.Hash, Reader: "Coldkeep schema-17, container-major-0/1, legacy-v1, CKBL-v1, none/zstd, plain/AES-GCM",
		Limits: l, Catalog: make(map[string][]Row), Checks: []string{"catalog requirements", "full graph closure", "container headers and extents", "whole-object SHA-256", "packed encodings and chunk digests", "direct legacy chunk digests", "no reconstructed-file verification"}}
	if err = readCatalog(ctx, tx, inv, h); err != nil {
		return
	}
	if err = validateGraph(ctx, inv); err != nil {
		return
	}
	if err = inspectPayloads(ctx, r, inv, h); err != nil {
		return
	}
	if err = lease.Check(); err != nil {
		return
	}
	if err = ctx.Err(); err != nil {
		return
	}
	// Enforce serialization size before presenting an admissible inventory.
	if err = inv.WriteDiagnostic(io.Discard); err != nil {
		return
	}
	if err = h.at("consuming", tx); err != nil {
		return
	}
	rows, objects, catalogBytes := inv.Rows, len(inv.Objects), inv.CatalogBytes
	if err = consume(ctx, r, tx, inv); err != nil {
		return
	}
	if err = errors.Join(ctx.Err(), lease.Check()); err != nil {
		return
	}
	// Round trip after consumption prevents an expired/failed snapshot from being
	// mistaken for a still-live cut. All rows/files have already been closed.
	if e = tx.QueryRowContext(ctx, "SELECT 1").Scan(new(int)); e != nil {
		return result, errors.New("catalog snapshot lost")
	}
	result.Complete = true
	result.Rows = rows
	result.Objects = objects
	result.CatalogBytes = catalogBytes
	return
}

type budgetWriter struct {
	w         io.Writer
	remaining int64
}

func (w *budgetWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		return 0, ErrLimit
	}
	n, e := w.w.Write(p)
	w.remaining -= int64(n)
	if e == nil && n != len(p) {
		e = io.ErrShortWrite
	}
	return n, e
}

// WriteDiagnostic writes bounded JSON to a caller-owned destination outside the
// source. It writes one bounded catalog row at a time, not a whole-report buffer.
// Partial output is diagnostic and never contains a recovery completion marker.
func (i *Inventory) WriteDiagnostic(w io.Writer) error {
	if w == nil {
		return ErrConfiguration
	}
	bw := &budgetWriter{w: w, remaining: i.Limits.DiagnosticBytes}
	header := *i
	header.Catalog = nil
	b, e := json.Marshal(header)
	if e != nil {
		return e
	}
	// Replace the null catalog with the streamed rows, preserving all other fields.
	b = bytesWithoutCatalog(b)
	if _, e = bw.Write(b[:len(b)-1]); e != nil {
		return e
	}
	if _, e = io.WriteString(bw, `,"catalog":{`); e != nil {
		return e
	}
	for ti, spec := range catalogTables {
		if ti > 0 {
			if _, e = io.WriteString(bw, ","); e != nil {
				return e
			}
		}
		if _, e = fmt.Fprintf(bw, "%q:[", spec.name); e != nil {
			return e
		}
		for ri, row := range i.Catalog[spec.name] {
			if ri > 0 {
				if _, e = io.WriteString(bw, ","); e != nil {
					return e
				}
			}
			b, e = json.Marshal(row)
			if e != nil {
				return e
			}
			if _, e = bw.Write(b); e != nil {
				return e
			}
		}
		if _, e = io.WriteString(bw, "]"); e != nil {
			return e
		}
	}
	_, e = io.WriteString(bw, "}}\n")
	return e
}
func bytesWithoutCatalog(b []byte) []byte {
	return []byte(strings.Replace(string(b), `"catalog":null,`, "", 1))
}
