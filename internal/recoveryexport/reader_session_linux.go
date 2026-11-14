package recoveryexport

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"github.com/franchoy/coldkeep/v2/internal/coordination"
	"github.com/lib/pq"
	"sync"
)

// RecoverySession owns one physical SQL connection. Close joins automatic
// rollback, driver close and cancellation transport before the lease may end.
type RecoverySession struct {
	*sql.DB
	session  *catalogSession
	dialer   *catalogDialer
	once     sync.Once
	closeErr error
}

func (r *RecoverySession) Close() error {
	r.once.Do(func() { r.closeErr = errors.Join(r.session.close(r.DB), r.dialer.join()) })
	return r.closeErr
}

// OpenRecoverySession creates an owned reader after full receipt/catalog/payload
// validation. The caller must Close it before releasing the root lease. The
// connection cannot be replaced or returned to an ordinary writer pool.
func OpenRecoverySession(ctx context.Context, c Config, lease coordination.ExistingLease, receipt ImportReceipt) (*RecoverySession, error) {
	return openRecoverySession(ctx, c, lease, receipt, nil)
}
func openRecoverySession(ctx context.Context, c Config, lease coordination.ExistingLease, receipt ImportReceipt, h *hooks) (*RecoverySession, error) {
	if e := ValidateRestoreOwned(ctx, c, lease, receipt); e != nil {
		return nil, e
	}
	r, e := resolve(ctx, c)
	if e != nil {
		return nil, e
	}
	defer clear(r.key)
	if receipt.KeyRequired != (len(r.key) > 0) || receipt.KeyReference != r.keyRef {
		return nil, ErrConfiguration
	}
	cfg := r.pg
	cfg.Runtime["default_transaction_read_only"] = "off"
	cfg.Runtime["search_path"] = "pg_catalog,public"
	cfg.Runtime["statement_timeout"] = "60000"
	cfg.ApplicationName = "coldkeep-recovery-reader"
	connector, e := pq.NewConnectorConfig(cfg)
	if e != nil {
		return nil, ErrConfiguration
	}
	dialer := &catalogDialer{ctx: ctx}
	connector.Dialer(dialer)
	var dc driver.Connector = connector
	if h != nil && h.wrapConnector != nil {
		dc = h.wrapConnector(dc)
	}
	session, e := openCatalogSession(ctx, dc)
	if e != nil {
		return nil, errors.Join(e, dialer.join())
	}
	conn := sql.OpenDB(adoptionConnector{session})
	reader := &RecoverySession{DB: conn, session: session, dialer: dialer}
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)
	var oid int64
	if e = conn.QueryRowContext(ctx, "SELECT oid::bigint FROM pg_database WHERE datname=current_database()").Scan(&oid); e == nil && oid != receipt.DatabaseOID {
		e = ErrConfiguration
	}
	if e == nil && receipt.Kind == "coldkeep-import-ready/v4" {
		_, e = conn.ExecContext(ctx, `SET coldkeep.recovery_pin_authority = 'schema20-v1'`)
	}
	if e != nil {
		return nil, errors.Join(e, reader.Close())
	}
	return reader, nil
}
