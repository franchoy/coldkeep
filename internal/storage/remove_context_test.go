package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
)

type removeRollbackProbeConnector struct {
	rollbackErr error
}

func (c *removeRollbackProbeConnector) Connect(context.Context) (driver.Conn, error) {
	return &removeRollbackProbeConn{rollbackErr: c.rollbackErr}, nil
}

func (c *removeRollbackProbeConnector) Driver() driver.Driver { return removeRollbackProbeDriver{} }

type removeRollbackProbeDriver struct{}

func (removeRollbackProbeDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("remove rollback probe requires connector")
}

type removeRollbackProbeConn struct {
	rollbackErr error
}

func (c *removeRollbackProbeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is unsupported")
}
func (c *removeRollbackProbeConn) Close() error { return nil }
func (c *removeRollbackProbeConn) Begin() (driver.Tx, error) {
	return &removeRollbackProbeTx{rollbackErr: c.rollbackErr}, nil
}
func (c *removeRollbackProbeConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return &removeRollbackProbeTx{rollbackErr: c.rollbackErr}, nil
}
func (c *removeRollbackProbeConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return nil, context.Canceled
}
func (c *removeRollbackProbeConn) CheckNamedValue(*driver.NamedValue) error { return nil }

type removeRollbackProbeTx struct {
	rollbackErr error
}

func (*removeRollbackProbeTx) Commit() error { return nil }
func (t *removeRollbackProbeTx) Rollback() error {
	return t.rollbackErr
}

func TestRemoveCancellationJoinsRollbackFailure(t *testing.T) {
	rollbackErr := errors.New("forced rollback failure")
	dbconn := sql.OpenDB(&removeRollbackProbeConnector{rollbackErr: rollbackErr})
	defer func() { _ = dbconn.Close() }()

	result, err := RemoveFileWithDBResultContext(context.Background(), dbconn, 1)
	if result != (RemoveFileResult{}) {
		t.Fatalf("remove result=%+v, want zero", result)
	}
	if !errors.Is(err, context.Canceled) || !errors.Is(err, rollbackErr) {
		t.Fatalf("remove error=%v, want joined cancellation and rollback failure", err)
	}
}
