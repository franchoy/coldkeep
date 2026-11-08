package proofdaemon

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/url"
	"sync"

	"github.com/lib/pq"
	"github.com/mattn/go-sqlite3"
)

// One physical connection, no reconnection after uncertain driver failure.
// Prepared statements preserve pq's joined statement cancellation behavior.
// Operations finish under service ownership; browser contexts never own SQL.
type joinedConnector struct {
	driver.Connector
	mu     sync.Mutex
	conn   *joinedConn
	sealed bool
}
type joinedConn struct {
	driver.Conn
	done     chan struct{}
	closeErr error
}

func (c *joinedConn) Close() error { c.closeErr = c.Conn.Close(); close(c.done); return c.closeErr }
func (c *joinedConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	if b, ok := c.Conn.(driver.ConnBeginTx); ok {
		return b.BeginTx(context.WithoutCancel(ctx), o)
	}
	if o.ReadOnly || o.Isolation != 0 {
		return nil, errors.New("unsupported transaction options")
	}
	return c.Begin()
}
func (c *joinedConnector) Connect(ctx context.Context) (driver.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sealed || c.conn != nil {
		return nil, errors.New("fixture catalog reconnection refused")
	}
	raw, e := c.Connector.Connect(ctx)
	if e != nil {
		return nil, e
	}
	c.conn = &joinedConn{Conn: raw, done: make(chan struct{})}
	return c.conn, nil
}
func (c *joinedConnector) close(db *sql.DB) error {
	c.mu.Lock()
	c.sealed = true
	raw := c.conn
	c.mu.Unlock()
	e := db.Close()
	if raw != nil {
		<-raw.done
		e = errors.Join(e, raw.closeErr)
	}
	return e
}

type sqliteConnector struct{ dsn string }

func (c sqliteConnector) Connect(context.Context) (driver.Conn, error) { return c.Driver().Open(c.dsn) }
func (c sqliteConnector) Driver() driver.Driver                        { return &sqlite3.SQLiteDriver{} }
func openJoined(cfg Config) (*sql.DB, *joinedConnector, error) {
	var connector driver.Connector
	if cfg.Backend == "sqlite" {
		u := url.URL{Scheme: "file", Path: cfg.Catalog}
		q := u.Query()
		q.Set("mode", "rw")
		q.Set("_foreign_keys", "on")
		q.Set("_busy_timeout", "1000")
		u.RawQuery = q.Encode()
		connector = sqliteConnector{u.String()}
	} else {
		u, e := url.Parse(cfg.Catalog)
		if e != nil {
			return nil, nil, ErrFixture
		}
		q := u.Query()
		q.Set("connect_timeout", "5")
		q.Set("statement_timeout", "5000")
		q.Set("lock_timeout", "1000")
		q.Set("application_name", "coldkeep-phase2-proof")
		u.RawQuery = q.Encode()
		connector, e = pq.NewConnector(u.String())
		if e != nil {
			return nil, nil, ErrFixture
		}
	}
	joined := &joinedConnector{Connector: connector}
	db := sql.OpenDB(joined)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, joined, nil
}
