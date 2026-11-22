package recoveryexport

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/lib/pq"
)

// A catalog session owns exactly one physical connection. database/sql's done
// flags precede actual rollback/driver Close (Go 1.26.10); DB.Close alone cannot
// join an automatic rollback. Wait for the driver's Close to finish instead.
// Refuse replacement connections: a failed snapshot is never retried as a cut.
type catalogSession struct {
	driver.Connector
	conn    *catalogConn
	mu      sync.Mutex
	claimed bool
	closed  bool
}

func openCatalogSession(ctx context.Context, connector driver.Connector) (*catalogSession, error) {
	conn, err := connector.Connect(ctx)
	if err != nil {
		return nil, errors.Join(errors.New("catalog connection unavailable"), ctx.Err())
	}
	raw, ok := conn.(catalogDriverConn)
	if !ok {
		return nil, errors.Join(errors.New("unsupported catalog driver"), conn.Close())
	}
	return &catalogSession{Connector: connector, conn: &catalogConn{catalogDriverConn: raw, done: make(chan struct{})}}, nil
}

func (s *catalogSession) Connect(ctx context.Context) (driver.Conn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.closed || s.claimed {
		return nil, driver.ErrBadConn
	}
	s.claimed = true
	return s.conn, nil
}

func (s *catalogSession) close(db *sql.DB) error {
	// Seal before closing the pool, so even a pending pool connection request
	// cannot obtain the physical connection after cleanup starts.
	s.mu.Lock()
	s.closed = true
	claimed := s.claimed
	s.mu.Unlock()
	err := db.Close()
	if !claimed {
		err = errors.Join(err, s.conn.Close())
	}
	<-s.conn.done
	return errors.Join(err, s.conn.rollbackErr, s.conn.closeErr)
}

// Only the direct-query interfaces used by this passive path are admitted.
// Embedded reset/validation preserve pq's database/sql cancellation behavior.
type catalogDriverConn interface {
	driver.Conn
	driver.ConnBeginTx
	driver.SessionResetter
	driver.Validator
}
type catalogConn struct {
	catalogDriverConn
	done                  chan struct{}
	rollbackErr, closeErr error // published by closing done, after sql releases Tx
}

func (c *catalogConn) Close() error {
	c.closeErr = c.catalogDriverConn.Close()
	close(c.done)
	return c.closeErr
}
func (c *catalogConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("passive catalog requires direct queries")
}
func (c *catalogConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.catalogDriverConn.BeginTx(context.WithoutCancel(ctx), opts)
	if err != nil {
		return nil, err
	}
	return &catalogTx{Tx: tx, conn: c}, nil
}
func (c *catalogConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	// pq's statement cancellation watcher (unlike its connection watcher)
	// joins the cancel request before QueryContext/Rows.Close returns. Prepare
	// itself has no watcher and is bounded by the owned socket deadline.
	stmt, err := c.catalogDriverConn.Prepare(q)
	if err != nil {
		return nil, err
	}
	query, ok := stmt.(driver.StmtQueryContext)
	if !ok {
		return nil, errors.Join(ErrUnsupported, stmt.Close())
	}
	rows, err := query.QueryContext(ctx, args)
	if err != nil {
		return nil, errors.Join(err, stmt.Close())
	}
	return &catalogRows{Rows: rows, stmt: stmt}, nil
}

type catalogRows struct {
	driver.Rows
	stmt driver.Stmt
}

func (r *catalogRows) Close() error { return errors.Join(r.Rows.Close(), r.stmt.Close()) }

type catalogTx struct {
	driver.Tx
	conn *catalogConn
}

func (tx *catalogTx) Rollback() error {
	tx.conn.rollbackErr = tx.Tx.Rollback()
	return tx.conn.rollbackErr
}

// pq v1.11.2's connection watchCancel does not join its cancel request.
// BeginTx above omits that watcher, while QueryContext uses pq's joined statement
// watcher. database/sql keeps the operation context and automatic rollback.
// The primary socket also gets a cancellation deadline: startup/BEGIN and any
// failed cancellation transport must unblock, including a stalled server. This
// is an I/O bound, never evidence of cleanup. close/join still wait for actual
// driver closure and every callback. Cancellation requests use pq's own bounded
// context/deadline so an expired operation cannot prevent sending cancellation.
type catalogDialer struct {
	ctx     context.Context
	sockets []*catalogSocket // Connect is synchronous, before pool construction
}
type catalogSocket struct {
	net.Conn
	once      sync.Once
	err       error
	stop      func() bool
	canceled  chan struct{}
	cancelErr error
}

func (c *catalogSocket) Close() error {
	c.once.Do(func() { c.err = c.Conn.Close() })
	return c.err
}
func (d *catalogDialer) Dial(network, address string) (net.Conn, error) {
	return d.DialContext(d.ctx, network, address)
}
func (d *catalogDialer) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(d.ctx, timeout)
	defer cancel()
	return d.DialContext(ctx, network, address)
}
func (d *catalogDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	socket := &catalogSocket{Conn: conn, canceled: make(chan struct{})}
	if len(d.sockets) == 0 {
		socket.stop = context.AfterFunc(d.ctx, func() {
			socket.cancelErr = socket.SetDeadline(time.Now().Add(5 * time.Second))
			if socket.cancelErr != nil {
				socket.cancelErr = errors.Join(socket.cancelErr, socket.Close())
			}
			close(socket.canceled)
		})
	} else {
		// Only joined statement-cancellation requests can open subsequent sockets.
		socket.stop = func() bool { return true }
	}
	d.sockets = append(d.sockets, socket)
	return socket, nil
}
func (d *catalogDialer) join() error {
	var err error
	for _, socket := range d.sockets {
		if !socket.stop() {
			<-socket.canceled
		}
		err = errors.Join(err, socket.Close(), socket.cancelErr)
	}
	return err
}

var _ pq.Dialer = (*catalogDialer)(nil)
var _ pq.DialerContext = (*catalogDialer)(nil)
