package proofdaemon

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"
)

type closeDriver struct{ raw *blockingClose }

func (d closeDriver) Open(string) (driver.Conn, error) { return d.raw, nil }

type closeConnector struct{ raw *blockingClose }

func (c closeConnector) Driver() driver.Driver                        { return closeDriver(c) }
func (c closeConnector) Connect(context.Context) (driver.Conn, error) { return c.raw, nil }

type blockingClose struct{ entered, release chan struct{} }

func (c *blockingClose) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unused") }
func (c *blockingClose) Begin() (driver.Tx, error)           { return nil, errors.New("unused") }
func (c *blockingClose) Close() error                        { close(c.entered); <-c.release; return nil }
func TestProofPhysicalCloseJoin(t *testing.T) {
	raw := &blockingClose{make(chan struct{}), make(chan struct{})}
	connector := &joinedConnector{Connector: closeConnector{raw}}
	database := sql.OpenDB(connector)
	if e := database.Ping(); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { done <- connector.close(database) }()
	<-raw.entered
	select {
	case <-done:
		t.Fatal("close returned before driver joined")
	case <-time.After(30 * time.Millisecond):
	}
	close(raw.release)
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if _, e := connector.Connect(context.Background()); e == nil {
		t.Fatal("reconnected after cleanup")
	}
	t.Log("PROOF_EVENT physical_driver_close_join_and_no_reconnect")
}
