package recoveryexport

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func assertNoInventorySessions(t *testing.T, db *sql.DB) {
	t.Helper()
	// Inspect already joins rollback, physical driver/socket closure and ownership.
	// Observe PostgreSQL's subsequent backend teardown with a separate deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	must(t, waitInventoryNoSessions(ctx, func(ctx context.Context) (int, error) {
		return countInventorySessions(ctx, db)
	}))
}

func TestPostgresCanceledQueryClosesSession(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "cancel"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			returned := make(chan struct{})
			var result Result
			var opErr error
			h := &hooks{event: func(stage string, tx *sql.Tx) error {
				if stage == "snapshot-opened" {
					return tx.QueryRowContext(ctx, "SELECT pg_sleep(30)").Scan(new(any))
				}
				return nil
			}}
			go func() { result, opErr = inspect(ctx, f.config, DefaultLimits(), noConsume, h); close(returned) }()
			defer func() { cancel(); awaitCleanup(t, returned) }()
			// Observe the real server running the query before triggering cancellation.
			observer, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			for {
				var sleeping bool
				must(t, f.db.QueryRowContext(observer, "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE datname=current_database() AND application_name='coldkeep-passive-inventory' AND wait_event='PgSleep')").Scan(&sleeping))
				if sleeping {
					break
				}
				select {
				case <-observer.Done():
					t.Fatal("query never reached PostgreSQL sleep")
				case <-ticker.C:
				}
			}
			if !deadline {
				cancel()
			}
			awaitCleanup(t, returned)
			want := context.Canceled
			if deadline {
				want = context.DeadlineExceeded
			}
			if result.Complete || !errors.Is(opErr, want) {
				t.Fatalf("cancelled query result=%+v error=%v", result, opErr)
			}
			assertNoInventorySessions(t, f.db)
			t.Log("SCENARIO PostgreSQL active query cancelled, cancellation request joined, zero inventory sessions on return")
		})
	}
}

// A real pq startup which receives no server response must still be bounded by
// operation cancellation. The socket observer sees EOF before Inspect returns.
func TestPostgresStalledStartupCancellation(t *testing.T) {
	for _, stage := range []string{"startup", "begin"} {
		t.Run(stage, func(t *testing.T) {
			f := newFixture(t)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			must(t, err)
			defer func() { _ = listener.Close() }()
			addr, ok := listener.Addr().(*net.TCPAddr)
			if !ok {
				t.Fatal("not TCP")
			}
			f.config.Host = "127.0.0.1"
			f.config.Port = uint16(addr.Port)
			started, closed := make(chan struct{}), make(chan struct{})
			serverErr := make(chan error, 1)
			go func() {
				defer close(closed)
				conn, e := listener.Accept()
				if e != nil {
					serverErr <- e
					return
				}
				defer func() { _ = conn.Close() }()
				_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
				var size [4]byte
				_, e = io.ReadFull(conn, size[:])
				if e == nil {
					n := binary.BigEndian.Uint32(size[:])
					if n < 8 || n > 10000 {
						e = fmt.Errorf("invalid startup size %d", n)
					} else {
						_, e = io.ReadFull(conn, make([]byte, n-4))
					}
				}
				if e == nil && stage == "begin" {
					e = acceptUntilBegin(conn)
				}
				if e != nil {
					serverErr <- e
					return
				}
				close(started)
				_, e = io.Copy(io.Discard, conn)
				serverErr <- e
			}()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			returned := make(chan struct{})
			var result Result
			var opErr error
			go func() { result, opErr = inspect(ctx, f.config, DefaultLimits(), noConsume, nil); close(returned) }()
			awaitCleanup(t, started)
			cancel()
			select {
			case <-returned:
			case <-time.After(8 * time.Second):
				t.Fatal("startup ignored cancellation")
			}
			awaitCleanup(t, closed)
			must(t, <-serverErr)
			if result.Complete || !errors.Is(opErr, context.Canceled) {
				t.Fatalf("startup result=%+v error=%v", result, opErr)
			}
			assertNoInventorySessions(t, f.db)
			t.Logf("SCENARIO stalled pq %s canceled, socket closed and observer joined, incomplete result", stage)
		})
	}
}

// Minimal PostgreSQL wire peer: authenticate and become ready, then withhold
// BEGIN's response. It exercises the pinned pq BeginTx I/O, not a fake Tx API.
func acceptUntilBegin(conn net.Conn) error {
	for _, message := range []struct {
		kind    byte
		payload []byte
	}{
		{'R', make([]byte, 4)},
		{'S', []byte("server_version\x0016.13\x00")},
		{'S', []byte("client_encoding\x00UTF8\x00")},
		{'S', []byte("standard_conforming_strings\x00on\x00")},
		{'Z', []byte{'I'}},
	} {
		frame := []byte{message.kind, 0, 0, 0, 0}
		binary.BigEndian.PutUint32(frame[1:], uint32(4+len(message.payload)))
		if _, err := conn.Write(append(frame, message.payload...)); err != nil {
			return err
		}
	}
	var header [5]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(header[1:])
	if header[0] != 'Q' || n < 4 || n > 10000 {
		return errors.New("expected bounded BEGIN query")
	}
	query := make([]byte, n-4)
	if _, err := io.ReadFull(conn, query); err != nil {
		return err
	}
	if !strings.HasPrefix(string(query), "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY") {
		return fmt.Errorf("unexpected transaction startup: %q", query)
	}
	return nil
}

func TestConsumerFailureClosesSession(t *testing.T) {
	f := newFixture(t)
	injected := errors.New("consumer failure")
	result, err := runPreserved(t, f, context.Background(), DefaultLimits(), nil, func(context.Context, *Inventory) error { return injected })
	if result.Complete || !errors.Is(err, injected) {
		t.Fatalf("consumer failure result=%+v error=%v", result, err)
	}
}
