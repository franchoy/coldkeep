package recoveryexport

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lib/pq"
)

func countInventorySessions(ctx context.Context, db *sql.DB) (int, error) {
	var n int
	err := db.QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name='coldkeep-passive-inventory'").Scan(&n)
	return n, err
}

// This is an independent server observation, not a substitute for joining the
// real driver close or holding ownership while rollback/close are blocked.
func waitInventoryNoSessions(ctx context.Context, count func(context.Context) (int, error)) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	remaining := -1
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("owned sessions remain %d: %w", remaining, err)
		}
		var err error
		remaining, err = count(ctx)
		if err != nil {
			return fmt.Errorf("owned-session observation failed: %w", errors.Join(err, ctx.Err()))
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("owned-session observation canceled: %w", err)
		}
		if remaining == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("owned sessions remain %d: %w", remaining, ctx.Err())
		case <-ticker.C:
		}
	}
}

func inventoryObserverHeldSession(t *testing.T, f *fixture) *sql.DB {
	t.Helper()
	r, err := resolve(context.Background(), f.config)
	must(t, err)
	defer clear(r.key)
	cfg := r.pg
	cfg.ApplicationName = "coldkeep-passive-inventory"
	connector, err := pq.NewConnectorConfig(cfg)
	must(t, err)
	held := sql.OpenDB(connector)
	held.SetMaxOpenConns(1)
	held.SetMaxIdleConns(1)
	t.Cleanup(func() { must(t, held.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	must(t, held.PingContext(ctx))
	n, err := countInventorySessions(ctx, f.db)
	must(t, err)
	if n != 1 {
		t.Fatalf("owned held-session control not established: %d", n)
	}
	return held
}

func TestInventorySessionObservation(t *testing.T) {
	t.Run("disappearance", func(t *testing.T) {
		f := newFixture(t)
		held := inventoryObserverHeldSession(t, f)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		observations := 0
		err := waitInventoryNoSessions(ctx, func(ctx context.Context) (int, error) {
			n, err := countInventorySessions(ctx, f.db)
			observations++
			if observations == 1 {
				if err != nil || n != 1 {
					t.Fatalf("first observation did not see held session: %d %v", n, err)
				}
				must(t, held.Close())
			}
			return n, err
		})
		must(t, err)
		if observations < 2 {
			t.Fatal("nonzero observation accepted without seeing actual disappearance")
		}
	})
	t.Run("held-session", func(t *testing.T) {
		f := newFixture(t)
		inventoryObserverHeldSession(t, f)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		err := waitInventoryNoSessions(ctx, func(ctx context.Context) (int, error) {
			return countInventorySessions(ctx, f.db)
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("live owned session accepted: %v", err)
		}
	})
	t.Run("query-error", func(t *testing.T) {
		f := newFixture(t)
		must(t, f.db.Close())
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := waitInventoryNoSessions(ctx, func(ctx context.Context) (int, error) {
			return countInventorySessions(ctx, f.db)
		}); err == nil {
			t.Fatal("failed observation accepted as no sessions")
		}
	})
	t.Run("canceled-after-zero", func(t *testing.T) {
		f := newFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := waitInventoryNoSessions(ctx, func(ctx context.Context) (int, error) {
			n, err := countInventorySessions(ctx, f.db)
			must(t, err)
			if n != 0 {
				t.Fatalf("zero-session control not established: %d", n)
			}
			cancel()
			return n, nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled observation accepted: %v", err)
		}
	})
	t.Run("query-deadline", func(t *testing.T) {
		f := newFixture(t)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		started := time.Now()
		err := waitInventoryNoSessions(ctx, func(ctx context.Context) (int, error) {
			var n int
			err := f.db.QueryRowContext(ctx, "SELECT 1 FROM pg_sleep(30)").Scan(&n)
			return n, err
		})
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 5*time.Second {
			t.Fatalf("observation deadline did not bound query: %v, %s", err, time.Since(started))
		}
	})
}
