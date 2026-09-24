package db

import (
	"context"
	"database/sql"
	"fmt"
)

// repositoryMutationAdvisoryLockID is shared with live GC. A repair publication
// and GC therefore cannot switch or collect physical authority concurrently.
const repositoryMutationAdvisoryLockID int64 = 847362

// AcquireRepositoryMutationLock obtains the transaction-scoped PostgreSQL
// advisory exclusion used by repair publication. SQLite is already serialized
// by its write transaction and needs no separate advisory primitive.
func AcquireRepositoryMutationLock(ctx context.Context, dbconn *sql.DB, tx *sql.Tx) error {
	if dbconn == nil || tx == nil {
		return fmt.Errorf("repository mutation lock requires database and transaction")
	}
	switch BackendFromDB(dbconn) {
	case BackendSQLite:
		return nil
	case BackendPostgres:
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, repositoryMutationAdvisoryLockID); err != nil {
			return fmt.Errorf("acquire repository mutation advisory lock: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("repository mutation lock does not support backend %s", BackendFromDB(dbconn))
	}
}
