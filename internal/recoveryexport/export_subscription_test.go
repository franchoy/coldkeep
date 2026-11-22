//go:build linux

package recoveryexport

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func disabledSubscription(t *testing.T, conn *sql.DB) {
	t.Helper()
	// No publisher connection, worker, slot or table synchronization is created.
	// A NULL slot also makes DROP SUBSCRIPTION entirely local during cleanup.
	execSQL(t, conn, `CREATE SUBSCRIPTION coldkeep_recovery_fixture
 CONNECTION 'host=127.0.0.1 port=1 dbname=coldkeep_absent user=coldkeep_fixture'
 PUBLICATION coldkeep_absent WITH (connect=false, slot_name=NONE)`)
	// Registered after fixture creation, so this runs before its database drops.
	t.Cleanup(func() {
		execSQL(t, conn, "DROP SUBSCRIPTION coldkeep_recovery_fixture")
	})
}

func subscriptionState(t *testing.T, conn *sql.DB) string {
	t.Helper()
	var state string
	must(t, conn.QueryRow(`SELECT json_build_array(subname,subowner,subenabled,
 subslotname,subpublications)::text FROM pg_catalog.pg_subscription
 WHERE subdbid=(SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database())
 AND subname='coldkeep_recovery_fixture'`).Scan(&state))
	return state
}

func subscriptionCatalogPrivileges(t *testing.T, f *fixture) {
	t.Helper()
	tx, e := f.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	must(t, e)
	defer func() { _ = tx.Rollback() }()
	_, e = tx.Exec("SET LOCAL ROLE " + pqIdent(f.config.User))
	must(t, e)
	var super, databaseID, connectionInfo bool
	must(t, tx.QueryRow(`SELECT
 (SELECT rolsuper FROM pg_roles WHERE rolname=current_user),
 has_column_privilege(current_user,'pg_catalog.pg_subscription','subdbid','SELECT'),
 has_column_privilege(current_user,'pg_catalog.pg_subscription','subconninfo','SELECT')`).Scan(&super, &databaseID, &connectionInfo))
	must(t, tx.Rollback())
	if super || !databaseID || connectionInfo {
		t.Fatalf("fixture privileges superuser=%v subdbid=%v subconninfo=%v", super, databaseID, connectionInfo)
	}
}

func TestRecoverySubscriptionRefusal(t *testing.T) {
	for _, owner := range []bool{false, true} {
		name := "restricted-reader"
		if owner {
			name = "database-owner"
		}
		t.Run(name, func(t *testing.T) {
			f := exportFixture(t)
			if owner {
				// Setup only: ordinary database ownership must neither hide the
				// subscription nor require access to its password-bearing column.
				execSQL(t, f.db, "ALTER DATABASE "+pqIdent(f.config.Database)+" OWNER TO "+pqIdent(f.config.User))
			}
			disabledSubscription(t, f.db)
			subscriptionCatalogPrivileges(t, f)
			before := subscriptionState(t, f.db)
			// A disabled subscription adds no public table/column requirements:
			// the unchanged passive schema/graph check still admits this catalog.
			inventory, e := runPreserved(t, f, context.Background(), DefaultLimits(), nil, noConsume)
			must(t, e)
			if !inventory.Complete {
				t.Fatal("disabled subscription changed passive catalog admission")
			}
			o := exportOptions(t)
			snapshotExported := false
			h := &exportHooks{event: func(stage string) error {
				if stage == "snapshot-exported" {
					snapshotExported = true
				}
				return nil
			}}
			result, e := exportPreserved(t, f, o, h)
			if !errors.Is(e, ErrUnsupported) || snapshotExported {
				t.Fatalf("subscription reached dump preparation: error=%v snapshotExported=%v", e, snapshotExported)
			}
			assertIncomplete(t, o, result, e)
			if subscriptionState(t, f.db) != before {
				t.Fatal("export changed subscription metadata")
			}
			t.Logf("SCENARIO current-database disabled subscription owner=%v refuses before dump; non-superuser can read subdbid but not subconninfo", owner)
		})
	}
}

func TestRecoverySubscriptionOtherDatabase(t *testing.T) {
	f := exportFixture(t)
	other := exportFixture(t)
	disabledSubscription(t, other.db)
	subscriptionCatalogPrivileges(t, f)
	before := subscriptionState(t, other.db)
	o := exportOptions(t)
	result, e := exportPreserved(t, f, o, nil)
	must(t, e)
	if !result.Complete || !result.Published {
		t.Fatalf("another database's subscription blocked export: %+v", result)
	}
	checked, e := Check(context.Background(), o.Destination, o.Limits)
	must(t, e)
	if !checked.ByteComplete || subscriptionState(t, other.db) != before {
		t.Fatal("another database's subscription changed or export was incomplete")
	}
	t.Log("SCENARIO subscription in another database remains unchanged; selected database exports successfully without subconninfo privilege")
}
