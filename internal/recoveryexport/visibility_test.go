package recoveryexport

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
)

func TestCatalogRowSecurityCannotHideClosure(t *testing.T) {
	for _, ownTables := range []bool{false, true} {
		name := "filtered-reader-refused"
		if ownTables {
			name = "already-authorized-owner-sees-full-catalog"
		}
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			if ownTables {
				// Fixture setup alone grants this role table ownership. Inspect must
				// neither change privileges nor turn off RLS on source tables.
				execSQL(t, f.db, "GRANT CREATE ON SCHEMA public TO "+pqIdent(f.config.User))
			}
			for _, spec := range catalogTables {
				if spec.name == "schema_version" {
					continue
				}
				execSQL(t, f.db, "ALTER TABLE public."+pqIdent(spec.name)+" ENABLE ROW LEVEL SECURITY")
				if ownTables {
					execSQL(t, f.db, "ALTER TABLE public."+pqIdent(spec.name)+" OWNER TO "+pqIdent(f.config.User))
				}
			}
			// A real control query demonstrates that the restricted role would
			// silently see zero objects under the normal row_security=on default.
			control, err := f.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
			must(t, err)
			defer func() { _ = control.Rollback() }()
			_, err = control.Exec("SET LOCAL ROLE " + pqIdent(f.config.User))
			must(t, err)
			_, err = control.Exec("SET LOCAL row_security=on")
			must(t, err)
			var visible int
			must(t, control.QueryRow("SELECT count(*) FROM public.container").Scan(&visible))
			must(t, control.Rollback())
			wantVisible := 0
			if ownTables {
				wantVisible = 1
			}
			t.Logf("SCENARIO row-security control ownTables=%v visibleContainers=%d", ownTables, visible)
			if visible != wantVisible {
				t.Fatalf("fixture visibility=%d, want %d", visible, wantVisible)
			}
			consumed := false
			result, err := runPreserved(t, f, context.Background(), DefaultLimits(), nil, func(_ context.Context, inventory *Inventory) error {
				consumed = true
				if !ownTables {
					return fmt.Errorf("restricted reader admitted a filtered catalog")
				}
				if len(inventory.Catalog["logical_file"]) != 7 || len(inventory.Catalog["chunk"]) != 35 || len(inventory.Objects) != 1 {
					t.Fatalf("authorized full inventory incomplete: %+v", inventory)
				}
				return nil
			})
			if ownTables {
				must(t, err)
				if !result.Complete || !consumed {
					t.Fatalf("full visible catalog refused: %+v", result)
				}
			} else if err == nil || result.Complete || consumed {
				t.Fatalf("filtered catalog admitted: result=%+v error=%v consumed=%v", result, err, consumed)
			}
		})
	}
}
