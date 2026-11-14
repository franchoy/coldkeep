package recoveryexport

// Reproduced on the exact submitted source; the preservation helper also checks
// source/package fingerprints, absent readiness and zero owned sessions.

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRecoveryImportHonorsInventoryDuration(t *testing.T) {
	f, _, o := importFixture(t)
	d, _ := importTarget(t, f)
	o.InventoryLimits.Duration = time.Nanosecond
	if o.Limits.Duration <= o.InventoryLimits.Duration {
		t.Fatal("probe must keep the independent global import duration longer")
	}
	r, err := importPreserved(t, f, d, o, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lower inventory duration must stop admission: result=%+v error=%v", r, err)
	}
	if r.Ready || r.Published {
		t.Fatalf("expired inventory published readiness: %+v", r)
	}
}
