//go:build linux && cgo

// Behavioral follow-up regressions, qualified against the submitted source.
package proofdaemon

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/engine"
)

func TestProofReviewRejectsOversizedNULLabelBeforeHistory(t *testing.T) {
	t.Setenv("COLDKEEP_KEY", "")
	c := fixture(t, t.TempDir())
	conn, err := sql.Open("sqlite3", c.Catalog)
	must(t, err)
	label := "safe\x00" + strings.Repeat("x", 64<<10)
	_, err = conn.Exec("UPDATE snapshot SET label=$1 WHERE id=$2", label, c.Snapshot)
	must(t, err)
	var reportedCharacters, storedBytes int
	must(t, conn.QueryRow("SELECT LENGTH(label), LENGTH(CAST(label AS BLOB)) FROM snapshot WHERE id=$1", c.Snapshot).Scan(&reportedCharacters, &storedBytes))
	if reportedCharacters != 4 || storedBytes != len(label) {
		t.Fatalf("counterexample setup: LENGTH=%d stored bytes=%d, expected 4/%d", reportedCharacters, storedBytes, len(label))
	}
	var version, encoding string
	must(t, conn.QueryRow("SELECT sqlite_version()").Scan(&version))
	must(t, conn.QueryRow("PRAGMA encoding").Scan(&encoding))
	t.Logf("SQLite=%s encoding=%s LENGTH=%d complete_bytes=%d", version, encoding, reportedCharacters, storedBytes)
	envelopeErr := checkEnvelope(conn)
	must(t, conn.Close())
	if !errors.Is(envelopeErr, ErrBounds) {
		// Qualify that the supposedly bounded existing read consumes the full
		// value, without asking a huge allocation or changing production code.
		runtime, openErr := Open(c)
		observedLabelBytes := -1
		var historyErr error
		if runtime != nil {
			defer func() { must(t, runtime.Close()) }()
		}
		if openErr == nil {
			history, err := runtime.engine.SnapshotShow(context.Background(), engine.SnapshotShowRequest{SnapshotID: c.Snapshot})
			historyErr = err
			observedLabelBytes = len(history.Snapshot.Label)
		}
		t.Fatalf("want ErrBounds before engine reads; scalar envelope=%v; Open=%v; SnapshotShow=%v; observed label bytes=%d; SQL LENGTH=%d, stored bytes=%d", envelopeErr, openErr, historyErr, observedLabelBytes, reportedCharacters, storedBytes)
	}
	runtime, openErr := Open(c)
	if runtime != nil {
		must(t, runtime.Close())
	}
	conn, err = sql.Open("sqlite3", c.Catalog)
	must(t, err)
	var retained string
	must(t, conn.QueryRow("SELECT label FROM snapshot WHERE id=$1", c.Snapshot).Scan(&retained))
	must(t, conn.Close())
	if retained != label {
		t.Fatal("refused catalog label changed")
	}
	t.Log("PROOF_EVENT nul_label_refused_before_engine_preserved")
	if !errors.Is(openErr, ErrBounds) {
		t.Fatalf("Open must preserve early ErrBounds, got %v", openErr)
	}
}

func TestProofReviewLabelBoundaryControls(t *testing.T) {
	t.Setenv("COLDKEEP_KEY", "")
	for _, tc := range []struct {
		name  string
		label string
		want  error
	}{
		{name: "ascii_at_1024", label: strings.Repeat("a", 1024)},
		{name: "utf8_1024_bytes", label: strings.Repeat("\u03bb", 512)},
		{name: "ascii_over_1024", label: strings.Repeat("a", 1025), want: ErrBounds},
		{name: "utf8_over_1024", label: strings.Repeat("λ", 512) + "a", want: ErrBounds},
		{name: "empty", label: ""},
		{name: "bounded_nul", label: "safe\x00tail"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := fixture(t, t.TempDir())
			conn, err := sql.Open("sqlite3", c.Catalog)
			must(t, err)
			_, err = conn.Exec("UPDATE snapshot SET label=$1 WHERE id=$2", tc.label, c.Snapshot)
			must(t, err)
			actual := checkEnvelope(conn)
			must(t, conn.Close())
			if !errors.Is(actual, tc.want) {
				t.Fatalf("envelope got %v, want %v", actual, tc.want)
			}
			if tc.want != nil {
				return
			}
			runtime, err := Open(c)
			must(t, err)
			defer func() { must(t, runtime.Close()) }()
			history, err := runtime.engine.SnapshotShow(context.Background(), engine.SnapshotShowRequest{SnapshotID: c.Snapshot})
			must(t, err)
			if history.Snapshot.Label != tc.label {
				t.Fatal("valid bounded label was changed or truncated")
			}
		})
	}
}
