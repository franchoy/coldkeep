package db_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/testutil/backendtest"
)

var errRowsAffectedUnsupported = errors.New("rows affected unsupported")

type unsupportedMutationResult struct{}

func (unsupportedMutationResult) LastInsertId() (int64, error) {
	return 0, errors.New("last insert id unsupported")
}

func (unsupportedMutationResult) RowsAffected() (int64, error) {
	return 0, errRowsAffectedUnsupported
}

func TestMutationRowsAffectedContractAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{Schema: backendtest.EmptySchema}, func(t *testing.T, backend backendtest.Backend) {
		ctx := context.Background()
		if _, err := backend.DB.ExecContext(ctx, `
			CREATE TABLE phase17_mutation_rows (
				id INTEGER PRIMARY KEY,
				value TEXT NOT NULL,
				optional_value INTEGER,
				flag BOOLEAN NOT NULL DEFAULT FALSE
			)
		`); err != nil {
			t.Fatalf("create mutation fixture: %v", err)
		}
		if _, err := backend.DB.ExecContext(ctx, `INSERT INTO phase17_mutation_rows (id, value) VALUES ($1, $2)`, 1, "baseline"); err != nil {
			t.Fatalf("insert mutation fixture: %v", err)
		}

		existing, err := backend.DB.ExecContext(ctx, `UPDATE phase17_mutation_rows SET value = $1 WHERE id = $2`, "updated", 1)
		if err != nil {
			t.Fatalf("update existing row: %v", err)
		}
		if err := db.RequireExactlyOneRow(existing, "update existing fixture"); err != nil {
			t.Fatalf("require existing row: %v", err)
		}

		sameValue, err := backend.DB.ExecContext(ctx, `UPDATE phase17_mutation_rows SET value = $1 WHERE id = $2`, "updated", 1)
		if err != nil {
			t.Fatalf("same-value update: %v", err)
		}
		if err := db.RequireExactlyOneRow(sameValue, "same-value fixture update"); err != nil {
			t.Fatalf("require same-value matched row: %v", err)
		}

		sameNullableValues, err := backend.DB.ExecContext(ctx, `
			UPDATE phase17_mutation_rows
			SET value = $1, optional_value = $2, flag = $3
			WHERE id = $4
		`, "updated", nil, false, 1)
		if err != nil {
			t.Fatalf("same nullable-value update: %v", err)
		}
		if err := db.RequireExactlyOneRow(sameNullableValues, "same nullable-value fixture update"); err != nil {
			t.Fatalf("require same nullable-value matched row: %v", err)
		}

		missing, err := backend.DB.ExecContext(ctx, `UPDATE phase17_mutation_rows SET value = $1 WHERE id = $2`, "missing", 999)
		if err != nil {
			t.Fatalf("update missing row: %v", err)
		}
		err = db.RequireExactlyOneRow(missing, "update missing fixture")
		if !errors.Is(err, db.ErrMutationCardinality) {
			t.Fatalf("missing-row error=%v, want ErrMutationCardinality", err)
		}
		if !strings.Contains(err.Error(), "update missing fixture") || !strings.Contains(err.Error(), "affected 0 rows; expected 1") {
			t.Fatalf("missing-row error lacks bounded cardinality details: %v", err)
		}

		deleted, err := backend.DB.ExecContext(ctx, `DELETE FROM phase17_mutation_rows WHERE id = $1`, 1)
		if err != nil {
			t.Fatalf("delete existing row: %v", err)
		}
		if err := db.RequireExactlyOneRow(deleted, "delete existing fixture"); err != nil {
			t.Fatalf("require deleted row: %v", err)
		}

		if _, err := backend.DB.ExecContext(ctx, `INSERT INTO phase17_mutation_rows (id, value) VALUES ($1, $2)`, 2, "conflict"); err != nil {
			t.Fatalf("insert upsert fixture: %v", err)
		}
		conflict, err := backend.DB.ExecContext(ctx, `
			INSERT INTO phase17_mutation_rows (id, value)
			VALUES ($1, $2)
			ON CONFLICT (id) DO NOTHING
		`, 2, "ignored")
		if err != nil {
			t.Fatalf("execute upsert conflict: %v", err)
		}
		if err := db.RequireRowsAffected(conflict, "upsert conflict fixture", 0); err != nil {
			t.Fatalf("require zero-row conflict branch: %v", err)
		}
	})

	t.Run("unsupported-result", func(t *testing.T) {
		err := db.RequireExactlyOneRow(unsupportedMutationResult{}, "unsupported fixture result")
		if !errors.Is(err, db.ErrMutationCardinality) {
			t.Fatalf("error=%v, want ErrMutationCardinality", err)
		}
		if !errors.Is(err, errRowsAffectedUnsupported) {
			t.Fatalf("error=%v, want RowsAffected cause", err)
		}
		if !strings.Contains(err.Error(), "unsupported fixture result") {
			t.Fatalf("error lacks operation label: %v", err)
		}
	})

	t.Run("nil-result", func(t *testing.T) {
		err := db.RequireExactlyOneRow(nil, "nil fixture result")
		if !errors.Is(err, db.ErrMutationCardinality) {
			t.Fatalf("error=%v, want ErrMutationCardinality", err)
		}
	})
}
