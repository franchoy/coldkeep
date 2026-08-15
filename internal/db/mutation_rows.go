package db

import (
	"database/sql"
	"errors"
	"fmt"
)

// ErrMutationCardinality identifies a required SQL mutation that did not
// affect the expected number of direct target rows.
var ErrMutationCardinality = errors.New("SQL mutation cardinality mismatch")

// RequireRowsAffected verifies the direct row count reported by a mutation.
// Operation must be a bounded logical label; callers must not include SQL text,
// connection details, paths, hashes, or bound values.
func RequireRowsAffected(result sql.Result, operation string, expected int64) error {
	if result == nil {
		return fmt.Errorf("%w: %s: result is nil", ErrMutationCardinality, operation)
	}

	actual, err := result.RowsAffected()
	if err != nil {
		return errors.Join(
			ErrMutationCardinality,
			fmt.Errorf("%s: determine rows affected: %w", operation, err),
		)
	}
	if actual != expected {
		return fmt.Errorf(
			"%w: %s affected %d rows; expected %d",
			ErrMutationCardinality,
			operation,
			actual,
			expected,
		)
	}

	return nil
}

// RequireExactlyOneRow verifies that a mutation affected one direct target row.
func RequireExactlyOneRow(result sql.Result, operation string) error {
	return RequireRowsAffected(result, operation, 1)
}
