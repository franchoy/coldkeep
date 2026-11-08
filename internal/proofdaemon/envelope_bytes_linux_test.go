//go:build linux && cgo

package proofdaemon

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/db"
)

// Runs on both the copied SQLite fixture and the Docker runner's disposable
// PostgreSQL fixture. Every mutation is rolled back before the next case.
func proofScalarByteControls(t *testing.T, conn *sql.DB) {
	t.Helper()
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)
	backend := db.BackendFromDB(conn)
	for _, field := range []struct {
		table, column, where string
		limit                int
	}{
		{"logical_file", "original_name", "id=(SELECT MIN(id) FROM logical_file)", 1024},
		{"snapshot_path", "path", "id=(SELECT MIN(id) FROM snapshot_path)", 1024},
		{"chunk", "chunk_hash", "id=(SELECT MIN(id) FROM chunk)", 128},
		{"snapshot", "id", "id='byte-control'", 128},
		{"snapshot", "label", "id='proof-history'", 1024},
		{"container", "filename", "id=(SELECT MIN(id) FROM container)", 255},
	} {
		t.Run(field.table+"_"+field.column, func(t *testing.T) {
			cases := []struct {
				name  string
				value any
				bad   bool
			}{
				{"ascii_exact", strings.Repeat("a", field.limit), false},
				{"ascii_over", strings.Repeat("a", field.limit+1), true},
				{"utf8_exact", strings.Repeat("λ", field.limit/2) + strings.Repeat("a", field.limit%2), false},
				{"utf8_over", strings.Repeat("λ", (field.limit+1)/2) + strings.Repeat("a", (field.limit+1)%2), true},
				{"empty", "", false},
			}
			if backend == db.BackendSQLite {
				cases = append(cases, struct {
					name  string
					value any
					bad   bool
				}{"nul_over", "a\x00" + strings.Repeat("x", field.limit), true})
			}
			if field.column == "label" {
				cases = append(cases, struct {
					name  string
					value any
					bad   bool
				}{"null", nil, false})
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					_, err := conn.Exec("BEGIN")
					must(t, err)
					defer func() { _, err := conn.Exec("ROLLBACK"); must(t, err) }()
					_, err = conn.Exec("INSERT INTO snapshot(id,type,created_at) VALUES ('byte-control','full',CURRENT_TIMESTAMP)")
					must(t, err)
					result, err := conn.Exec("UPDATE "+field.table+" SET "+field.column+"=$1 WHERE "+field.where, tc.value)
					if field.column == "path" && tc.name == "empty" {
						if err == nil {
							t.Fatal("existing schema must refuse empty path")
						}
						t.Log("empty path refused by preserved schema constraint")
						return
					}
					must(t, err)
					n, err := result.RowsAffected()
					must(t, err)
					if n != 1 {
						t.Fatalf("control must modify exactly one row, got %d", n)
					}
					got := checkEnvelope(conn)
					if tc.bad {
						if !errors.Is(got, ErrBounds) {
							t.Fatalf("want early byte bound, got %v", got)
						}
					} else {
						must(t, got)
					}
					t.Logf("backend=%s field=%s.%s case=%s result=%v", backend, field.table, field.column, tc.name, got)
				})
			}
		})
	}
	must(t, checkEnvelope(conn))
	t.Logf("PROOF_EVENT scalar_byte_fields_%s", backend)
}

func TestProofScalarByteFieldsSQLite(t *testing.T) {
	c := fixture(t, t.TempDir())
	conn, err := sql.Open("sqlite3", c.Catalog)
	must(t, err)
	defer func() { must(t, conn.Close()) }()
	proofScalarByteControls(t, conn)
}

func TestProofSQLiteEncodingAdmission(t *testing.T) {
	for _, encoding := range []string{"UTF-16le", "UTF-16be"} {
		t.Run(encoding, func(t *testing.T) {
			conn, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "encoding.db"))
			must(t, err)
			defer func() { must(t, conn.Close()) }()
			_, err = conn.Exec(fmt.Sprintf("PRAGMA encoding='%s'; CREATE TABLE value_witness(value TEXT)", encoding))
			must(t, err)
			_, err = conn.Exec("INSERT INTO value_witness VALUES ($1)", strings.Repeat("漢", 512))
			must(t, err)
			var stored int
			var delivered string
			must(t, conn.QueryRow("SELECT octet_length(value),value FROM value_witness").Scan(&stored, &delivered))
			if stored != 1024 || len(delivered) != 1536 {
				t.Fatalf("encoding witness %d/%d", stored, len(delivered))
			}
			if !errors.Is(checkEnvelope(conn), ErrEncoding) {
				t.Fatal("must refuse encoding before catalog table/history reads")
			}
			t.Logf("PROOF_EVENT sqlite_utf16_refused_before_reads: %s storage=%d Go_UTF8=%d", encoding, stored, len(delivered))
		})
	}
}
