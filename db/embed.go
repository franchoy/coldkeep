package dbschema

import _ "embed"

var (
	//go:embed schema_sqlite.sql
	SQLiteSchema string

	//go:embed schema_postgres.sql
	PostgresSchema string
)
