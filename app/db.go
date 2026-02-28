package main

import (
	"database/sql"
	"log"
	"os"
	"strings"

	_ "github.com/lib/pq"
)

func connectDB() (*sql.DB, error) {
	connStr := "host=" + os.Getenv("DB_HOST") +
		" port=" + os.Getenv("DB_PORT") +
		" user=" + os.Getenv("DB_USER") +
		" password=" + os.Getenv("DB_PASSWORD") +
		" dbname=" + os.Getenv("DB_NAME") +
		" sslmode=" + envOrDefault("DB_SSLMODE", "disable")
	safeConnStr := strings.ReplaceAll(connStr, "password="+os.Getenv("DB_PASSWORD"), "password=***")

	log.Printf("Connecting to DB with: %s", safeConnStr) // Log the connection string (without password)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

// DBTX is implemented by *sql.DB and *sql.Tx (so we can reuse helpers inside a tx).
type DBTX interface {
	Exec(query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) *sql.Row
}
