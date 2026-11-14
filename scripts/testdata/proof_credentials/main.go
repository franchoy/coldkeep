// Explicit disposable-fixture credential qualification; never logs credentials.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/franchoy/coldkeep/v2/internal/db"
	"github.com/lib/pq"
)

func require(ok bool, message string) {
	if !ok {
		panic(message)
	}
}

func authenticate(config pq.Config, accepted bool) {
	config.ConnectTimeout = 5 * time.Second
	connector, err := pq.NewConnectorConfig(config)
	require(err == nil, "credential connector construction failed")
	connection := sql.OpenDB(connector)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = connection.PingContext(ctx)
	defer connection.Close()
	if accepted {
		require(err == nil, "correct-password TCP authentication failed")
	} else {
		var refusal *pq.Error
		require(errors.As(err, &refusal) && refusal.Code == "28P01", "expected password authentication refusal")
	}
}

func main() {
	secret := os.Getenv("PGPASSWORD")
	require(len(secret) == 64, "explicit disposable credential required")
	raw, err := os.ReadFile(os.Getenv("COLDKEEP_PROOF_PG_CONFIG"))
	require(err == nil, "fixture configuration unavailable")
	var fixture struct{ Catalog string }
	require(json.Unmarshal(raw, &fixture) == nil, "fixture configuration invalid")
	u, err := url.Parse(fixture.Catalog)
	require(err == nil && u.Scheme == "postgres" && u.Hostname() != "", "explicit TCP target required")
	_, hasPassword := u.User.Password()
	require(!hasPassword, "fixture URL must omit password")
	config, err := pq.NewConfig(fixture.Catalog)
	require(err == nil && config.Password == secret, "password-free URL must consume PGPASSWORD")
	authenticate(config, true)
	for _, password := range []string{"", "deliberately-incorrect-disposable-password"} {
		bad := config
		bad.Password = password
		authenticate(bad, false)
	}
	// The retained CLI's explicit keyword must override pq's inherited value.
	for key, value := range map[string]string{"DB_HOST": u.Hostname(), "DB_PORT": u.Port(),
		"DB_USER": u.User.Username(), "DB_NAME": u.Path[1:], "DB_SSLMODE": "disable", "DB_PASSWORD": ""} {
		require(os.Setenv(key, value) == nil, "credential control environment failed")
	}
	dsn, err := db.BuildPostgresConnStringFromEnv("")
	require(err == nil, "CLI configuration failed")
	config, err = pq.NewConfig(dsn)
	require(err == nil && config.Password == "", "CLI empty keyword must override PGPASSWORD")
	authenticate(config, false)
	require(os.Setenv("DB_PASSWORD", secret) == nil, "CLI credential setup failed")
	dsn, err = db.BuildPostgresConnStringFromEnv("")
	require(err == nil, "CLI configuration failed")
	config, err = pq.NewConfig(dsn)
	require(err == nil && config.Password == secret, "CLI must consume DB_PASSWORD")
	authenticate(config, true)
	fmt.Println("PROOF_EVENT postgres_credential_channels_tcp_success_and_refusals")
}
