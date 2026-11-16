package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/franchoy/coldkeep/v2/internal/container"
	dbconfig "github.com/franchoy/coldkeep/v2/internal/db"
	"github.com/franchoy/coldkeep/v2/internal/engine"
	"github.com/franchoy/coldkeep/v2/internal/storage"
	"github.com/lib/pq"
	"os"
	"strings"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		var pg *pq.Error
		if errors.As(e, &pg) && pg.Code == "P0001" && strings.Contains(pg.Message, "future version catalog_version 20") {
			fmt.Println("SCHEMA20_FENCE_CONFIRMED")
		}
		os.Exit(1)
	}
}
func run() error {
	var sc storage.StorageContext
	if os.Getenv("COLDKEEP_PROBE_BORROWED") == "1" {
		dsn, e := dbconfig.BuildPostgresConnStringFromEnv(os.Getenv("DB_NAME"))
		if e != nil {
			return e
		}
		conn, e := sql.Open("postgres", dsn)
		if e != nil {
			return e
		}
		defer func() { _ = conn.Close() }()
		sc = storage.StorageContext{DB: conn, ContainerDir: os.Getenv("COLDKEEP_STORAGE_DIR"), Writer: container.NewLocalWriterWithDirAndDB(os.Getenv("COLDKEEP_STORAGE_DIR"), container.GetContainerMaxSize(), conn)}
		defer func() { _ = sc.Writer.FinalizeContainer() }()
	} else {
		var e error
		sc, e = storage.OpenLocalStorage(os.Getenv("COLDKEEP_STORAGE_DIR"))
		if e != nil {
			return e
		}
		defer func() { _ = sc.Close() }()
	}
	eng, e := engine.New(engine.Config{DB: sc.DB, ContainerDir: sc.ContainerDir, StoreContext: &sc})
	if e != nil {
		return e
	}
	switch os.Getenv("COLDKEEP_PROBE_ACTION") {
	case "store-new", "store-reuse":
		_, e = eng.Store(context.Background(), engine.StoreRequest{SourcePath: os.Getenv("COLDKEEP_PROBE_SOURCE"), Codec: "plain"})
		return e
	case "gc":
		r, e := eng.GarbageCollect(context.Background(), engine.GarbageCollectRequest{Workers: 1})
		fmt.Printf("gc result=%+v\n", r)
		return e
	case "repair":
		r, e := eng.Repair(context.Background(), engine.RepairRequest{Targets: []string{"ref-counts", "chunk-live-ref-counts"}})
		fmt.Printf("repair result=%+v\n", r)
		if e != nil {
			return e
		}
		if r.Summary.Failed != len(r.Targets) {
			return nil
		}
		for _, item := range r.Targets {
			if !strings.Contains(item.Message, "future version catalog_version 20") {
				return fmt.Errorf("unexpected repair failure")
			}
		}
		return fmt.Errorf("SCHEMA20_FENCE_CONFIRMED: all repair targets refused mutation")
	}
	_, e = eng.SetConfiguration(context.Background(), engine.SetConfigurationRequest{Key: engine.ConfigurationKey("compression"), Value: "none"})
	return e
}
