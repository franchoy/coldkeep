package main

import (
	"context"
	"fmt"
	"github.com/franchoy/coldkeep/v2/internal/engine"
	"github.com/franchoy/coldkeep/v2/internal/storage"
	"os"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	sc, e := storage.OpenLocalStorage(os.Getenv("COLDKEEP_STORAGE_DIR"))
	if e != nil {
		return e
	}
	defer func() { _ = sc.Close() }()
	eng, e := engine.New(engine.Config{DB: sc.DB, ContainerDir: sc.ContainerDir, StoreContext: &sc})
	if e != nil {
		return e
	}
	_, e = eng.SetConfiguration(context.Background(), engine.SetConfigurationRequest{Key: engine.ConfigurationKey("compression"), Value: "none"})
	return e
}
