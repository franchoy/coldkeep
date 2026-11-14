// coldkeepd is an internal action 2.02 fixture proof, not a product service.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/franchoy/coldkeep/v2/internal/proofdaemon"
	"github.com/franchoy/coldkeep/v2/internal/version"
	"github.com/franchoy/coldkeep/v2/internal/webui"
)

func main() { os.Exit(run()) }
func run() int {
	config := flag.String("config", "", "private operator fixture configuration")
	diagnostic := flag.Bool("diagnostics", false, "inspect embedded build identity without opening a repository")
	flag.Parse()
	log.SetOutput(io.Discard)
	bundle, e := webui.Load()
	if e != nil {
		fmt.Fprintln(os.Stderr, "embedded_assets_missing_or_invalid: run the prepared build")
		return 2
	}
	if *diagnostic {
		if e = json.NewEncoder(os.Stdout).Encode(struct {
			Application string            `json:"application"`
			Native      version.BuildInfo `json:"native_vcs"`
			Prepared    webui.Manifest    `json:"prepared"`
		}{version.String(), version.Build(), bundle.Manifest}); e != nil {
			return 1
		}
		return 0
	}
	c, e := proofdaemon.ReadConfig(*config)
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		return 2
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if e = proofdaemon.Run(ctx, c, bundle); e != nil {
		fmt.Fprintln(os.Stderr, "fixture_runtime_failed:", e)
		return 1
	}
	return 0
}
