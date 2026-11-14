// coldkeep-recovery is the bounded Linux operator adapter for package-v1 import
// and constrained schema17/18 snapshot restoration, with explicit adoption.
// Ordinary startup never initializes or adopts a catalog.
package main

import (
	"context"
	"fmt"
	"os"
	"time"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	o, e := parseCommandOptions(os.Args)
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if handled, e := runUnconfigured(ctx, o); handled {
		return e
	}
	return runConfigured(ctx, o)
}
