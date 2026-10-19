// Command wallet runs the wallet service until SIGINT or SIGTERM stops it.
package main

import (
	"fmt"
	"os"

	"go.uber.org/fx"

	"github.com/Brunohvc/backend-challenge-go/internal/bootstrap"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
)

const exitUsage = 2

func main() {
	cfg, err := config.Load(os.LookupEnv)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "wallet: %v\n", err)
		os.Exit(exitUsage)
	}
	fx.New(bootstrap.Module(cfg)).Run()
}
