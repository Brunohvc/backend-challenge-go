//go:build integration

// Package integration runs the whole service in process against real containers.
package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Brunohvc/backend-challenge-go/test/testenv"
)

var env *testenv.Env

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	e, err := testenv.Start(ctx)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() {
		if err := e.Terminate(context.Background()); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}()
	ctx, cancel = context.WithTimeout(context.Background(), time.Minute)
	err = e.KC.AllowAdminOverHTTP(ctx)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	env = e
	return m.Run()
}
