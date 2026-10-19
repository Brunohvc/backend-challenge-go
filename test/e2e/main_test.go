//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Brunohvc/backend-challenge-go/test/testenv"
)

const servicePackage = "github.com/Brunohvc/backend-challenge-go/cmd/wallet"

const setupTimeout = 5 * time.Minute

func TestMain(m *testing.M) {
	os.Exit(runSuite(m))
}

func runSuite(m *testing.M) int {
	if err := killLeftovers(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	dir, err := os.MkdirTemp("", "wallet-e2e-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() { _ = os.RemoveAll(dir) }()

	ctx, cancel := context.WithTimeout(context.Background(), setupTimeout)
	defer cancel()
	binary := filepath.Join(dir, "wallet")
	var (
		env              *testenv.Env
		envErr, buildErr error
		wg               sync.WaitGroup
	)
	wg.Go(func() { env, envErr = testenv.Start(ctx) })
	wg.Go(func() { buildErr = buildService(ctx, binary) })
	wg.Wait()
	if env != nil {
		defer func() {
			if err := env.Terminate(context.Background()); err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		}()
	}
	if err := errors.Join(envErr, buildErr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	transport := &http.Transport{MaxIdleConnsPerHost: 64}
	defer transport.CloseIdleConnections()
	suite.env = env
	suite.binary = binary
	suite.internal = newTokenSource(internalClient)
	suite.http = &http.Client{Transport: transport, Timeout: 30 * time.Second}
	return m.Run()
}

const leftoversTimeout = 10 * time.Second

var leftoverInstance = regexp.MustCompile(`/wallet-e2e-[0-9]+/wallet$`)

func killLeftovers(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, leftoversTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "ps", "-A", "-o", "pid=,ppid=,args=").Output()
	if err != nil {
		return fmt.Errorf("e2e: list processes: %w", err)
	}
	for line := range strings.Lines(string(out)) {
		fields := strings.Fields(line)
		if len(fields) < 3 || fields[1] != "1" || !leftoverInstance.MatchString(fields[2]) {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil {
			return fmt.Errorf("e2e: process id %q: %w", fields[0], err)
		}
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
			return fmt.Errorf("e2e: kill instance %d left by an earlier run: %w", pid, err)
		}
		fmt.Fprintf(os.Stderr, "e2e: killed instance %d left by an earlier run: %s\n", pid, fields[2])
	}
	return nil
}

func buildService(ctx context.Context, binary string) error {
	args := []string{"build", "-tags", "failpoints", "-o", binary}
	if raceEnabled {
		args = append(args, "-race")
	}
	out, err := exec.CommandContext(ctx, "go", append(args, servicePackage)...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("e2e: build %s: %w\n%s", servicePackage, err, out)
	}
	return nil
}
