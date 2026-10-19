//go:build e2e

// Package e2e runs the real service binary as several processes and drives it over HTTP and SQS.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Brunohvc/backend-challenge-go/test/testenv"
)

const (
	readyTimeout = 30 * time.Second
	stopTimeout  = 30 * time.Second
	outputDelay  = 5 * time.Second
	pollEvery    = 50 * time.Millisecond
)

const consumerWait = time.Second

var instanceDefaults = map[string]string{
	"CONSUMER_WAIT_TIME":          consumerWait.String(),
	"CONSUMER_VISIBILITY_TIMEOUT": "8s",
	"CONSUMER_PROCESSING_TIMEOUT": "6s",
}

var lastExit struct {
	sync.Mutex
	at time.Time
}

func noteExit() {
	lastExit.Lock()
	defer lastExit.Unlock()
	lastExit.at = time.Now()
}

const deadPollMargin = 500 * time.Millisecond

func awaitDeadPolls() {
	lastExit.Lock()
	wait := consumerWait + deadPollMargin - time.Since(lastExit.at)
	lastExit.Unlock()
	if wait > 0 {
		time.Sleep(wait)
	}
}

var suite struct {
	env      *testenv.Env
	binary   string
	internal *tokenSource
	http     *http.Client
}

// Instance is a running process of the service.
type Instance struct {
	Name     string
	URL      string
	AdminURL string

	api bool

	cmd     *exec.Cmd
	out     *processOutput
	exited  chan struct{}
	stopped atomic.Bool
}

// StartInstance starts an instance with env, waits until it is ready and stops it with the test.
func StartInstance(t *testing.T, name string, env map[string]string) *Instance {
	t.Helper()
	awaitDeadPolls()
	freshQueues(t)
	inst := launch(t, name, env)
	inst.awaitReady(t)
	return inst
}

// Launch starts an instance as StartInstance does, without waiting for it to be ready.
func Launch(t *testing.T, name string, env map[string]string) *Instance {
	t.Helper()
	awaitDeadPolls()
	freshQueues(t)
	return launch(t, name, env)
}

// StartCluster starts n instances, app-1 to app-n, all with overrides.
func StartCluster(t *testing.T, n int, overrides map[string]string) []*Instance {
	t.Helper()
	awaitDeadPolls()
	freshQueues(t)
	cluster := make([]*Instance, n)
	for i := range cluster {
		cluster[i] = launch(t, fmt.Sprintf("app-%d", i+1), overrides)
	}
	t.Cleanup(func() {
		var wg sync.WaitGroup
		for _, inst := range cluster {
			wg.Go(func() { inst.end(t) })
		}
		wg.Wait()
	})
	for _, inst := range cluster {
		inst.awaitReady(t)
	}
	return cluster
}

func launch(t *testing.T, name string, env map[string]string) *Instance {
	t.Helper()
	settings := maps.Clone(instanceDefaults)
	maps.Copy(settings, env)

	cmd := exec.CommandContext(context.WithoutCancel(t.Context()), suite.binary) //nolint:gosec // the service TestMain built
	cmd.Env = environ(suite.env.AppEnv(name, settings))
	out := &processOutput{addrs: make(map[string]string), started: make(map[string]bool)}
	cmd.Stdout, cmd.Stderr = out, out
	cmd.WaitDelay = outputDelay
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}

	inst := &Instance{Name: name, api: settings["HTTP_ENABLED"] != "false", cmd: cmd, out: out, exited: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		noteExit()
		close(inst.exited)
	}()
	t.Cleanup(func() { inst.cleanup(t) })
	return inst
}

func (i *Instance) awaitReady(t *testing.T) {
	t.Helper()
	timeout := time.NewTimer(readyTimeout)
	defer timeout.Stop()
	for {
		if i.URL == "" {
			if addr := i.out.addr("http"); addr != "" {
				i.URL = "http://" + addr
			}
		}
		if i.AdminURL == "" {
			if addr := i.out.addr("admin"); addr != "" {
				i.AdminURL = "http://" + addr
			}
		}
		if i.AdminURL != "" && i.ready(t) {
			return
		}
		select {
		case <-i.exited:
			t.Fatalf("%s exited before it was ready: %s", i.Name, i.cmd.ProcessState)
		case <-timeout.C:
			i.Kill()
			t.Fatalf("%s was not ready within %s", i.Name, readyTimeout)
		case <-time.After(pollEvery):
		}
	}
}

func (i *Instance) ready(t *testing.T) bool {
	if !i.api {
		return i.out.startedComponent("workers")
	}
	if i.URL == "" {
		return false
	}
	r, err := call(t.Context(), http.MethodGet, i.URL+"/health/ready", "", nil, nil)
	return err == nil && r.status == http.StatusOK
}

// Kill ends the process with SIGKILL and waits until it is gone.
func (i *Instance) Kill() {
	i.stopped.Store(true)
	_ = i.cmd.Process.Kill()
	<-i.exited
}

// Terminate stops the process with SIGTERM and fails unless it exits with status 0 in time.
func (i *Instance) Terminate(t *testing.T, timeout time.Duration) {
	t.Helper()
	i.stopped.Store(true)
	select {
	case <-i.exited:
		t.Errorf("%s had already exited (%s) when it was to be stopped", i.Name, i.cmd.ProcessState)
		return
	default:
	}
	if err := i.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Errorf("SIGTERM to %s: %v", i.Name, err)
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-i.exited:
	case <-timer.C:
		i.Kill()
		t.Errorf("%s did not stop within %s of SIGTERM and was killed", i.Name, timeout)
		return
	}
	if code := i.cmd.ProcessState.ExitCode(); code != 0 {
		t.Errorf("%s stopped with %s, want exit status 0", i.Name, i.cmd.ProcessState)
	}
}

// WaitExit waits for the process to exit on its own and returns its exit status.
func (i *Instance) WaitExit(t *testing.T, timeout time.Duration) int {
	t.Helper()
	i.stopped.Store(true)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-i.exited:
		return i.cmd.ProcessState.ExitCode()
	case <-timer.C:
		i.Kill()
		t.Fatalf("%s did not exit within %s", i.Name, timeout)
		return 0
	}
}

// Output returns everything the process has written so far.
func (i *Instance) Output() string {
	return i.out.String()
}

func (i *Instance) cleanup(t *testing.T) {
	i.end(t)
	if t.Failed() {
		t.Logf("output of %s (%s):\n%s", i.Name, i.cmd.ProcessState, i.Output())
	}
}

func (i *Instance) end(t *testing.T) {
	select {
	case <-i.exited:
		if !i.stopped.Swap(true) {
			t.Errorf("%s exited on its own during the test: %s", i.Name, i.cmd.ProcessState)
		}
	default:
		if t.Failed() {
			i.Kill()
		} else {
			i.Terminate(t, stopTimeout)
		}
	}
}

func environ(vars map[string]string) []string {
	env := make([]string, 0, len(vars))
	for _, name := range slices.Sorted(maps.Keys(vars)) {
		env = append(env, name+"="+vars[name])
	}
	return env
}

type processOutput struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	scanned int
	addrs   map[string]string
	started map[string]bool
}

func (o *processOutput) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.buf.Write(p)
	for {
		rest := o.buf.Bytes()[o.scanned:]
		end := bytes.IndexByte(rest, '\n')
		if end < 0 {
			return len(p), nil
		}
		o.learn(rest[:end])
		o.scanned += end + 1
	}
}

func (o *processOutput) learn(line []byte) {
	var rec struct {
		Msg       string `json:"msg"`
		Component string `json:"component"`
		Addr      string `json:"addr"`
	}
	if json.Unmarshal(line, &rec) != nil || rec.Msg != "component started" {
		return
	}
	o.started[rec.Component] = true
	if rec.Addr != "" {
		o.addrs[rec.Component] = rec.Addr
	}
}

func (o *processOutput) addr(component string) string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.addrs[component]
}

func (o *processOutput) startedComponent(component string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.started[component]
}

func (o *processOutput) String() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.buf.String()
}
