//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"io"
	"maps"
	"net/url"
	"os"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	nginxImage = "nginx:1.30.6-alpine"
	nginxPort  = "80/tcp"
	hostAlias  = "host.docker.internal"
)

const nginxConf = "../../deploy/nginx/nginx.conf"

const pinIPv4 = `set -eu
addr=$(getent ahostsv4 %[1]s | awk 'NR == 1 { print $1 }')
if [ -z "$addr" ]; then
	echo "%[1]s has no IPv4 address" >&2
	exit 1
fi
sed -i "s/%[2]s:/$addr:/" /etc/nginx/nginx.conf
exec nginx -g 'daemon off;'
`

var upstreamServer = regexp.MustCompile(`(?m)^(\s*)server app-[0-9]+:8080 resolve (.*);\n`)

// Nginx is the load balancer of the compose stack in front of the instances of a test.
type Nginx struct {
	URL string

	container testcontainers.Container
}

// StartNginx runs nginx with the compose configuration in front of cluster.
func StartNginx(t *testing.T, cluster []*Instance) *Nginx {
	t.Helper()
	conf, err := os.ReadFile(nginxConf)
	if err != nil {
		t.Fatalf("read %s: %v", nginxConf, err)
	}
	if n := len(upstreamServer.FindAll(conf, -1)); n != len(cluster) {
		t.Fatalf("%s lists %d replicas in its upstream, want one for each of the %d instances", nginxConf, n, len(cluster))
	}
	m := upstreamServer.FindSubmatch(conf)
	indent, params := string(m[1]), string(m[2])
	var servers strings.Builder
	for _, inst := range cluster {
		u, err := url.Parse(inst.URL)
		if err != nil {
			t.Fatalf("URL of %s: %v", inst.Name, err)
		}
		fmt.Fprintf(&servers, "%sserver %s:%s %s;\n", indent, hostAlias, u.Port(), params)
	}
	first := true
	adapted := upstreamServer.ReplaceAllFunc(conf, func([]byte) []byte {
		if !first {
			return nil
		}
		first = false
		return []byte(servers.String())
	})

	ctx := t.Context()
	c, err := testcontainers.Run(ctx, nginxImage,
		testcontainers.WithExposedPorts(nginxPort),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.ExtraHosts = append(hc.ExtraHosts, hostAlias+":host-gateway")
		}),
		testcontainers.WithEntrypoint("sh", "-c", fmt.Sprintf(pinIPv4, hostAlias, regexp.QuoteMeta(hostAlias))),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			Reader:            strings.NewReader(string(adapted)),
			ContainerFilePath: "/etc/nginx/nginx.conf",
			FileMode:          0o644,
		}),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/health/live").WithPort(nginxPort).WithStartupTimeout(time.Minute)),
	)
	t.Cleanup(func() {
		ctx := context.WithoutCancel(ctx)
		if t.Failed() && c != nil {
			t.Logf("log of nginx:\n%s", logOf(ctx, c))
		}
		if err := testcontainers.TerminateContainer(c); err != nil {
			t.Errorf("terminate nginx: %v", err)
		}
	})
	if err != nil {
		t.Fatalf("start nginx: %v", err)
	}
	endpoint, err := c.PortEndpoint(ctx, nginxPort, "http")
	if err != nil {
		t.Fatalf("nginx endpoint: %v", err)
	}
	return &Nginx{URL: endpoint, container: c}
}

// BehindNginx returns env adjusted for instances that run behind StartNginx.
func BehindNginx(env map[string]string) map[string]string {
	if runtime.GOOS != "linux" {
		return env
	}
	out := make(map[string]string, len(env)+1)
	maps.Copy(out, env)
	out["HTTP_ADDR"] = "0.0.0.0:0"
	return out
}

// Log returns what nginx has written so far.
func (n *Nginx) Log(t *testing.T) string {
	t.Helper()
	return logOf(t.Context(), n.container)
}

var connectFailure = regexp.MustCompile(`(?m)^.*connect\(\) (to \S+ )?failed.*$`)

// ConnectFailures returns the lines of log that record a failed connection to a replica.
func ConnectFailures(log string) []string {
	return connectFailure.FindAllString(log, -1)
}

func logOf(ctx context.Context, c testcontainers.Container) string {
	r, err := c.Logs(ctx)
	if err != nil {
		return fmt.Sprintf("(no log: %v)", err)
	}
	defer func() { _ = r.Close() }()
	b, err := io.ReadAll(r)
	if err != nil {
		return fmt.Sprintf("(log cut short: %v)\n%s", err, b)
	}
	return string(b)
}
