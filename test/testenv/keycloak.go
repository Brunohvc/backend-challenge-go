//go:build integration || e2e

package testenv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// KeycloakAlias is the host name of Keycloak on the network passed to StartKeycloak.
const KeycloakAlias = "keycloak"

const (
	keycloakImage          = "quay.io/keycloak/keycloak:26.8.1"
	keycloakPort           = "8080/tcp"
	keycloakManagementPort = "9000/tcp"
	realmName              = "wagering"

	keycloakAdmin         = "admin"
	keycloakAdminPassword = "admin"

	keycloakStartTimeout = 3 * time.Minute
)

// Keycloak is a running Keycloak with the wagering realm imported.
type Keycloak struct {
	BaseURL   string
	Issuer    string
	JWKSURL   string
	Container testcontainers.Container
}

// StartKeycloak starts Keycloak with the wagering realm and waits until it is ready.
func StartKeycloak(ctx context.Context, network string) (*Keycloak, error) {
	root, err := repoRoot()
	if err != nil {
		return nil, err
	}
	opts := []testcontainers.ContainerCustomizer{
		testcontainers.WithCmd("start-dev", "--import-realm"),
		testcontainers.WithEnv(map[string]string{
			"KC_BOOTSTRAP_ADMIN_USERNAME": keycloakAdmin,
			"KC_BOOTSTRAP_ADMIN_PASSWORD": keycloakAdminPassword,
			"KC_HEALTH_ENABLED":           "true",
		}),
		testcontainers.WithExposedPorts(keycloakPort, keycloakManagementPort),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			HostFilePath:      filepath.Join(root, "deploy", "keycloak", "realm-wagering.json"),
			ContainerFilePath: "/opt/keycloak/data/import/realm-wagering.json",
			FileMode:          0o644,
		}),
		testcontainers.WithWaitStrategyAndDeadline(keycloakStartTimeout,
			wait.ForHTTP("/health/ready").WithPort(keycloakManagementPort).WithStartupTimeout(keycloakStartTimeout),
		),
	}
	if network != "" {
		opts = append(opts, tcnetwork.WithNetworkName([]string{KeycloakAlias}, network))
	}
	c, err := testcontainers.Run(ctx, keycloakImage, opts...)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("testenv: start Keycloak: %w", err), testcontainers.TerminateContainer(c))
	}
	base, err := c.PortEndpoint(ctx, keycloakPort, "http")
	if err != nil {
		return nil, errors.Join(fmt.Errorf("testenv: Keycloak endpoint: %w", err), testcontainers.TerminateContainer(c))
	}
	issuer := base + "/realms/" + realmName
	return &Keycloak{
		BaseURL:   base,
		Issuer:    issuer,
		JWKSURL:   issuer + "/protocol/openid-connect/certs",
		Container: c,
	}, nil
}

// Token returns an access token for the client through the client_credentials grant.
func (k *Keycloak) Token(ctx context.Context, clientID, secret string) (string, error) {
	form := url.Values{"grant_type": {"client_credentials"}}
	return k.token(ctx, k.Issuer+"/protocol/openid-connect/token", form, clientID, func(r *http.Request) {
		r.SetBasicAuth(clientID, secret)
	})
}

// AllowAdminOverHTTP lets the admin API be reached over plain HTTP from the host.
func (k *Keycloak) AllowAdminOverHTTP(ctx context.Context) error {
	const config = "/tmp/kcadm.config"
	for _, args := range [][]string{
		{"config", "credentials", "--server", "http://localhost:8080", "--realm", "master",
			"--user", keycloakAdmin, "--password", keycloakAdminPassword, "--config", config},
		{"update", "realms/master", "-s", "sslRequired=NONE", "--config", config},
	} {
		code, out, err := k.Container.Exec(ctx, append([]string{"/opt/keycloak/bin/kcadm.sh"}, args...), tcexec.Multiplexed())
		if err != nil {
			return fmt.Errorf("testenv: kcadm %s: %w", args[0], err)
		}
		if code != 0 {
			raw, _ := io.ReadAll(out)
			return fmt.Errorf("testenv: kcadm %s: exit status %d: %s", args[0], code, raw)
		}
	}
	return nil
}

// AdminToken returns an access token of Keycloak's administrator.
func (k *Keycloak) AdminToken(ctx context.Context) (string, error) {
	form := url.Values{
		"grant_type": {"password"},
		"client_id":  {"admin-cli"},
		"username":   {keycloakAdmin},
		"password":   {keycloakAdminPassword},
	}
	return k.token(ctx, k.BaseURL+"/realms/master/protocol/openid-connect/token", form, keycloakAdmin, func(*http.Request) {})
}

// CreateClient adds the client rep describes to the wagering realm and returns its URL.
func (k *Keycloak) CreateClient(ctx context.Context, rep map[string]any) (string, error) {
	resp, err := k.admin(ctx, http.MethodPost, k.BaseURL+"/admin/realms/"+realmName+"/clients", rep)
	if err != nil {
		return "", fmt.Errorf("testenv: create client: %w", err)
	}
	if resp.status != http.StatusCreated {
		return "", fmt.Errorf("testenv: create client: status %d: %s", resp.status, resp.body)
	}
	location := resp.header.Get("Location")
	if location == "" {
		return "", errors.New("testenv: create client: no Location in the response")
	}
	return location, nil
}

// DeleteClient removes the client at location, a URL CreateClient returned.
func (k *Keycloak) DeleteClient(ctx context.Context, location string) error {
	resp, err := k.admin(ctx, http.MethodDelete, location, nil)
	if err != nil {
		return fmt.Errorf("testenv: delete client: %w", err)
	}
	if resp.status != http.StatusNoContent {
		return fmt.Errorf("testenv: delete client: status %d: %s", resp.status, resp.body)
	}
	return nil
}

func (k *Keycloak) token(ctx context.Context, endpoint string, form url.Values, who string, auth func(*http.Request)) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("testenv: token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	auth(req)

	resp, err := sendKeycloak(req)
	if err != nil {
		return "", fmt.Errorf("testenv: token for %s: %w", who, err)
	}
	if resp.status != http.StatusOK {
		return "", fmt.Errorf("testenv: token for %s: status %d: %s", who, resp.status, resp.body)
	}
	var tok struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(resp.body, &tok); err != nil || tok.AccessToken == "" {
		return "", fmt.Errorf("testenv: token for %s: no access_token in response: %w", who, err)
	}
	return tok.AccessToken, nil
}

func (k *Keycloak) admin(ctx context.Context, method, target string, body any) (keycloakResponse, error) {
	var r io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return keycloakResponse{}, fmt.Errorf("encode request: %w", err)
		}
		r = bytes.NewReader(raw)
	}
	tok, err := k.AdminToken(ctx)
	if err != nil {
		return keycloakResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, method, target, r)
	if err != nil {
		return keycloakResponse{}, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return sendKeycloak(req)
}

type keycloakResponse struct {
	status int
	header http.Header
	body   []byte
}

func sendKeycloak(req *http.Request) (keycloakResponse, error) {
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return keycloakResponse{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return keycloakResponse{}, fmt.Errorf("read response: %w", err)
	}
	return keycloakResponse{status: resp.StatusCode, header: resp.Header, body: body}, nil
}

// Terminate stops and removes the container.
func (k *Keycloak) Terminate(context.Context) error {
	if err := testcontainers.TerminateContainer(k.Container); err != nil {
		return fmt.Errorf("testenv: terminate Keycloak: %w", err)
	}
	return nil
}
