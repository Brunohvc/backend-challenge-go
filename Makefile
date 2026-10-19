SHELL := /bin/bash
.DEFAULT_GOAL := test

COMPOSE ?= docker compose

N ?= 1

ARGS ?=
LOAD_COMPOSE := $(COMPOSE) -f docker-compose.yml -f test/load/compose.yaml

KEYCLOAK_URL ?= http://localhost:8080
TOKEN_URL := $(KEYCLOAK_URL)/realms/wagering/protocol/openid-connect/token
PROVIDER_A_SECRET ?= provider-a-secret
PROVIDER_B_SECRET ?= provider-b-secret
INTERNAL_SECRET ?= wallet-service-secret

.PHONY: up down migrate-up migrate-down token-provider-a token-provider-b token-internal \
	test test-race test-integration test-failpoints test-e2e load-test lint vet fmt

up:
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down -v

migrate-up:
	$(COMPOSE) run --rm --build migrate up

migrate-down:
	$(COMPOSE) run --rm --build migrate down $(N)

define fetch_token
set -o pipefail; \
token=$$(curl -sSf -u '$(1):$(2)' -d grant_type=client_credentials '$(TOKEN_URL)' \
	| sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p') || exit 1; \
if [ -z "$$token" ]; then \
	echo "no access_token for client $(1) in the response of $(TOKEN_URL)" >&2; exit 1; \
fi; \
echo "$$token"
endef

token-provider-a:
	@$(call fetch_token,provider-a,$(PROVIDER_A_SECRET))

token-provider-b:
	@$(call fetch_token,provider-b,$(PROVIDER_B_SECRET))

token-internal:
	@$(call fetch_token,wallet-service,$(INTERNAL_SECRET))

test:
	go test -tags=load ./...

test-race:
	go test -tags=load -race ./...

test-integration:
	go test -tags=integration -race ./...

test-failpoints:
	go test -tags=failpoints -race ./...

test-e2e:
	go test -tags=e2e -race -count=1 ./test/e2e/...

load-test: up
	mkdir -p tmp/load
	$(LOAD_COMPOSE) run --rm --build load $(ARGS)

lint:
	golangci-lint run --build-tags=integration,e2e,load ./...
	golangci-lint run --build-tags=integration,e2e,failpoints ./...

vet:
	go vet ./...

fmt:
	golangci-lint fmt
