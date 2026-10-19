package archtest

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

const (
	otelSDK       = "go.opentelemetry.io/otel/sdk"
	otelExporters = "go.opentelemetry.io/otel/exporters"
)

type importRule struct {
	tree      string
	importers []string
}

func tracingRules(modulePath string) []importRule {
	var (
		bootstrap   = modulePath + "/internal/bootstrap"
		otlp        = modulePath + "/internal/platform/tracing/otlp"
		tracingtest = modulePath + "/internal/platform/tracing/tracingtest"
	)
	return []importRule{
		{tree: otelSDK, importers: []string{bootstrap, otlp, tracingtest}},
		{tree: otelExporters, importers: []string{bootstrap, otlp}},
		{tree: otlp, importers: []string{bootstrap}},
		{tree: tracingtest},
	}
}

func importViolations(rules []importRule, pkgs []listedPackage) []string {
	var violations []string
	for _, p := range pkgs {
		for _, imp := range p.imports {
			for _, rule := range rules {
				if !within(imp, rule.tree) || within(p.importPath, rule.tree) {
					continue
				}
				if !slices.ContainsFunc(rule.importers, func(dir string) bool { return within(p.importPath, dir) }) {
					violations = append(violations, fmt.Sprintf("%s imports %s", p.importPath, imp))
				}
			}
		}
	}
	return violations
}

func providerDependencies(rules []importRule, deps []string) []string {
	var violations []string
	for _, dep := range deps {
		if slices.ContainsFunc(rules, func(rule importRule) bool { return within(dep, rule.tree) }) {
			violations = append(violations, dep)
		}
	}
	return violations
}

func TestTracingSDKStaysInBootstrap(t *testing.T) {
	t.Parallel()

	root := moduleRoot(t)
	modulePath := strings.TrimSpace(goList(t, root, "-m"))

	out := goList(t, root, "-tags", "failpoints,load",
		"-f", `{{.ImportPath}} {{.Standard}} {{join .Imports ","}}`, "./...")
	pkgs, err := parseListedPackages(out)
	if err != nil {
		t.Fatal(err)
	}
	otlp := modulePath + "/internal/platform/tracing/otlp"
	if !slices.ContainsFunc(pkgs, func(p listedPackage) bool {
		return p.importPath == otlp && slices.ContainsFunc(p.imports, func(imp string) bool { return within(imp, otelSDK) })
	}) {
		t.Fatalf("go list did not report %s importing the SDK; got %d packages", otlp, len(pkgs))
	}

	if violations := importViolations(tracingRules(modulePath), pkgs); len(violations) > 0 {
		t.Errorf("only internal/bootstrap and internal/platform/tracing/otlp may import the OpenTelemetry SDK and "+
			"exporters, and only tests may import tracingtest, but:\n  %s", strings.Join(violations, "\n  "))
	}
}

func TestCoreDependsOnNoTracingProvider(t *testing.T) {
	t.Parallel()

	root := moduleRoot(t)
	modulePath := strings.TrimSpace(goList(t, root, "-m"))

	out := goList(t, root, "-deps", "-tags", "failpoints", "-f", "{{.ImportPath}}",
		"./internal/domain/...", "./internal/app/...", "./internal/adapter/...", "./internal/worker/...")
	deps := strings.Fields(out)
	if !slices.Contains(deps, modulePath+"/internal/platform/tracing") {
		t.Fatalf("go list did not report the core depending on internal/platform/tracing; got %d packages", len(deps))
	}

	if violations := providerDependencies(tracingRules(modulePath), deps); len(violations) > 0 {
		t.Errorf("internal/domain, app, adapter and worker must not depend, directly or through another package, on "+
			"the OpenTelemetry SDK, the exporters, internal/platform/tracing/otlp or tracingtest, but depend on:\n  %s",
			strings.Join(violations, "\n  "))
	}
}

func TestProviderDependencies(t *testing.T) {
	t.Parallel()

	const mod = "example.com/svc"
	rules := tracingRules(mod)

	tests := []struct {
		name string
		deps []string
		want []string
	}{
		{
			name: "the API, the auto-instrumentation SDK it links and the tracing package are allowed",
			deps: []string{
				"go.opentelemetry.io/otel/trace", "go.opentelemetry.io/otel/propagation", "go.opentelemetry.io/auto/sdk",
				mod + "/internal/platform/tracing", mod + "/internal/app/wagering",
			},
		},
		{
			name: "the SDK, the exporters, the provider and the test helper are refused at any depth",
			deps: []string{
				otelSDK + "/trace", otelExporters + "/otlp/otlptrace/otlptracehttp", mod + "/internal/app/wagering",
				mod + "/internal/platform/tracing/otlp", mod + "/internal/platform/tracing/tracingtest",
			},
			want: []string{
				otelSDK + "/trace", otelExporters + "/otlp/otlptrace/otlptracehttp",
				mod + "/internal/platform/tracing/otlp", mod + "/internal/platform/tracing/tracingtest",
			},
		},
		{
			name: "a package that only shares a prefix is allowed",
			deps: []string{otelSDK + "x", mod + "/internal/platform/tracing/otlpx"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := providerDependencies(rules, tc.deps); !slices.Equal(got, tc.want) {
				t.Errorf("providerDependencies() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestImportViolations(t *testing.T) {
	t.Parallel()

	const mod = "example.com/svc"
	rules := tracingRules(mod)

	tests := []struct {
		name string
		pkgs []listedPackage
		want []string
	}{
		{
			name: "the provider setup and the bootstrap may use the SDK and the exporter",
			pkgs: []listedPackage{
				{importPath: mod + "/internal/platform/tracing/otlp", imports: []string{
					otelSDK + "/trace", otelExporters + "/otlp/otlptrace/otlptracehttp", mod + "/internal/platform/tracing",
				}},
				{importPath: mod + "/internal/bootstrap", imports: []string{mod + "/internal/platform/tracing/otlp"}},
				{importPath: mod + "/internal/platform/tracing/tracingtest", imports: []string{otelSDK + "/trace/tracetest"}},
			},
		},
		{
			name: "the API is open to every package",
			pkgs: []listedPackage{
				{importPath: mod + "/internal/app/wagering", imports: []string{
					"go.opentelemetry.io/otel/trace", "go.opentelemetry.io/otel/attribute", mod + "/internal/platform/tracing",
				}},
			},
		},
		{
			name: "an adapter or a use case is refused the SDK, the exporter and the provider",
			pkgs: []listedPackage{
				{importPath: mod + "/internal/adapter/postgres", imports: []string{otelSDK + "/trace"}},
				{importPath: mod + "/internal/app/wagering", imports: []string{otelExporters + "/otlp/otlptrace"}},
				{importPath: mod + "/internal/worker/outboxrelay", imports: []string{mod + "/internal/platform/tracing/otlp"}},
			},
			want: []string{
				mod + "/internal/adapter/postgres imports " + otelSDK + "/trace",
				mod + "/internal/app/wagering imports " + otelExporters + "/otlp/otlptrace",
				mod + "/internal/worker/outboxrelay imports " + mod + "/internal/platform/tracing/otlp",
			},
		},
		{
			name: "the test helper is refused the exporter and to production code",
			pkgs: []listedPackage{
				{importPath: mod + "/internal/platform/tracing/tracingtest", imports: []string{otelExporters + "/otlp"}},
				{importPath: mod + "/internal/bootstrap", imports: []string{mod + "/internal/platform/tracing/tracingtest"}},
			},
			want: []string{
				mod + "/internal/platform/tracing/tracingtest imports " + otelExporters + "/otlp",
				mod + "/internal/bootstrap imports " + mod + "/internal/platform/tracing/tracingtest",
			},
		},
		{
			name: "a package that only shares a prefix is refused",
			pkgs: []listedPackage{
				{importPath: mod + "/internal/bootstrapx", imports: []string{otelSDK + "/trace"}},
				{importPath: mod + "/internal/platform/tracing/otlpx", imports: []string{otelSDK + "/trace"}},
			},
			want: []string{
				mod + "/internal/bootstrapx imports " + otelSDK + "/trace",
				mod + "/internal/platform/tracing/otlpx imports " + otelSDK + "/trace",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := importViolations(rules, tc.pkgs); !slices.Equal(got, tc.want) {
				t.Errorf("importViolations() = %q, want %q", got, tc.want)
			}
		})
	}
}
