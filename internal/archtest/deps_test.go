package archtest

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

const uuidModule = "github.com/google/uuid"

var forbiddenStdlib = []string{
	"crypto/rand", "database/sql", "io/fs", "io/ioutil", "log", "math/rand",
	"net", "os", "plugin", "syscall",
}

type listedPackage struct {
	importPath string
	standard   bool
	imports    []string
}

func parseListedPackages(out string) ([]listedPackage, error) {
	var pkgs []listedPackage
	for line := range strings.Lines(out) {
		fields := strings.Fields(line)
		if len(fields) < 2 || len(fields) > 3 {
			return nil, fmt.Errorf("unexpected go list line %q", strings.TrimSpace(line))
		}
		p := listedPackage{importPath: fields[0], standard: fields[1] == "true"}
		if len(fields) == 3 {
			p.imports = strings.Split(fields[2], ",")
		}
		pkgs = append(pkgs, p)
	}
	return pkgs, nil
}

func domainViolations(modulePath string, pkgs []listedPackage) []string {
	domain := modulePath + "/internal/domain"

	var violations []string
	for _, p := range slices.Backward(pkgs) {
		if p.importPath == uuidModule || within(p.importPath, domain) {
			continue
		}
		if p.standard && !forbidden(p.importPath) {
			continue
		}
		var importers []string
		for _, q := range pkgs {
			if slices.Contains(q.imports, p.importPath) && (!p.standard || within(q.importPath, domain)) {
				importers = append(importers, q.importPath)
			}
		}
		if len(importers) == 0 {
			continue
		}
		violations = append(violations, fmt.Sprintf("%s (imported by %s)", p.importPath, strings.Join(importers, ", ")))
	}
	return violations
}

func forbidden(importPath string) bool {
	return slices.ContainsFunc(forbiddenStdlib, func(tree string) bool { return within(importPath, tree) })
}

func TestDomainDependencies(t *testing.T) {
	t.Parallel()

	root := moduleRoot(t)
	modulePath := strings.TrimSpace(goList(t, root, "-m"))

	out := goList(t, root, "-deps", "-f", `{{.ImportPath}} {{.Standard}} {{join .Imports ","}}`, "./internal/domain/...")
	pkgs, err := parseListedPackages(out)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(pkgs, func(p listedPackage) bool { return p.importPath == modulePath+"/internal/domain/money" }) {
		t.Fatalf("go list did not report the money package; got %d packages", len(pkgs))
	}

	if violations := domainViolations(modulePath, pkgs); len(violations) > 0 {
		t.Errorf("internal/domain may import only the standard library short of its I/O, %s and itself, but depends on:\n  %s",
			uuidModule, strings.Join(violations, "\n  "))
	}
}

func TestDomainViolations(t *testing.T) {
	t.Parallel()

	const mod = "example.com/svc"

	tests := []struct {
		name string
		pkgs []listedPackage
		want []string
	}{
		{
			name: "standard library, uuid and the domain itself are allowed",
			pkgs: []listedPackage{
				{importPath: "fmt", standard: true},
				{importPath: "vendor/golang.org/x/net/idna", standard: true},
				{importPath: uuidModule},
				{importPath: mod + "/internal/domain/money", imports: []string{"fmt"}},
				{importPath: mod + "/internal/domain/wallet", imports: []string{mod + "/internal/domain/money", uuidModule}},
			},
		},
		{
			name: "another package of the module is refused",
			pkgs: []listedPackage{
				{importPath: mod + "/internal/platform/clock"},
				{importPath: mod + "/internal/domain/wallet", imports: []string{mod + "/internal/platform/clock"}},
			},
			want: []string{mod + "/internal/platform/clock (imported by " + mod + "/internal/domain/wallet)"},
		},
		{
			name: "a third-party package is refused",
			pkgs: []listedPackage{
				{importPath: "github.com/aws/aws-sdk-go-v2/aws"},
				{importPath: mod + "/internal/domain/event", imports: []string{"github.com/aws/aws-sdk-go-v2/aws"}},
			},
			want: []string{"github.com/aws/aws-sdk-go-v2/aws (imported by " + mod + "/internal/domain/event)"},
		},
		{
			name: "a sibling whose name starts with domain is refused",
			pkgs: []listedPackage{
				{importPath: mod + "/internal/domainx"},
				{importPath: mod + "/internal/domain/money", imports: []string{mod + "/internal/domainx"}},
			},
			want: []string{mod + "/internal/domainx (imported by " + mod + "/internal/domain/money)"},
		},
		{
			name: "an I/O package of the standard library is refused",
			pkgs: []listedPackage{
				{importPath: "net/http", standard: true},
				{importPath: mod + "/internal/domain/money", imports: []string{"fmt", "net/http"}},
			},
			want: []string{"net/http (imported by " + mod + "/internal/domain/money)"},
		},
		{
			name: "a package below a refused tree is refused",
			pkgs: []listedPackage{
				{importPath: "os/exec", standard: true},
				{importPath: "database/sql/driver", standard: true},
				{importPath: mod + "/internal/domain/wallet", imports: []string{"database/sql/driver", "os/exec"}},
			},
			want: []string{
				"database/sql/driver (imported by " + mod + "/internal/domain/wallet)",
				"os/exec (imported by " + mod + "/internal/domain/wallet)",
			},
		},
		{
			name: "the standard library may use those packages itself",
			pkgs: []listedPackage{
				{importPath: "os", standard: true},
				{importPath: "fmt", standard: true, imports: []string{"os"}},
				{importPath: mod + "/internal/domain/money", imports: []string{"fmt"}},
			},
		},
		{
			name: "a standard package that only shares a prefix is allowed",
			pkgs: []listedPackage{
				{importPath: "logx", standard: true},
				{importPath: mod + "/internal/domain/money", imports: []string{"logx"}},
			},
		},
		{
			name: "a package below the uuid path is refused",
			pkgs: []listedPackage{
				{importPath: uuidModule + "/v2"},
				{importPath: mod + "/internal/domain/wallet", imports: []string{uuidModule + "/v2"}},
			},
			want: []string{uuidModule + "/v2 (imported by " + mod + "/internal/domain/wallet)"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := domainViolations(mod, tc.pkgs); !slices.Equal(got, tc.want) {
				t.Errorf("domainViolations() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseListedPackages(t *testing.T) {
	t.Parallel()

	got, err := parseListedPackages("fmt true errors,io\nexample.com/svc/internal/domain/money false \nexample.com/svc/x false a,b\n")
	if err != nil {
		t.Fatalf("parseListedPackages() error = %v", err)
	}
	want := []listedPackage{
		{importPath: "fmt", standard: true, imports: []string{"errors", "io"}},
		{importPath: "example.com/svc/internal/domain/money"},
		{importPath: "example.com/svc/x", imports: []string{"a", "b"}},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d packages, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i].importPath != want[i].importPath || got[i].standard != want[i].standard || !slices.Equal(got[i].imports, want[i].imports) {
			t.Errorf("package %d = %+v, want %+v", i, got[i], want[i])
		}
	}

	if _, err := parseListedPackages("malformed\n"); err == nil {
		t.Error("parseListedPackages(malformed) error = nil, want an error")
	}
}
