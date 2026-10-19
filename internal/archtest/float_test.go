package archtest

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

type floatUse struct {
	pos  token.Position
	what string
}

var floatSelectors = map[string]func(name string) bool{
	"strconv": func(name string) bool {
		return name == "ParseFloat" || name == "FormatFloat" || name == "AppendFloat"
	},
	"math": func(name string) bool {
		return strings.HasPrefix(name, "Float64") || strings.HasPrefix(name, "Float32")
	},
}

func findFloatUses(fset *token.FileSet, f *ast.File) []floatUse {
	names := make(map[string]string)
	for _, imp := range f.Imports {
		importPath, err := strconv.Unquote(imp.Path.Value)
		if err != nil || floatSelectors[importPath] == nil {
			continue
		}
		local := path.Base(importPath)
		if imp.Name != nil {
			local = imp.Name.Name
		}
		names[local] = importPath
	}

	var uses []floatUse
	ast.Inspect(f, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.Ident:
			if n.Name == "float32" || n.Name == "float64" {
				uses = append(uses, floatUse{pos: fset.Position(n.Pos()), what: n.Name})
			}
		case *ast.BasicLit:
			if n.Kind == token.FLOAT && strings.Contains(n.Value, ".") {
				uses = append(uses, floatUse{pos: fset.Position(n.Pos()), what: "float literal " + n.Value})
			}
		case *ast.SelectorExpr:
			pkg, ok := n.X.(*ast.Ident)
			if !ok {
				break
			}
			if importPath, ok := names[pkg.Name]; ok && floatSelectors[importPath](n.Sel.Name) {
				uses = append(uses, floatUse{pos: fset.Position(n.Pos()), what: importPath + "." + n.Sel.Name})
			}
		}
		return true
	})
	return uses
}

func skipDir(rel string) bool {
	name := path.Base(rel)
	switch {
	case name == "vendor", name == "testdata":
		return true
	case strings.HasPrefix(name, "."), strings.HasPrefix(name, "_"):
		return true
	}
	return false
}

var floatAllowedDirs = []string{"internal/platform/metrics"}

func floatAllowed(rel string) bool {
	return slices.ContainsFunc(floatAllowedDirs, func(dir string) bool { return within(rel, dir) })
}

func scanFloatUses(root string) (scanned int, violations []string, err error) {
	fset := token.NewFileSet()

	err = filepath.WalkDir(root, func(file string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, file)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)

		if d.IsDir() {
			if rel != "." && skipDir(rel) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, "_test.go") || floatAllowed(rel) {
			return nil
		}

		f, err := parser.ParseFile(fset, file, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		scanned++
		for _, use := range findFloatUses(fset, f) {
			violations = append(violations, fmt.Sprintf("%s:%d:%d: %s", rel, use.pos.Line, use.pos.Column, use.what))
		}
		return nil
	})
	return scanned, violations, err
}

func TestNoFloatInProduction(t *testing.T) {
	t.Parallel()

	root := moduleRoot(t)

	scanned, violations, err := scanFloatUses(root)
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	if scanned == 0 {
		t.Fatalf("no production Go files found under %s", root)
	}

	if len(violations) > 0 {
		t.Errorf("floating point in production code (money is int64 minor units):\n  %s",
			strings.Join(violations, "\n  "))
	}
}

func writeTree(t *testing.T, files map[string]string) string {
	t.Helper()

	root := t.TempDir()
	for name, src := range files {
		file := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(file), 0o750); err != nil {
			t.Fatalf("MkdirAll() error = %v", err)
		}
		if err := os.WriteFile(file, []byte(src), 0o600); err != nil {
			t.Fatalf("WriteFile() error = %v", err)
		}
	}
	return root
}

func TestScanFloatUses_AllowsFloatOnlyInTelemetry(t *testing.T) {
	t.Parallel()

	const withFloat = "package p\n\nvar v float64\n"

	root := writeTree(t, map[string]string{
		"internal/platform/metrics/metrics.go":      withFloat,
		"internal/platform/metrics/sub/sub.go":      withFloat,
		"internal/platform/metricsx/metricsx.go":    withFloat,
		"internal/platform/clock/clock.go":          withFloat,
		"internal/domain/money/money.go":            withFloat,
		"internal/platform/metrics/metrics_test.go": withFloat,
	})

	scanned, violations, err := scanFloatUses(root)
	if err != nil {
		t.Fatalf("scanFloatUses() error = %v", err)
	}

	want := []string{
		"internal/domain/money/money.go:3:7: float64",
		"internal/platform/clock/clock.go:3:7: float64",
		"internal/platform/metricsx/metricsx.go:3:7: float64",
	}
	if !slices.Equal(violations, want) {
		t.Errorf("violations = %q, want %q", violations, want)
	}
	if scanned != len(want) {
		t.Errorf("scanned = %d, want %d: exempt files must not be counted", scanned, len(want))
	}
}

func TestScanFloatUses_Literals(t *testing.T) {
	t.Parallel()

	const (
		priceLiteral    = "package p\n\nvar price = 19.90\n"
		exponentLiteral = "package p\n\nconst nanos = 1e9\n"
	)

	root := writeTree(t, map[string]string{
		"internal/domain/money/price.go":        priceLiteral,
		"internal/app/wager/amount.go":          priceLiteral,
		"internal/platform/metrics/buckets.go":  priceLiteral,
		"internal/platform/metricsx/buckets.go": priceLiteral,
		"internal/platform/clock/nanos.go":      exponentLiteral,
		"internal/domain/money/price_test.go":   priceLiteral,
	})

	scanned, violations, err := scanFloatUses(root)
	if err != nil {
		t.Fatalf("scanFloatUses() error = %v", err)
	}

	want := []string{
		"internal/app/wager/amount.go:3:13: float literal 19.90",
		"internal/domain/money/price.go:3:13: float literal 19.90",
		"internal/platform/metricsx/buckets.go:3:13: float literal 19.90",
	}
	if !slices.Equal(violations, want) {
		t.Errorf("violations = %q, want %q", violations, want)
	}
	if scanned != 4 {
		t.Errorf("scanned = %d, want 4", scanned)
	}
}

func TestFloatAllowed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rel  string
		want bool
	}{
		{"internal/platform/metrics/metrics.go", true},
		{"internal/platform/metrics/sub/sub.go", true},
		{"internal/platform/metricsx/metricsx.go", false},
		{"internal/platform/clock/clock.go", false},
		{"internal/domain/money/money.go", false},
		{"cmd/api/main.go", false},
	}
	for _, tc := range tests {
		if got := floatAllowed(tc.rel); got != tc.want {
			t.Errorf("floatAllowed(%q) = %v, want %v", tc.rel, got, tc.want)
		}
	}
}

func TestFindFloatUses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want []string
	}{
		{
			name: "float types",
			src: `package p
var a float64
var b float32
func f(x float64) float32 { return 0 }`,
			want: []string{"float64", "float32", "float64", "float32"},
		},
		{
			name: "strconv conversions",
			src: `package p
import "strconv"
var _, _ = strconv.ParseFloat("1", 64)
var _ = strconv.FormatFloat(1, 'f', 2, 64)
var _ = strconv.AppendFloat(nil, 1, 'f', 2, 64)`,
			want: []string{"strconv.ParseFloat", "strconv.FormatFloat", "strconv.AppendFloat"},
		},
		{
			name: "math bit conversions",
			src: `package p
import "math"
var _ = math.Float64bits(1)
var _ = math.Float64frombits(1)
var _ = math.Float32bits(1)
var _ = math.Float32frombits(1)`,
			want: []string{"math.Float64bits", "math.Float64frombits", "math.Float32bits", "math.Float32frombits"},
		},
		{
			name: "renamed import",
			src: `package p
import conv "strconv"
var _ = conv.FormatFloat(1, 'f', 2, 64)`,
			want: []string{"strconv.FormatFloat"},
		},
		{
			name: "float literals with a decimal point",
			src: `package p
var price = 19.90
var cents = int64(price * 100)
const half = .5
const hex = 0x1.8p1
func f() { _ = []int{1}[0] + int(2.0) }`,
			want: []string{"float literal 19.90", "float literal .5", "float literal 0x1.8p1", "float literal 2.0"},
		},
		{
			name: "exponent literals, integers and text are not float literals",
			src: `package p
const (
	nano  = 1e9
	big   = 5E3
	hex   = 0x1p-2
	count = 100
	imag  = 2i
	text  = "19.90"
	char  = '.'
)`,
		},
		{
			name: "integer work and duration methods are fine",
			src: `package p
import (
	"math"
	"strconv"
	"time"
)
func f(d time.Duration) (int64, string, int) {
	_ = d.Seconds()
	return math.MaxInt64, strconv.FormatInt(1, 10), math.MaxInt
}`,
		},
		{
			name: "same names on other values are fine",
			src: `package p
type parser struct{}
func (parser) ParseFloat() {}
func f(p parser, math struct{ Float64bits func() }) {
	p.ParseFloat()
	math.Float64bits()
}`,
		},
		{
			name: "comments and strings are not code",
			src: `package p
// float64 is never used here, and neither is strconv.ParseFloat.
var s = "float64"`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, "p.go", tc.src, parser.SkipObjectResolution)
			if err != nil {
				t.Fatalf("ParseFile() error = %v", err)
			}
			var got []string
			for _, use := range findFloatUses(fset, f) {
				got = append(got, use.what)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("findFloatUses() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSkipDir(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rel  string
		want bool
	}{
		{"test", false},
		{"test/load", false},
		{"vendor", true},
		{"internal/domain/event/testdata", true},
		{".git", true},
		{".cache", true},
		{"_scratch", true},
		{"cmd", false},
		{"internal/domain/money", false},
		{"internal/test", false},
		{"internal/vendored", false},
	}
	for _, tc := range tests {
		if got := skipDir(tc.rel); got != tc.want {
			t.Errorf("skipDir(%q) = %v, want %v", tc.rel, got, tc.want)
		}
	}
}
