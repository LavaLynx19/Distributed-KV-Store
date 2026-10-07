// Package purity guards the determinism rules of A§4.2: the pure packages
// must not reach for clocks, sockets, files, shared-memory primitives or
// unseeded randomness, and must not start goroutines.
package purity

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// purePackages are checked when they exist. internal/core always does.
var purePackages = []string{"core", "naive", "raft", "fsm", "tree"}

// forbidden import paths, and every package beneath them.
var forbidden = []string{"time", "net", "os", "sync", "math/rand"}

func isForbidden(path string) bool {
	for _, f := range forbidden {
		if path == f || strings.HasPrefix(path, f+"/") {
			return true
		}
	}
	return false
}

// violations lists every broken rule in the non-test Go files of dir.
func violations(t *testing.T, dir string) []string {
	t.Helper()
	fset := token.NewFileSet()
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	var found []string
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range f.Imports {
			if p := strings.Trim(imp.Path.Value, `"`); isForbidden(p) {
				found = append(found, fset.Position(imp.Pos()).String()+": imports "+p)
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			if g, ok := n.(*ast.GoStmt); ok {
				found = append(found, fset.Position(g.Pos()).String()+": starts a goroutine")
			}
			return true
		})
	}
	return found
}

func TestPurePackagesStayPure(t *testing.T) {
	for _, pkg := range purePackages {
		dir := filepath.Join("..", pkg)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			if pkg == "core" {
				t.Fatalf("%s is missing", dir)
			}
			continue
		}
		for _, v := range violations(t, dir) {
			t.Error(v)
		}
	}
}

// The guard itself must catch what it claims to.
func TestGuardCatchesViolations(t *testing.T) {
	dir := t.TempDir()
	src := `package bad

import (
	"math/rand/v2"
	"time"
)

func f() {
	go func() { _ = time.Now(); _ = rand.Uint64() }()
}
`
	if err := os.WriteFile(filepath.Join(dir, "bad.go"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	got := violations(t, dir)
	if len(got) != 3 {
		t.Fatalf("want 3 violations (two imports, one goroutine), got %d: %v", len(got), got)
	}
}
