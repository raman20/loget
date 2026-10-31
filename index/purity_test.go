package index

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
)

// TestPurityImports enforces "index = pure fold over the log": non-test index
// code may not import anything that could observe the outside world.
func TestPurityImports(t *testing.T) {
	allowed := map[string]bool{
		"bytes": true, "crypto/sha256": true, "encoding/binary": true, "encoding/json": true,
		"errors": true, "hash/crc32": true, "math": true, "slices": true, "sort": true, "strconv": true, "strings": true,
		"github.com/raman20/loget/format": true,
		// runtime.KeepAlive only: a run mapped from a file must stay
		// reachable while its memory is read (design 21).
		"runtime": true,
	}
	ents, _ := os.ReadDir(".")
	for _, e := range ents {
		n := e.Name()
		if !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), n, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(x ast.Node) bool {
			if s, ok := x.(*ast.SelectorExpr); ok {
				if id, ok := s.X.(*ast.Ident); ok && id.Name == "runtime" && s.Sel.Name != "KeepAlive" {
					t.Errorf("%s uses runtime.%s: only runtime.KeepAlive is allowed", n, s.Sel.Name)
				}
			}
			return true
		})
		for _, im := range f.Imports {
			p, _ := strconv.Unquote(im.Path.Value)
			if !allowed[p] {
				t.Errorf("%s imports %q: indexes must be pure (no clocks, randomness, IO, concurrency)", n, p)
			}
		}
	}
}
