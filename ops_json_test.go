package flo

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
)

type codeName struct {
	Code uint64 `json:"code"`
	Name string `json:"name"`
}

// The status codes and reasons are coded by hand; the server's ops.json
// (FLO_OPS_JSON, from `zig build gen`) lists its own. This reads the
// constants from the source, so one added, dropped or renumbered on either
// side fails here. A constant's name is its type's name and the server's
// name in CamelCase: not_found is StatusNotFound, cas_mismatch ReasonCASMismatch.
func TestStatusesAndReasonsMatchTheServer(t *testing.T) {
	path := os.Getenv("FLO_OPS_JSON")
	if path == "" {
		if os.Getenv("CI") == "1" {
			t.Fatal("CI=1 but FLO_OPS_JSON is not set")
		}
		t.Skip("FLO_OPS_JSON is not set")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var ops struct {
		Statuses []codeName `json:"statuses"`
		Reasons  []codeName `json:"reasons"`
	}
	if err := json.Unmarshal(raw, &ops); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	if len(ops.Statuses) == 0 || len(ops.Reasons) == 0 {
		t.Fatalf("%s lists no statuses or no reasons", path)
	}
	sdk := constsOfType(t, "types.go", "StatusCode", "Reason")
	compare(t, "status", "Status", ops.Statuses, sdk["StatusCode"])
	compare(t, "reason", "Reason", ops.Reasons, sdk["Reason"])
	for _, r := range ops.Reasons {
		if got := Reason(r.Code).String(); got != r.Name {
			t.Errorf("Reason(%d).String() = %q, want %q", r.Code, got, r.Name)
		}
	}
}

func compare(t *testing.T, what, prefix string, server []codeName, sdk map[uint64]string) {
	t.Helper()
	seen := map[uint64]bool{}
	for _, s := range server {
		seen[s.Code] = true
		ident, ok := sdk[s.Code]
		if !ok {
			t.Errorf("server %s %d %s: no SDK constant", what, s.Code, s.Name)
			continue
		}
		if !strings.EqualFold(prefix+strings.ReplaceAll(s.Name, "_", ""), ident) {
			t.Errorf("server %s %d is %s, SDK's is %s", what, s.Code, s.Name, ident)
		}
	}
	for code, ident := range sdk {
		if !seen[code] {
			t.Errorf("SDK %s %s = %d: not the server's", what, ident, code)
		}
	}
}

// constsOfType maps each of types to its constants' values and names in file.
func constsOfType(t *testing.T, file string, types ...string) map[string]map[uint64]string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]map[uint64]string{}
	for _, typ := range types {
		out[typ] = map[uint64]string{}
	}
	for _, decl := range f.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			vs := spec.(*ast.ValueSpec)
			typ, ok := vs.Type.(*ast.Ident)
			if !ok || out[typ.Name] == nil {
				continue
			}
			if len(vs.Names) != 1 || len(vs.Values) != 1 {
				t.Fatalf("%s: a %s constant without one name and one value", file, typ.Name)
			}
			lit, ok := vs.Values[0].(*ast.BasicLit)
			if !ok || lit.Kind != token.INT {
				t.Fatalf("%s: %s is not an integer literal", file, vs.Names[0].Name)
			}
			v, err := strconv.ParseUint(lit.Value, 0, 64)
			if err != nil {
				t.Fatal(err)
			}
			if prev, dup := out[typ.Name][v]; dup {
				t.Fatalf("%s: %s and %s are both %d", file, prev, vs.Names[0].Name, v)
			}
			out[typ.Name][v] = vs.Names[0].Name
		}
	}
	return out
}
