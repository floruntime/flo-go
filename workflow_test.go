package flo

import (
	"bytes"
	"errors"
	"testing"
)

func TestExtractWorkflowMeta(t *testing.T) {
	tests := []struct {
		name        string
		yaml        string
		wantName    string
		wantVersion string
		wantErr     bool
	}{
		{
			name: "YAML unquoted",
			yaml: `kind: Workflow
name: order-pipeline
version: 2.1.0
steps:
  - name: validate`,
			wantName:    "order-pipeline",
			wantVersion: "2.1.0",
		},
		{
			name: "YAML quoted",
			yaml: `kind: Workflow
name: "my-workflow"
version: "1.0.0"`,
			wantName:    "my-workflow",
			wantVersion: "1.0.0",
		},
		{
			name: "YAML single-quoted",
			yaml: `kind: Workflow
name: 'deploy-service'
version: '3'`,
			wantName:    "deploy-service",
			wantVersion: "3",
		},
		{
			name: "JSON format",
			yaml: `{
  "kind": "Workflow",
  "name": "json-wf",
  "version": "1.2.3"
}`,
			wantName:    "json-wf",
			wantVersion: "1.2.3",
		},
		{
			name:    "missing name",
			yaml:    `version: 1.0.0`,
			wantErr: true,
		},
		{
			name:    "missing version",
			yaml:    `name: my-wf`,
			wantErr: true,
		},
		{
			name:    "empty",
			yaml:    ``,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, version, _, err := extractWorkflowMeta([]byte(tt.yaml))
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if name != tt.wantName {
				t.Errorf("name = %q, want %q", name, tt.wantName)
			}
			if version != tt.wantVersion {
				t.Errorf("version = %q, want %q", version, tt.wantVersion)
			}
		})
	}
}

func TestExtractYAMLField(t *testing.T) {
	yaml := `kind: Workflow
name: test-wf
version: "2.0"
# this is a comment
description: "a test workflow"`

	tests := []struct {
		field string
		want  string
	}{
		{"kind", "Workflow"},
		{"name", "test-wf"},
		{"version", "2.0"},
		{"description", "a test workflow"},
		{"nonexistent", ""},
	}

	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			got := extractYAMLField([]byte(yaml), tt.field)
			if got != tt.want {
				t.Errorf("extractYAMLField(%q) = %q, want %q", tt.field, got, tt.want)
			}
		})
	}
}

func TestUnquote(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`"hello"`, "hello"},
		{`'hello'`, "hello"},
		{`hello`, "hello"},
		{`""`, ""},
		{`"`, `"`},
	}
	for _, tt := range tests {
		got := unquote(tt.input)
		if got != tt.want {
			t.Errorf("unquote(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// workflowStatusAnswer is workflow status's answer as the server encodes
// it: an ended run, no input, its output and output_truncated when it has
// one.
func workflowStatusAnswer(output []byte, truncated byte) []byte {
	var w bodyWriter
	for _, f := range []string{"run-1", "orders", "1.0.0"} {
		w.bytes16([]byte(f))
	}
	w.u8(3) // completed
	w.bytes16([]byte("Done"))
	w.bytes32(nil) // an ended run's input
	w.u64(1000)
	w.u8(1)
	w.u64(1000)
	w.u8(1)
	w.u64(2000)
	w.u8(0) // no wait signal
	if output == nil {
		w.u8(0)
	} else {
		w.u8(1)
		w.bytes32(output)
		w.u8(truncated)
	}
	return w.buf
}

func TestWorkflowStatusDecodesOutputAndItsTruncatedFlag(t *testing.T) {
	preview := bytes.Repeat([]byte("o"), 256)
	for _, tr := range []byte{0, 1} {
		st, err := parseWorkflowStatus(workflowStatusAnswer(preview, tr))
		if err != nil || st.Status != "completed" || len(st.Input) != 0 || !bytes.Equal(st.Output, preview) || st.OutputTruncated != (tr == 1) || *st.CompletedAt != 2000 {
			t.Fatalf("truncated=%d: %+v, %v", tr, st, err)
		}
	}
	st, err := parseWorkflowStatus(workflowStatusAnswer(nil, 0))
	if err != nil || st.Output != nil || st.OutputTruncated {
		t.Fatalf("no output: %+v, %v", st, err)
	}
	whole := workflowStatusAnswer([]byte("out"), 1)
	for n := range whole {
		if _, err := parseWorkflowStatus(whole[:n]); err == nil {
			t.Fatalf("cut at %d decoded", n)
		}
	}
	for name, b := range map[string][]byte{
		"a flag of 2":        workflowStatusAnswer([]byte("out"), 2),
		"a byte past it":     append(whole, 0),
		"a byte past no out": append(workflowStatusAnswer(nil, 0), 0),
	} {
		if _, err := parseWorkflowStatus(b); !errors.Is(err, ErrIncompleteResponse) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if isTableOp(OpWorkflowStatus) {
		t.Fatal("workflow status is hand-coded, not a table op")
	}
}
