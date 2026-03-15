package flo

import "testing"

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
			name, version, err := extractWorkflowMeta([]byte(tt.yaml))
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
