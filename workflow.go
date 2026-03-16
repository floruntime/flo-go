package flo

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// WorkflowClient provides workflow operations.
type WorkflowClient struct {
	client *Client
}

// =============================================================================
// Core Workflow Operations
// =============================================================================

// Create creates (or replaces) a workflow from a YAML definition.
// The workflow name is extracted from the YAML and sent as the key.
func (w *WorkflowClient) Create(name string, yaml []byte, opts *WorkflowCreateOptions) error {
	if opts == nil {
		opts = &WorkflowCreateOptions{}
	}
	namespace := w.client.getNamespace(opts.Namespace)

	_, err := w.client.sendAndCheck(OpWorkflowCreate, namespace, []byte(name), yaml, nil, false)
	return err
}

// GetDefinition retrieves the YAML definition of a workflow by name.
// Returns the raw YAML bytes, or ErrNotFound if the workflow doesn't exist.
func (w *WorkflowClient) GetDefinition(name string, opts *WorkflowGetDefinitionOptions) ([]byte, error) {
	if opts == nil {
		opts = &WorkflowGetDefinitionOptions{}
	}
	namespace := w.client.getNamespace(opts.Namespace)

	// If a specific version is requested, send it as the value
	var value []byte
	if opts.Version != "" {
		value = []byte(opts.Version)
	}

	resp, err := w.client.sendAndCheck(OpWorkflowGetDefinition, namespace, []byte(name), value, nil, true)
	if err != nil {
		return nil, err
	}

	if resp.Status == StatusNotFound {
		return nil, ErrNotFound
	}

	return resp.Data, nil
}

// Start starts a workflow run with optional input data.
// Returns the run ID.
func (w *WorkflowClient) Start(name string, input []byte, opts *WorkflowStartOptions) (string, error) {
	if opts == nil {
		opts = &WorkflowStartOptions{}
	}
	namespace := w.client.getNamespace(opts.Namespace)

	// Build value: [has_idem:u8][idem_len:u16]?[idem]?[has_run_id:u8][run_id_len:u16]?[run_id]?[input...]
	value := make([]byte, 0, 4+len(input))

	// Idempotency key
	if opts.IdempotencyKey != "" {
		value = append(value, 1)
		idemBytes := []byte(opts.IdempotencyKey)
		value = append(value, byte(len(idemBytes)&0xFF), byte(len(idemBytes)>>8))
		value = append(value, idemBytes...)
	} else {
		value = append(value, 0)
	}

	// Explicit run ID
	if opts.RunID != "" {
		value = append(value, 1)
		ridBytes := []byte(opts.RunID)
		value = append(value, byte(len(ridBytes)&0xFF), byte(len(ridBytes)>>8))
		value = append(value, ridBytes...)
	} else {
		value = append(value, 0)
	}

	// Input payload
	value = append(value, input...)

	resp, err := w.client.sendAndCheck(OpWorkflowStart, namespace, []byte(name), value, nil, false)
	if err != nil {
		return "", err
	}

	return string(resp.Data), nil
}

// Status gets the status of a workflow run.
func (w *WorkflowClient) Status(runID string, opts *WorkflowStatusOptions) (*WorkflowStatusResult, error) {
	if opts == nil {
		opts = &WorkflowStatusOptions{}
	}
	namespace := w.client.getNamespace(opts.Namespace)

	resp, err := w.client.sendAndCheck(OpWorkflowStatus, namespace, []byte(runID), nil, nil, false)
	if err != nil {
		return nil, err
	}

	return parseWorkflowStatus(resp.Data)
}

var statusNames = []string{"pending", "running", "waiting", "completed", "failed", "cancelled", "timed_out"}

func parseWorkflowStatus(data []byte) (*WorkflowStatusResult, error) {
	pos := 0
	readU16 := func() (int, error) {
		if pos+2 > len(data) {
			return 0, errors.New("unexpected end of status data")
		}
		v := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
		pos += 2
		return v, nil
	}
	readStr := func() (string, error) {
		n, err := readU16()
		if err != nil {
			return "", err
		}
		if pos+n > len(data) {
			return "", errors.New("unexpected end of status data")
		}
		s := string(data[pos : pos+n])
		pos += n
		return s, nil
	}

	rid, err := readStr()
	if err != nil {
		return nil, fmt.Errorf("parse status run_id: %w", err)
	}
	workflow, err := readStr()
	if err != nil {
		return nil, fmt.Errorf("parse status workflow: %w", err)
	}
	version, err := readStr()
	if err != nil {
		return nil, fmt.Errorf("parse status version: %w", err)
	}

	if pos >= len(data) {
		return nil, errors.New("unexpected end of status data at status byte")
	}
	statusByte := data[pos]
	pos++
	statusStr := "unknown"
	if int(statusByte) < len(statusNames) {
		statusStr = statusNames[statusByte]
	}

	currentStep, err := readStr()
	if err != nil {
		return nil, fmt.Errorf("parse status current_step: %w", err)
	}

	if pos+4 > len(data) {
		return nil, errors.New("unexpected end of status data at input_len")
	}
	inputLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
	pos += 4
	if pos+inputLen > len(data) {
		return nil, errors.New("unexpected end of status data at input")
	}
	input := make([]byte, inputLen)
	copy(input, data[pos:pos+inputLen])
	pos += inputLen

	if pos+8 > len(data) {
		return nil, errors.New("unexpected end of status data at created_at")
	}
	createdAt := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
	pos += 8

	result := &WorkflowStatusResult{
		RunID:       rid,
		Workflow:    workflow,
		Version:     version,
		Status:      statusStr,
		CurrentStep: currentStep,
		Input:       input,
		CreatedAt:   createdAt,
	}

	// Optional: started_at
	if pos < len(data) && data[pos] == 1 {
		pos++
		if pos+8 > len(data) {
			return nil, errors.New("unexpected end of status data at started_at")
		}
		v := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
		pos += 8
		result.StartedAt = &v
	} else if pos < len(data) {
		pos++ // skip has_started=0
	}

	// Optional: completed_at
	if pos < len(data) && data[pos] == 1 {
		pos++
		if pos+8 > len(data) {
			return nil, errors.New("unexpected end of status data at completed_at")
		}
		v := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
		pos += 8
		result.CompletedAt = &v
	} else if pos < len(data) {
		pos++ // skip has_completed=0
	}

	// Optional: wait_signal
	if pos < len(data) && data[pos] == 1 {
		pos++
		ws, err := readStr()
		if err != nil {
			return nil, fmt.Errorf("parse status wait_signal: %w", err)
		}
		result.WaitSignal = &ws
	}

	return result, nil
}

// Signal sends a signal to a running workflow.
func (w *WorkflowClient) Signal(runID string, signalName string, data []byte, opts *WorkflowSignalOptions) error {
	if opts == nil {
		opts = &WorkflowSignalOptions{}
	}
	namespace := w.client.getNamespace(opts.Namespace)

	// Value format: [signal_name_len:u16][signal_name][data...]
	value := make([]byte, 0, 2+len(signalName)+len(data))
	value = append(value, byte(len(signalName)&0xFF), byte(len(signalName)>>8))
	value = append(value, []byte(signalName)...)
	value = append(value, data...)

	_, err := w.client.sendAndCheck(OpWorkflowSignal, namespace, []byte(runID), value, nil, false)
	return err
}

// Cancel cancels a running workflow.
func (w *WorkflowClient) Cancel(runID string, opts *WorkflowCancelOptions) error {
	if opts == nil {
		opts = &WorkflowCancelOptions{}
	}
	namespace := w.client.getNamespace(opts.Namespace)

	_, err := w.client.sendAndCheck(OpWorkflowCancel, namespace, []byte(runID), nil, nil, false)
	return err
}

// History retrieves the execution history of a workflow run.
// Returns the raw response bytes (binary format from server).
func (w *WorkflowClient) History(runID string, opts *WorkflowHistoryOptions) ([]byte, error) {
	if opts == nil {
		opts = &WorkflowHistoryOptions{}
	}
	namespace := w.client.getNamespace(opts.Namespace)

	resp, err := w.client.sendAndCheck(OpWorkflowHistory, namespace, []byte(runID), nil, nil, true)
	if err != nil {
		return nil, err
	}

	return resp.Data, nil
}

// ListRuns lists workflow runs, optionally filtered by workflow name.
// If name is empty, lists all runs across all workflows.
// Returns the raw response bytes (binary format from server).
func (w *WorkflowClient) ListRuns(name string, opts *WorkflowListRunsOptions) ([]byte, error) {
	if opts == nil {
		opts = &WorkflowListRunsOptions{}
	}
	namespace := w.client.getNamespace(opts.Namespace)

	// Build value with limit
	limit := opts.Limit
	if limit == 0 {
		limit = 100
	}
	value := make([]byte, 4)
	binary.LittleEndian.PutUint32(value, limit)

	resp, err := w.client.sendAndCheck(OpWorkflowListRuns, namespace, []byte(name), value, nil, false)
	if err != nil {
		return nil, err
	}

	return resp.Data, nil
}

// ListDefinitions lists all workflow definitions.
// Returns the raw response bytes (binary format from server).
func (w *WorkflowClient) ListDefinitions(opts *WorkflowListDefinitionsOptions) ([]byte, error) {
	if opts == nil {
		opts = &WorkflowListDefinitionsOptions{}
	}
	namespace := w.client.getNamespace(opts.Namespace)

	// Build value with limit
	limit := opts.Limit
	if limit == 0 {
		limit = 100
	}
	value := make([]byte, 4)
	binary.LittleEndian.PutUint32(value, limit)

	resp, err := w.client.sendAndCheck(OpWorkflowListDefinitions, namespace, nil, value, nil, false)
	if err != nil {
		return nil, err
	}

	return resp.Data, nil
}

// Disable disables a workflow definition (prevents new runs from starting).
func (w *WorkflowClient) Disable(name string, opts *WorkflowDisableOptions) error {
	if opts == nil {
		opts = &WorkflowDisableOptions{}
	}
	namespace := w.client.getNamespace(opts.Namespace)

	_, err := w.client.sendAndCheck(OpWorkflowDisable, namespace, []byte(name), nil, nil, false)
	return err
}

// Enable re-enables a previously disabled workflow definition.
func (w *WorkflowClient) Enable(name string, opts *WorkflowEnableOptions) error {
	if opts == nil {
		opts = &WorkflowEnableOptions{}
	}
	namespace := w.client.getNamespace(opts.Namespace)

	_, err := w.client.sendAndCheck(OpWorkflowEnable, namespace, []byte(name), nil, nil, false)
	return err
}

// =============================================================================
// Declarative Sync
// =============================================================================

// SyncResult describes what happened during a Sync operation.
type SyncResult struct {
	Name    string // Workflow name from YAML
	Version string // Version from YAML
	Action  string // "created", "updated", "unchanged"
}

// Sync performs a declarative, idempotent sync of a workflow YAML file.
//
// On every call (e.g. worker boot), Sync:
//  1. Reads and parses the YAML to extract name and version
//  2. Fetches the existing definition from the server
//  3. Compares versions:
//     - Not found → creates the workflow
//     - Same version → no-op (returns "unchanged")
//     - Different version → updates the workflow (upsert)
//
// This makes it safe to call on every startup. The version field in the
// YAML acts as a change guard — same version means no server round-trip
// beyond the initial GET.
func (w *WorkflowClient) Sync(yamlPath string, opts *WorkflowSyncOptions) (*SyncResult, error) {
	if opts == nil {
		opts = &WorkflowSyncOptions{}
	}

	// Read the YAML file
	yamlBytes, err := os.ReadFile(yamlPath)
	if err != nil {
		return nil, fmt.Errorf("flo: failed to read workflow file %s: %w", yamlPath, err)
	}

	return w.SyncBytes(yamlBytes, opts)
}

// SyncBytes is like Sync but accepts raw YAML bytes instead of a file path.
func (w *WorkflowClient) SyncBytes(yaml []byte, opts *WorkflowSyncOptions) (*SyncResult, error) {
	if opts == nil {
		opts = &WorkflowSyncOptions{}
	}

	// Extract name and version from YAML
	name, version, err := extractWorkflowMeta(yaml)
	if err != nil {
		return nil, err
	}

	namespace := w.client.getNamespace(opts.Namespace)

	// Check if the workflow already exists on the server
	existing, err := w.GetDefinition(name, &WorkflowGetDefinitionOptions{Namespace: namespace})
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, fmt.Errorf("flo: failed to check existing workflow %q: %w", name, err)
	}

	if existing != nil {
		// Workflow exists — check version
		existingVersion, _ := extractVersionFromYAML(existing)
		if existingVersion == version {
			return &SyncResult{Name: name, Version: version, Action: "unchanged"}, nil
		}
	}

	// Create or update (server's handleCreate is an upsert)
	if err := w.Create(name, yaml, &WorkflowCreateOptions{Namespace: namespace}); err != nil {
		return nil, fmt.Errorf("flo: failed to sync workflow %q: %w", name, err)
	}

	action := "created"
	if existing != nil {
		action = "updated"
	}

	return &SyncResult{Name: name, Version: version, Action: action}, nil
}

// SyncDir syncs all YAML files in a directory. Returns results for each file.
func (w *WorkflowClient) SyncDir(dir string, opts *WorkflowSyncOptions) ([]*SyncResult, error) {
	if opts == nil {
		opts = &WorkflowSyncOptions{}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("flo: failed to read directory %s: %w", dir, err)
	}

	var results []*SyncResult
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".yaml") || strings.HasSuffix(name, ".yml") {
			result, err := w.Sync(filepath.Join(dir, name), opts)
			if err != nil {
				return results, fmt.Errorf("flo: failed to sync %s: %w", name, err)
			}
			results = append(results, result)
		}
	}

	return results, nil
}

// =============================================================================
// YAML Metadata Extraction (lightweight — no full parser needed)
// =============================================================================

// extractWorkflowMeta extracts the name and version from workflow YAML.
// Works with both YAML and JSON formats.
func extractWorkflowMeta(data []byte) (name, version string, err error) {
	name = extractYAMLField(data, "name")
	version = extractYAMLField(data, "version")

	if name == "" {
		return "", "", fmt.Errorf("flo: workflow YAML missing required 'name' field")
	}
	if version == "" {
		return "", "", fmt.Errorf("flo: workflow YAML missing required 'version' field")
	}

	return name, version, nil
}

// extractVersionFromYAML extracts just the version field from YAML bytes.
func extractVersionFromYAML(data []byte) (string, error) {
	v := extractYAMLField(data, "version")
	if v == "" {
		return "", fmt.Errorf("flo: YAML missing 'version' field")
	}
	return v, nil
}

// extractYAMLField does a lightweight extraction of a top-level scalar field
// from YAML or JSON. Handles: `field: value`, `field: "value"`, `"field": "value"`.
// This avoids importing a full YAML parser for a simple metadata check.
func extractYAMLField(data []byte, field string) string {
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Skip comments and empty lines
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		// YAML: `field: value` or `field: "value"`
		if strings.HasPrefix(trimmed, field+":") {
			val := strings.TrimSpace(trimmed[len(field)+1:])
			return unquote(val)
		}

		// JSON: `"field": "value"` or `"field": value`
		jsonKey := `"` + field + `"`
		if strings.HasPrefix(trimmed, jsonKey) {
			rest := strings.TrimSpace(trimmed[len(jsonKey):])
			if strings.HasPrefix(rest, ":") {
				val := strings.TrimSpace(rest[1:])
				val = strings.TrimSuffix(val, ",")
				return unquote(val)
			}
		}
	}
	return ""
}

// unquote removes surrounding quotes from a string value.
func unquote(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}
