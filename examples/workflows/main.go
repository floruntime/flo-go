// Workflow orchestration example for the Flo Go SDK.
//
// This example walks through the core workflow lifecycle:
//
//  1. Sync a workflow definition (from YAML file or inline)
//  2. Start a workflow run with JSON input
//  3. Poll for completion and read output
//  4. Send a signal to advance a waiting workflow
//  5. Inspect run history
//  6. List runs and definitions
//
// Usage:
//
//	# Start the action worker (handles workflow steps):
//	cd sdks/go && go run ./examples/action_worker/
//
//	# In another terminal, run this example:
//	cd sdks/go && go run ./examples/workflows/
//
// Prerequisites:
//   - A running Flo server on localhost:4453 (or set FLO_ENDPOINT)
//   - The action worker from examples/action_worker running
//
// For comprehensive automated tests, see workflows_test.go.
package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	flo "github.com/floruntime/flo-go"
)

func main() {
	// -------------------------------------------------------------------------
	// Connect
	// -------------------------------------------------------------------------
	endpoint := getEnv("FLO_ENDPOINT", "localhost:4453")
	client := flo.NewClient(endpoint, flo.WithNamespace("example"))
	if err := client.Connect(); err != nil {
		log.Fatalf("Failed to connect to %s: %v", endpoint, err)
	}
	defer client.Close()
	fmt.Printf("Connected to Flo at %s\n", endpoint)

	// -------------------------------------------------------------------------
	// 1. Sync workflow definitions
	// -------------------------------------------------------------------------
	// Sync from a YAML file on disk — idempotent, safe to call on every boot.
	fmt.Println("\n--- Sync Definitions ---")

	r, err := client.Workflow.Sync("examples/workflows/order-workflow.yaml", nil)
	if err != nil {
		log.Fatalf("Sync failed: %v", err)
	}
	fmt.Printf("  %s v%s — %s\n", r.Name, r.Version, r.Action)

	// You can also sync inline YAML with SyncBytes:
	r, err = client.Workflow.SyncBytes([]byte(approvalWorkflowYAML), nil)
	if err != nil {
		log.Fatalf("SyncBytes failed: %v", err)
	}
	fmt.Printf("  %s v%s — %s\n", r.Name, r.Version, r.Action)

	// Or sync an entire directory of YAML files at once:
	results, err := client.Workflow.SyncDir("examples/workflows", nil)
	if err != nil {
		log.Fatalf("SyncDir failed: %v", err)
	}
	for _, sr := range results {
		fmt.Printf("  %s v%s — %s\n", sr.Name, sr.Version, sr.Action)
	}

	// -------------------------------------------------------------------------
	// 2. Start a workflow run
	// -------------------------------------------------------------------------
	fmt.Println("\n--- Start Workflow ---")

	input, _ := json.Marshal(map[string]interface{}{
		"orderId":    "ORD-1234",
		"customerId": "C-100",
		"amount":     99.99,
	})
	runID, err := client.Workflow.Start("order-processing", input, nil)
	if err != nil {
		log.Fatalf("Start failed: %v", err)
	}
	fmt.Printf("  Started run: %s\n", runID)

	// -------------------------------------------------------------------------
	// 3. Poll for completion
	// -------------------------------------------------------------------------
	fmt.Println("\n--- Poll Status ---")

	s := pollUntilDone(client, runID, 60)
	fmt.Printf("  Final: status=%s step=%s\n", s.Status, s.CurrentStep)
	if len(s.Output) > 0 {
		fmt.Printf("  Output: %s\n", s.Output)
	}

	// -------------------------------------------------------------------------
	// 4. Send a signal to a waiting workflow
	// -------------------------------------------------------------------------
	fmt.Println("\n--- Signal ---")

	// Start an approval workflow — it will pause at wait_for_approval.
	sigInput, _ := json.Marshal(map[string]interface{}{
		"expense": "Travel", "amount": 500,
	})
	sigRunID, err := client.Workflow.Start("expense-approval", sigInput, nil)
	if err != nil {
		log.Fatalf("Start failed: %v", err)
	}
	fmt.Printf("  Started approval run: %s\n", sigRunID)

	// Wait until the workflow reaches the signal step.
	pollUntilStep(client, sigRunID, "wait_for_approval", 60)
	fmt.Println("  Workflow is waiting for approval_decision signal")

	// Send the signal with a JSON payload.
	sigData, _ := json.Marshal(map[string]interface{}{
		"approved": true,
		"approver": "manager@corp.com",
	})
	if err := client.Workflow.Signal(sigRunID, "approval_decision", sigData, nil); err != nil {
		log.Fatalf("Signal failed: %v", err)
	}
	fmt.Println("  Sent approval_decision signal")

	ss := pollUntilDone(client, sigRunID, 60)
	fmt.Printf("  Final: status=%s\n", ss.Status)

	// -------------------------------------------------------------------------
	// 5. Inspect run history
	// -------------------------------------------------------------------------
	fmt.Println("\n--- History ---")

	histData, err := client.Workflow.History(runID, nil)
	if err != nil {
		log.Fatalf("History failed: %v", err)
	}
	for _, ev := range parseHistory(histData) {
		fmt.Printf("  [%s] %s\n", ev.Type, ev.Detail)
	}

	// -------------------------------------------------------------------------
	// 6. List runs and definitions
	// -------------------------------------------------------------------------
	fmt.Println("\n--- List Runs ---")

	runsData, err := client.Workflow.ListRuns("order-processing", &flo.WorkflowListRunsOptions{Limit: 5})
	if err != nil {
		log.Fatalf("ListRuns failed: %v", err)
	}
	for _, run := range parseListRuns(runsData) {
		fmt.Printf("  %s — %s — %s\n", run.RunID, run.Workflow, run.Status)
	}

	fmt.Println("\n--- List Definitions ---")

	defsData, err := client.Workflow.ListDefinitions(nil)
	if err != nil {
		log.Fatalf("ListDefinitions failed: %v", err)
	}
	for _, d := range parseListDefinitions(defsData) {
		fmt.Printf("  %s v%s\n", d.Name, d.Version)
	}

	fmt.Println("\nDone.")
}

// =============================================================================
// Inline workflow YAML (for the signal example above)
// =============================================================================

const approvalWorkflowYAML = `kind: Workflow
name: expense-approval
version: "1.0.0"

start:
  run: "@actions/validate-expense"
  transitions:
    success: wait_for_approval
    failure: flo.Failed

steps:
  wait_for_approval:
    waitForSignal:
      type: "approval_decision"
      timeoutMs: 86400000
      onTimeout: flo.Failed
    transitions:
      success: process_expense
      failure: flo.Failed

  process_expense:
    run: "@actions/process-expense"
    transitions:
      success: flo.Completed
      failure: flo.Failed
`

// =============================================================================
// Helpers
// =============================================================================

func pollUntilDone(client *flo.Client, runID string, timeoutSec int) *flo.WorkflowStatusResult {
	deadline := time.Now().Add(time.Duration(timeoutSec) * time.Second)
	for {
		s, err := client.Workflow.Status(runID, nil)
		if err != nil {
			log.Fatalf("Status failed: %v", err)
		}
		if s.Status == "completed" || s.Status == "failed" {
			return s
		}
		if time.Now().After(deadline) {
			log.Fatalf("Timed out waiting for run %s (last status: %s)", runID, s.Status)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func pollUntilStep(client *flo.Client, runID, step string, timeoutSec int) {
	deadline := time.Now().Add(time.Duration(timeoutSec) * time.Second)
	for {
		s, err := client.Workflow.Status(runID, nil)
		if err != nil {
			log.Fatalf("Status failed: %v", err)
		}
		if s.CurrentStep == step {
			return
		}
		if time.Now().After(deadline) {
			log.Fatalf("Timed out waiting for step %q (at %q)", step, s.CurrentStep)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// =============================================================================
// Binary response parsers
// =============================================================================

type HistoryEvent struct {
	Type, Detail string
	Timestamp    int64
}

type RunEntry struct {
	RunID, Workflow, Status string
	CreatedAt               int64
}

type DefinitionEntry struct {
	Name, Version string
	CreatedAt     int64
}

func parseHistory(data []byte) []HistoryEvent {
	if len(data) < 4 {
		return nil
	}
	count := binary.LittleEndian.Uint32(data[0:4])
	pos := 4
	events := make([]HistoryEvent, 0, count)
	for i := uint32(0); i < count; i++ {
		typeLen := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
		pos += 2
		typ := string(data[pos : pos+typeLen])
		pos += typeLen
		detailLen := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
		pos += 2
		detail := string(data[pos : pos+detailLen])
		pos += detailLen
		ts := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
		pos += 8
		events = append(events, HistoryEvent{Type: typ, Detail: detail, Timestamp: ts})
	}
	return events
}

func parseListRuns(data []byte) []RunEntry {
	if len(data) < 4 {
		return nil
	}
	count := binary.LittleEndian.Uint32(data[0:4])
	pos := 4
	runs := make([]RunEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		ridLen := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
		pos += 2
		rid := string(data[pos : pos+ridLen])
		pos += ridLen
		wfLen := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
		pos += 2
		wf := string(data[pos : pos+wfLen])
		pos += wfLen
		stLen := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
		pos += 2
		st := string(data[pos : pos+stLen])
		pos += stLen
		ts := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
		pos += 8
		runs = append(runs, RunEntry{RunID: rid, Workflow: wf, Status: st, CreatedAt: ts})
	}
	return runs
}

func parseListDefinitions(data []byte) []DefinitionEntry {
	if len(data) < 4 {
		return nil
	}
	count := binary.LittleEndian.Uint32(data[0:4])
	pos := 4
	defs := make([]DefinitionEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		nameLen := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
		pos += 2
		name := string(data[pos : pos+nameLen])
		pos += nameLen
		verLen := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
		pos += 2
		ver := string(data[pos : pos+verLen])
		pos += verLen
		ts := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
		pos += 8
		defs = append(defs, DefinitionEntry{Name: name, Version: ver, CreatedAt: ts})
	}
	return defs
}
