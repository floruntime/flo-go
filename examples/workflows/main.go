// Workflow example for the Flo Go SDK.
//
// This example exercises the full workflow lifecycle: sync, start, status, signal,
// history, listRuns, listDefinitions, disable/enable, cancel, and
// outcome-based routing.
//
// Usage:
//
//	# Terminal 1: start the action worker
//	cd sdks/go && go run ./examples/action_worker/
//
//	# Terminal 2: run this e2e test
//	cd sdks/go && go run ./examples/workflows/
//
// Prerequisites:
//   - A running Flo server on localhost:4453
//   - The action worker from examples/action_worker running
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

// =============================================================================
// Workflow YAML Definitions
// =============================================================================

const orderWorkflowYAML = `kind: Workflow
name: process-order
version: "1.0.0"
idempotency: required

start:
  run: "@actions/validate-order"
  transitions:
    success: charge_payment
    failure: flo.Failed

steps:
  charge_payment:
    run: "@actions/charge-payment"
    transitions:
      success: ship_order
      failure: flo.Failed

  ship_order:
    run: "@actions/ship-order"
    transitions:
      success: flo.Completed
      failure: flo.Failed
`

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

const signalTimeoutWorkflowYAML = `kind: Workflow
name: signal-timeout-test
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
      timeoutMs: 3000
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

const outcomeWorkflowYAML = `kind: Workflow
name: order-review
version: "1.0.0"

start:
  run: "@actions/review-order"
  transitions:
    approved: fulfill
    rejected: notify_rejection
    needs_review: manual_review
    failure: flo.Failed

steps:
  fulfill:
    run: "@actions/fulfill-order"
    transitions:
      success: flo.Completed
      failure: flo.Failed

  notify_rejection:
    run: "@actions/notify-rejection"
    transitions:
      success: flo.Completed
      failure: flo.Failed

  manual_review:
    run: "@actions/manual-review"
    transitions:
      success: flo.Completed
      failure: flo.Failed
`

// WorkflowStatus is an alias for the SDK's parsed status result.
type WorkflowStatus = flo.WorkflowStatusResult

// =============================================================================
// Binary parsers for history / listRuns / listDefinitions
// =============================================================================

type HistoryEvent struct {
	Type      string
	Detail    string
	Timestamp int64
}

type RunEntry struct {
	RunID     string
	Workflow  string
	Status    string
	CreatedAt int64
}

type DefinitionEntry struct {
	Name      string
	Version   string
	CreatedAt int64
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
		runID := string(data[pos : pos+ridLen])
		pos += ridLen

		wfLen := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
		pos += 2
		workflow := string(data[pos : pos+wfLen])
		pos += wfLen

		stLen := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
		pos += 2
		status := string(data[pos : pos+stLen])
		pos += stLen

		ts := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
		pos += 8

		runs = append(runs, RunEntry{RunID: runID, Workflow: workflow, Status: status, CreatedAt: ts})
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
		version := string(data[pos : pos+verLen])
		pos += verLen

		ts := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
		pos += 8

		defs = append(defs, DefinitionEntry{Name: name, Version: version, CreatedAt: ts})
	}
	return defs
}

// =============================================================================
// Helpers
// =============================================================================

func getStatus(client *flo.Client, runID string) WorkflowStatus {
	s, err := client.Workflow.Status(runID, nil)
	if err != nil {
		log.Fatalf("Status failed: %v", err)
	}
	return *s
}

func pollStatus(client *flo.Client, runID string, check func(WorkflowStatus) bool, timeoutSec int) WorkflowStatus {
	deadline := time.Now().Add(time.Duration(timeoutSec) * time.Second)
	s := getStatus(client, runID)
	for !check(s) && time.Now().Before(deadline) {
		time.Sleep(300 * time.Millisecond)
		s = getStatus(client, runID)
	}
	return s
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// =============================================================================
// Main
// =============================================================================

func main() {
	client := flo.NewClient(getEnv("FLO_ENDPOINT", "localhost:4453"),
		flo.WithNamespace(getEnv("FLO_NAMESPACE", "example")),
	)
	if err := client.Connect(); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()
	fmt.Println("Connected to Flo server")

	// =========================================================================
	// 1. Declarative Sync — safe to call on every boot
	// =========================================================================
	fmt.Println("\n=== Declarative Sync ===")

	r1, err := client.Workflow.SyncBytes([]byte(orderWorkflowYAML), nil)
	if err != nil {
		log.Fatalf("Sync failed: %v", err)
	}
	fmt.Printf("Synced %q v%s: %s\n", r1.Name, r1.Version, r1.Action)

	// Re-sync — should be "unchanged"
	r2, err := client.Workflow.SyncBytes([]byte(orderWorkflowYAML), nil)
	if err != nil {
		log.Fatalf("Re-sync failed: %v", err)
	}
	fmt.Printf("Re-sync %q v%s: %s\n", r2.Name, r2.Version, r2.Action)

	// Sync approval workflow
	r3, err := client.Workflow.SyncBytes([]byte(approvalWorkflowYAML), nil)
	if err != nil {
		log.Fatalf("Sync failed: %v", err)
	}
	fmt.Printf("Synced %q v%s: %s\n", r3.Name, r3.Version, r3.Action)

	// Sync outcome workflow
	r4, err := client.Workflow.SyncBytes([]byte(outcomeWorkflowYAML), nil)
	if err != nil {
		log.Fatalf("Sync failed: %v", err)
	}
	fmt.Printf("Synced %q v%s: %s\n", r4.Name, r4.Version, r4.Action)

	// Sync signal-timeout-test
	r5, err := client.Workflow.SyncBytes([]byte(signalTimeoutWorkflowYAML), nil)
	if err != nil {
		log.Fatalf("Sync failed: %v", err)
	}
	fmt.Printf("Synced %q v%s: %s\n", r5.Name, r5.Version, r5.Action)

	// =========================================================================
	// 2. List definitions
	// =========================================================================
	fmt.Println("\n=== List Definitions ===")

	defsData, err := client.Workflow.ListDefinitions(nil)
	if err != nil {
		log.Fatalf("ListDefinitions failed: %v", err)
	}
	defs := parseListDefinitions(defsData)
	for _, d := range defs {
		fmt.Printf("  %s v%s (created: %d)\n", d.Name, d.Version, d.CreatedAt)
	}

	// =========================================================================
	// 3. Get definition YAML
	// =========================================================================
	fmt.Println("\n=== Get Definition ===")

	yamlBytes, err := client.Workflow.GetDefinition("process-order", nil)
	if err != nil {
		log.Fatalf("GetDefinition failed: %v", err)
	}
	fmt.Printf("  Retrieved YAML (%d bytes)\n", len(yamlBytes))

	// =========================================================================
	// 4. Start a workflow run
	// =========================================================================
	fmt.Println("\n=== Start Workflow ===")

	input, _ := json.Marshal(map[string]interface{}{
		"orderId": "ORD-1234",
		"amount":  99.99,
	})
	runID, err := client.Workflow.Start("process-order", input, nil)
	if err != nil {
		log.Fatalf("Start failed: %v", err)
	}
	fmt.Printf("  Started run: %s\n", runID)

	// =========================================================================
	// 5. Check status
	// =========================================================================
	fmt.Println("\n=== Workflow Status ===")

	time.Sleep(2 * time.Second)
	s := getStatus(client, runID)
	fmt.Printf("  Run: %s\n", s.RunID)
	fmt.Printf("  Workflow: %s v%s\n", s.Workflow, s.Version)
	fmt.Printf("  Status: %s\n", s.Status)
	fmt.Printf("  Step: %s\n", s.CurrentStep)

	// Start a failing workflow (amount > $2000 → validate-order rejects)
	fmt.Println("\n=== Start Failing Workflow ===")

	failInput, _ := json.Marshal(map[string]interface{}{
		"orderId": "ORD-BIGSPEND",
		"amount":  5000,
	})
	failRunID, err := client.Workflow.Start("process-order", failInput, nil)
	if err != nil {
		log.Fatalf("Start (fail) failed: %v", err)
	}
	fmt.Printf("  Started run: %s\n", failRunID)
	time.Sleep(2 * time.Second)

	fs := getStatus(client, failRunID)
	fmt.Printf("  Status: %s (step: %s)\n", fs.Status, fs.CurrentStep)
	if fs.Status == "failed" {
		fmt.Println("  ✓ Workflow correctly failed (non-retryable error)")
	} else {
		fmt.Printf("  ⚠ Expected 'failed' but got '%s'\n", fs.Status)
	}

	failHistData, err := client.Workflow.History(failRunID, nil)
	if err == nil {
		for _, ev := range parseHistory(failHistData) {
			fmt.Printf("  [%s] %s\n", ev.Type, ev.Detail)
		}
	}

	// =========================================================================
	// 6. Signal tests
	// =========================================================================
	fmt.Println("\n=== Signal Tests ===")

	// 6a. Signal advances the workflow
	fmt.Println("\n  --- 6a. Signal Advances Workflow ---")

	sigInput, _ := json.Marshal(map[string]interface{}{
		"expense": "Travel",
		"amount":  500,
	})
	sigRunID, err := client.Workflow.Start("expense-approval", sigInput, nil)
	if err != nil {
		log.Fatalf("Start (signal) failed: %v", err)
	}
	fmt.Printf("  Started signal run: %s\n", sigRunID)

	// Wait for wait_for_approval
	ws := pollStatus(client, sigRunID, func(s WorkflowStatus) bool {
		return s.CurrentStep == "wait_for_approval" || s.Status == "waiting"
	}, 10)
	fmt.Printf("  Before signal: step=%s status=%s\n", ws.CurrentStep, ws.Status)

	// Send the approval signal
	sigData, _ := json.Marshal(map[string]interface{}{
		"approved": true,
		"approver": "manager@corp.com",
	})
	if err := client.Workflow.Signal(sigRunID, "approval_decision", sigData, nil); err != nil {
		log.Fatalf("Signal failed: %v", err)
	}
	fmt.Println("  Sent approval_decision signal")

	// Wait for completion
	ws = pollStatus(client, sigRunID, func(s WorkflowStatus) bool {
		return s.Status == "completed" || s.Status == "failed"
	}, 10)
	fmt.Printf("  After signal: step=%s status=%s\n", ws.CurrentStep, ws.Status)
	if ws.Status == "completed" {
		fmt.Println("  ✓ Signal correctly advanced the workflow to completion")
	} else {
		fmt.Printf("  ⚠ Expected 'completed' but got '%s'\n", ws.Status)
	}

	sigHistData, err := client.Workflow.History(sigRunID, nil)
	if err == nil {
		for _, ev := range parseHistory(sigHistData) {
			fmt.Printf("    [%s] %s\n", ev.Type, ev.Detail)
		}
	}

	// 6b. Signal timeout
	fmt.Println("\n  --- 6b. Signal Timeout ---")

	toInput, _ := json.Marshal(map[string]interface{}{
		"expense": "Conference",
		"amount":  200,
	})
	toRunID, err := client.Workflow.Start("signal-timeout-test", toInput, nil)
	if err != nil {
		log.Fatalf("Start (timeout) failed: %v", err)
	}
	fmt.Printf("  Started timeout run: %s\n", toRunID)

	// Wait for wait step
	ts := pollStatus(client, toRunID, func(s WorkflowStatus) bool {
		return s.CurrentStep == "wait_for_approval" || s.Status == "waiting"
	}, 10)
	fmt.Printf("  Waiting: step=%s status=%s\n", ts.CurrentStep, ts.Status)

	// Don't send a signal — let the 3s timeout expire
	fmt.Println("  Waiting for 3s timeout to expire...")
	time.Sleep(5 * time.Second)

	ts = getStatus(client, toRunID)
	fmt.Printf("  After timeout: step=%s status=%s\n", ts.CurrentStep, ts.Status)
	if ts.Status == "failed" || ts.Status == "timed_out" {
		fmt.Println("  ✓ Workflow correctly timed out")
	} else {
		fmt.Printf("  ⚠ Expected 'failed' or 'timed_out' but got '%s'\n", ts.Status)
	}

	toHistData, err := client.Workflow.History(toRunID, nil)
	if err == nil {
		for _, ev := range parseHistory(toHistData) {
			fmt.Printf("    [%s] %s\n", ev.Type, ev.Detail)
		}
	}

	// =========================================================================
	// 7. History
	// =========================================================================
	fmt.Println("\n=== Run History ===")

	histData, err := client.Workflow.History(runID, nil)
	if err != nil {
		log.Fatalf("History failed: %v", err)
	}
	for _, ev := range parseHistory(histData) {
		fmt.Printf("  [%d] %s: %s\n", ev.Timestamp, ev.Type, ev.Detail)
	}

	// =========================================================================
	// 8. List runs
	// =========================================================================
	fmt.Println("\n=== List Runs ===")

	runsData, err := client.Workflow.ListRuns("process-order", &flo.WorkflowListRunsOptions{Limit: 5})
	if err != nil {
		log.Fatalf("ListRuns failed: %v", err)
	}
	for _, r := range parseListRuns(runsData) {
		fmt.Printf("  %s — %s — %s (%d)\n", r.RunID, r.Workflow, r.Status, r.CreatedAt)
	}

	// =========================================================================
	// 9. Disable / Enable
	// =========================================================================
	fmt.Println("\n=== Disable / Enable ===")

	if err := client.Workflow.Disable("process-order", nil); err != nil {
		log.Fatalf("Disable failed: %v", err)
	}
	fmt.Println("  Disabled process-order")

	if err := client.Workflow.Enable("process-order", nil); err != nil {
		log.Fatalf("Enable failed: %v", err)
	}
	fmt.Println("  Re-enabled process-order")

	// =========================================================================
	// 10. Cancel a run
	// =========================================================================
	fmt.Println("\n=== Cancel Run ===")

	cancelInput, _ := json.Marshal(map[string]interface{}{"orderId": "ORD-9999"})
	cancelRunID, err := client.Workflow.Start("process-order", cancelInput, nil)
	if err != nil {
		log.Fatalf("Start (cancel) failed: %v", err)
	}
	if err := client.Workflow.Cancel(cancelRunID, nil); err != nil {
		log.Fatalf("Cancel failed: %v", err)
	}
	fmt.Printf("  Cancelled run: %s\n", cancelRunID)

	// =========================================================================
	// 11. Outcome-Based Routing
	// =========================================================================
	fmt.Println("\n=== Outcome-Based Routing ===")

	runOutcomeTest := func(label string, amount float64, orderID, expectedStep string) {
		inp, _ := json.Marshal(map[string]interface{}{
			"orderId": orderID,
			"amount":  amount,
		})
		rid, err := client.Workflow.Start("order-review", inp, nil)
		if err != nil {
			log.Fatalf("Start (%s) failed: %v", label, err)
		}
		fmt.Printf("  %s: started %s\n", label, rid)

		// Poll until past start step
		os := pollStatus(client, rid, func(s WorkflowStatus) bool {
			return s.CurrentStep != "start"
		}, 10)

		fmt.Printf("  %s: step=%s status=%s\n", label, os.CurrentStep, os.Status)
		if os.CurrentStep == expectedStep || os.Status == "completed" {
			fmt.Printf("  ✓ Routed correctly to %s\n", os.CurrentStep)
		} else {
			fmt.Printf("  ⚠ Expected step '%s' but got '%s'\n", expectedStep, os.CurrentStep)
		}

		hdata, err := client.Workflow.History(rid, nil)
		if err == nil {
			for _, ev := range parseHistory(hdata) {
				fmt.Printf("    [%s] %s\n", ev.Type, ev.Detail)
			}
		}
	}

	runOutcomeTest("Small  ($50) ", 50, "ORD-SMALL", "fulfill")
	runOutcomeTest("Large  ($750)", 750, "ORD-BIG", "notify_rejection")
	runOutcomeTest("Medium ($250)", 250, "ORD-MID", "manual_review")

	// =========================================================================
	// 12. SyncDir — sync all YAML files in the example directory
	// =========================================================================
	fmt.Println("\n=== SyncDir ===")

	// Sync the workflows directory (contains onboarding-workflow.yaml, order-workflow.yaml)
	syncDirResults, err := client.Workflow.SyncDir("examples/workflows", nil)
	if err != nil {
		log.Fatalf("SyncDir failed: %v", err)
	}
	fmt.Printf("  Synced %d workflow(s) from directory:\n", len(syncDirResults))
	for _, r := range syncDirResults {
		fmt.Printf("    %q v%s: %s\n", r.Name, r.Version, r.Action)
	}

	// Verify each synced workflow is retrievable by getting its definition
	fmt.Println("  Verifying definitions are stored:")
	allOk := true
	for _, r := range syncDirResults {
		yamlBytes, err := client.Workflow.GetDefinition(r.Name, nil)
		if err != nil {
			fmt.Printf("    ✗ %q — GetDefinition failed: %v\n", r.Name, err)
			allOk = false
			continue
		}
		if len(yamlBytes) == 0 {
			fmt.Printf("    ✗ %q — definition returned empty\n", r.Name)
			allOk = false
			continue
		}
		fmt.Printf("    ✓ %q — %d bytes stored\n", r.Name, len(yamlBytes))
	}

	// Re-run SyncDir — all results should be "unchanged"
	fmt.Println("  Re-syncing (expect all unchanged):")
	resync, err := client.Workflow.SyncDir("examples/workflows", nil)
	if err != nil {
		log.Fatalf("SyncDir re-sync failed: %v", err)
	}
	for _, r := range resync {
		if r.Action == "unchanged" {
			fmt.Printf("    ✓ %q: unchanged\n", r.Name)
		} else {
			fmt.Printf("    ⚠ %q: expected unchanged but got %q\n", r.Name, r.Action)
			allOk = false
		}
	}

	if allOk {
		fmt.Println("  ✓ SyncDir passed")
	} else {
		fmt.Println("  ✗ SyncDir had failures")
	}

	fmt.Println("\nDone ✓")
}
