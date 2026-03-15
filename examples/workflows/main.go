// Example: Workflow API usage with the Flo Go SDK
//
// This example demonstrates the declarative workflow sync pattern:
// call Sync/SyncDir on startup and the SDK will create or update
// only the workflows whose version has changed.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	flo "github.com/floruntime/flo-go"
)

func main() {
	// Create and connect client
	client := flo.NewClient(getEnv("FLO_ENDPOINT", "localhost:3000"),
		flo.WithNamespace(getEnv("FLO_NAMESPACE", "myapp")),
	)
	if err := client.Connect(); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	// =========================================================================
	// Declarative Sync — run on every boot
	// =========================================================================

	fmt.Println("=== Syncing Workflow Definitions ===")

	// Option A: sync a whole directory of YAML files
	results, err := client.Workflow.SyncDir("./workflows", nil)
	if err != nil {
		log.Fatalf("SyncDir failed: %v", err)
	}
	for _, r := range results {
		fmt.Printf("  %s v%s → %s\n", r.Name, r.Version, r.Action)
	}

	// Option B: sync a single file
	result, err := client.Workflow.Sync("./workflows/order-workflow.yaml", nil)
	if err != nil {
		log.Fatalf("Sync failed: %v", err)
	}
	fmt.Printf("  %s v%s → %s\n", result.Name, result.Version, result.Action)

	// Option C: sync from embedded bytes (useful for go:embed)
	yamlBytes := []byte(`
kind: workflow
name: inline-example
version: "0.1.0"
initial_state: start
states:
  start:
    action: say-hello
    on_success: done
  done:
    terminal: true
`)
	result, err = client.Workflow.SyncBytes(yamlBytes, nil)
	if err != nil {
		log.Fatalf("SyncBytes failed: %v", err)
	}
	fmt.Printf("  %s v%s → %s\n", result.Name, result.Version, result.Action)

	// =========================================================================
	// Imperative Operations
	// =========================================================================

	fmt.Println("\n=== Starting a Workflow Run ===")

	// Start a workflow run with input data
	input, _ := json.Marshal(map[string]interface{}{
		"order_id":    "ORD-12345",
		"customer_id": "CUST-67890",
		"amount":      299.99,
		"items": []map[string]interface{}{
			{"sku": "WIDGET-A", "quantity": 2},
			{"sku": "GADGET-B", "quantity": 1},
		},
	})

	runID, err := client.Workflow.Start("order-processing", input, &flo.WorkflowStartOptions{
		IdempotencyKey: "ORD-12345-run-1", // prevents duplicate runs
	})
	if err != nil {
		log.Fatalf("Start failed: %v", err)
	}
	fmt.Printf("Started run: %s\n", runID)

	// Check status
	status, err := client.Workflow.Status(runID, nil)
	if err != nil {
		log.Fatalf("Status failed: %v", err)
	}
	fmt.Printf("Status: %s\n", status)

	// =========================================================================
	// Signals — send external input to a running workflow
	// =========================================================================

	fmt.Println("\n=== Sending a Signal ===")

	// If the workflow is in a wait_for_signal state, this unblocks it
	signalData, _ := json.Marshal(map[string]string{
		"decision": "approve",
		"reviewer": "admin@example.com",
	})
	if err := client.Workflow.Signal(runID, "review_decision", signalData, nil); err != nil {
		log.Fatalf("Signal failed: %v", err)
	}
	fmt.Println("Signal sent")

	// =========================================================================
	// Lifecycle: Disable / Enable / Cancel
	// =========================================================================

	fmt.Println("\n=== Lifecycle Operations ===")

	// Disable a workflow (no new runs can start)
	if err := client.Workflow.Disable("order-processing", nil); err != nil {
		log.Fatalf("Disable failed: %v", err)
	}
	fmt.Println("Disabled order-processing")

	// Re-enable it
	if err := client.Workflow.Enable("order-processing", nil); err != nil {
		log.Fatalf("Enable failed: %v", err)
	}
	fmt.Println("Re-enabled order-processing")

	// Cancel a running workflow
	if err := client.Workflow.Cancel(runID, nil); err != nil {
		log.Fatalf("Cancel failed: %v", err)
	}
	fmt.Println("Cancelled run:", runID)

	// =========================================================================
	// Retrieve a stored definition
	// =========================================================================

	fmt.Println("\n=== Get Definition ===")

	yaml, err := client.Workflow.GetDefinition("order-processing", nil)
	if err != nil {
		log.Fatalf("GetDefinition failed: %v", err)
	}
	fmt.Printf("Definition (%d bytes):\n%s\n", len(yaml), yaml)

	// Get a specific version
	yaml, err = client.Workflow.GetDefinition("order-processing", &flo.WorkflowGetDefinitionOptions{
		Version: "1.2.0",
	})
	if err != nil {
		if err == flo.ErrNotFound {
			fmt.Println("Version 1.2.0 not found")
		} else {
			log.Fatalf("GetDefinition with version failed: %v", err)
		}
	} else {
		fmt.Printf("Got version 1.2.0 (%d bytes)\n", len(yaml))
	}

	// =========================================================================
	// Production pattern: sync on boot, then run worker loop
	// =========================================================================

	fmt.Println("\n=== Production Pattern ===")
	productionBoot(client)
}

// productionBoot shows the recommended pattern: sync definitions on startup,
// then start an action worker to handle the workflow steps.
func productionBoot(client *flo.Client) {
	// 1. Sync all workflow definitions
	results, err := client.Workflow.SyncDir("./workflows", nil)
	if err != nil {
		log.Fatalf("Failed to sync workflows: %v", err)
	}
	for _, r := range results {
		log.Printf("workflow %s v%s: %s", r.Name, r.Version, r.Action)
	}

	// 2. Create an action worker to handle the workflow steps
	worker, err := client.NewActionWorker(flo.ActionWorkerOptions{
		Concurrency: 10,
	})
	if err != nil {
		log.Fatalf("Failed to create worker: %v", err)
	}
	defer worker.Close()

	// 3. Register handlers for each action referenced in the workflows
	worker.MustRegisterAction("validate-payment", func(ctx *flo.ActionContext) ([]byte, error) {
		log.Printf("Validating payment for %s", ctx.Input)
		time.Sleep(100 * time.Millisecond)
		return json.Marshal(map[string]string{"status": "valid"})
	})
	worker.MustRegisterAction("process-payment", func(ctx *flo.ActionContext) ([]byte, error) {
		log.Printf("Processing payment for %s", ctx.Input)
		time.Sleep(200 * time.Millisecond)
		return json.Marshal(map[string]string{"charged": "true"})
	})
	worker.MustRegisterAction("fulfill-order", func(ctx *flo.ActionContext) ([]byte, error) {
		log.Printf("Fulfilling order %s", ctx.Input)
		return json.Marshal(map[string]string{"shipped": "true"})
	})
	worker.MustRegisterAction("send-confirmation", func(ctx *flo.ActionContext) ([]byte, error) {
		log.Printf("Sending confirmation")
		return []byte("ok"), nil
	})
	worker.MustRegisterAction("send-rejection", func(ctx *flo.ActionContext) ([]byte, error) {
		log.Printf("Sending rejection")
		return []byte("ok"), nil
	})

	// 4. Start the worker with graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down gracefully...")
		worker.Stop()
		cancel()
	}()

	log.Println("Worker started — processing workflow actions")
	if err := worker.Start(ctx); err != nil && err != context.Canceled {
		log.Fatalf("Worker error: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
