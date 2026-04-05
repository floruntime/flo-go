// Stream processing example for the Flo Go SDK.
//
// This example walks through the core processing lifecycle:
//
//  1. Submit a processing pipeline (from YAML file)
//  2. Write records to the source stream
//  3. Read transformed records from the sink stream
//  4. Inspect job status and list running jobs
//  5. Create a savepoint and stop the job
//  6. Restore from savepoint and rescale
//  7. Declarative sync (SyncBytes + SyncDir)
//  8. Cancel the job
//
// Usage:
//
//	cd sdks/go/examples/processing && go run ./cmd/
//
// Prerequisites:
//   - A running Flo server on localhost:4453 (or set FLO_ENDPOINT)
//
// For comprehensive automated tests, see processing_test.go.
package main

import (
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
	// 1. Submit a passthrough pipeline
	// -------------------------------------------------------------------------
	fmt.Println("\n--- Submit Pipeline ---")

	yaml, err := os.ReadFile("pipelines/passthrough.yaml")
	if err != nil {
		log.Fatalf("Failed to read pipeline YAML: %v", err)
	}

	jobID, err := client.Processing.Submit(yaml, nil)
	if err != nil {
		log.Fatalf("Submit failed: %v", err)
	}
	fmt.Printf("  Submitted passthrough pipeline: jobID=%s\n", jobID)

	// -------------------------------------------------------------------------
	// 2. Write records to the source stream
	// -------------------------------------------------------------------------
	fmt.Println("\n--- Write Source Records ---")

	for i := 0; i < 5; i++ {
		payload, _ := json.Marshal(map[string]interface{}{
			"event": "click",
			"page":  fmt.Sprintf("/page/%d", i),
			"seq":   i,
		})
		if _, err := client.Stream.Append("raw-events", payload, nil); err != nil {
			log.Fatalf("Append failed: %v", err)
		}
	}
	fmt.Println("  Appended 5 records to raw-events")

	// Give the pipeline time to process
	time.Sleep(2 * time.Second)

	// -------------------------------------------------------------------------
	// 3. Read from the sink stream
	// -------------------------------------------------------------------------
	fmt.Println("\n--- Read Sink Records ---")

	count := uint32(20)
	result, err := client.Stream.Read("passthrough-out", &flo.StreamReadOptions{Count: &count})
	if err != nil {
		log.Fatalf("Read failed: %v", err)
	}
	fmt.Printf("  Read %d records from passthrough-out\n", len(result.Records))
	for i, rec := range result.Records {
		fmt.Printf("    [%d] %s\n", i, string(rec.Payload))
	}

	// -------------------------------------------------------------------------
	// 4. List running jobs
	// -------------------------------------------------------------------------
	fmt.Println("\n--- List Jobs ---")

	entries, err := client.Processing.List(nil)
	if err != nil {
		log.Fatalf("List failed: %v", err)
	}
	fmt.Printf("  %d jobs:\n", len(entries))
	for _, e := range entries {
		fmt.Printf("    %s (id=%s) status=%s\n", e.Name, e.JobID, e.Status)
	}

	// -------------------------------------------------------------------------
	// 5. Create a savepoint and stop the job
	// -------------------------------------------------------------------------
	fmt.Println("\n--- Savepoint + Stop ---")

	sp, err := client.Processing.Savepoint(jobID, nil)
	if err != nil {
		fmt.Printf("  Savepoint failed (non-fatal): %v\n", err)
	} else {
		fmt.Printf("  Created savepoint: %s\n", sp)
	}

	if err := client.Processing.Stop(jobID, nil); err != nil {
		fmt.Printf("  Stop failed (non-fatal): %v\n", err)
	} else {
		fmt.Println("  Stopped job")
	}

	time.Sleep(500 * time.Millisecond)

	// -------------------------------------------------------------------------
	// 6. Restore from savepoint and rescale
	// -------------------------------------------------------------------------
	fmt.Println("\n--- Restore + Rescale ---")

	if sp != "" {
		if err := client.Processing.Restore(jobID, sp, nil); err != nil {
			fmt.Printf("  Restore failed (non-fatal): %v\n", err)
		} else {
			fmt.Printf("  Restored from savepoint %s\n", sp)
		}
	}

	if err := client.Processing.Rescale(jobID, 2, nil); err != nil {
		fmt.Printf("  Rescale failed (non-fatal): %v\n", err)
	} else {
		fmt.Println("  Rescaled to parallelism=2")
	}

	// -------------------------------------------------------------------------
	// 7. Declarative sync
	// -------------------------------------------------------------------------
	fmt.Println("\n--- Declarative Sync ---")

	// SyncBytes — submit a single pipeline from raw YAML
	syncResult, err := client.Processing.SyncBytes(yaml, nil)
	if err != nil {
		log.Fatalf("SyncBytes failed: %v", err)
	}
	fmt.Printf("  SyncBytes: %s -> jobID=%s\n", syncResult.Name, syncResult.JobID)

	// SyncDir — submit all YAML files in a directory
	dirResults, err := client.Processing.SyncDir("pipelines", nil)
	if err != nil {
		log.Fatalf("SyncDir failed: %v", err)
	}
	fmt.Printf("  SyncDir synced %d pipelines:\n", len(dirResults))
	for _, r := range dirResults {
		fmt.Printf("    %s -> jobID=%s\n", r.Name, r.JobID)
	}

	// Clean up synced jobs
	client.Processing.Cancel(syncResult.JobID, nil) //nolint:errcheck
	for _, r := range dirResults {
		client.Processing.Cancel(r.JobID, nil) //nolint:errcheck
	}

	// -------------------------------------------------------------------------
	// 8. Cancel the original job
	// -------------------------------------------------------------------------
	fmt.Println("\n--- Cancel ---")

	if err := client.Processing.Cancel(jobID, nil); err != nil {
		fmt.Printf("  Cancel failed: %v\n", err)
	} else {
		fmt.Println("  Cancelled job")
	}

	fmt.Println("\nDone.")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
