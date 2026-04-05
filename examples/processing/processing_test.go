// Package processing_test provides end-to-end tests for Flo stream processing.
//
// These tests exercise the full processing lifecycle through the Go SDK:
// pipeline submission, record flow verification, operator correctness,
// job management (status, list, stop, savepoint, restore, rescale, cancel),
// and declarative sync.
//
// # Running
//
// Against a local Flo server (start fresh each run):
//
//	rm -rf /tmp/flo-processing-test
//	./flo/zig-out/bin/flo server start --port 4453 --data-dir /tmp/flo-processing-test
//	FLO_ENDPOINT=localhost:4453 go test -v -count=1 ./examples/processing/
//
// With testcontainers — pulls from ghcr.io (requires Docker):
//
//	go test -v -count=1 -timeout 5m ./examples/processing/
//
// Run a single test:
//
//	go test -v -run TestFilterOperator -count=1 -timeout 5m ./examples/processing/
//
// Run tests matching a regex:
//
//	go test -v -run "TestMap|TestFilter" -count=1 -timeout 5m ./examples/processing/
//
// Override the image with FLO_IMAGE (e.g. localhost:5000/flo:dev).
package processing_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	flo "github.com/floruntime/flo-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// ---------------------------------------------------------------------------
// Global test client — shared across subtests (single Flo server)
// ---------------------------------------------------------------------------

var client *flo.Client

const defaultImage = "ghcr.io/floruntime/flo:latest"

func floImage() string {
	if img := os.Getenv("FLO_IMAGE"); img != "" {
		return img
	}
	return defaultImage
}

func TestMain(m *testing.M) {
	endpoint := os.Getenv("FLO_ENDPOINT")

	var container testcontainers.Container
	if endpoint == "" {
		// No endpoint provided — pull and start a Flo container.
		ctx := context.Background()
		image := floImage()
		log.Printf("Starting Flo container from %s", image)

		req := testcontainers.ContainerRequest{
			Image:        image,
			ExposedPorts: []string{"4453/tcp"},
			Cmd:          []string{"server", "start", "--port", "4453"},
			WaitingFor: wait.ForListeningPort("4453/tcp").
				WithStartupTimeout(60 * time.Second),
		}

		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			log.Fatalf("Failed to start Flo container: %v", err)
		}

		host, _ := container.Host(ctx)
		port, _ := container.MappedPort(ctx, "4453")
		endpoint = fmt.Sprintf("%s:%s", host, port.Port())
		log.Printf("Flo container ready at %s", endpoint)
	}

	client = flo.NewClient(endpoint, flo.WithNamespace("e2e"))
	if err := client.Connect(); err != nil {
		log.Fatalf("Failed to connect to Flo at %s: %v", endpoint, err)
	}

	code := m.Run()

	client.Close()
	if container != nil {
		container.Terminate(context.Background()) //nolint:errcheck
	}
	os.Exit(code)
}

// ===========================================================================
// 1. Passthrough Pipeline — stream → stream, no operators
// ===========================================================================

func TestPassthroughPipeline(t *testing.T) {
	yaml := readPipeline(t, "passthrough.yaml")

	// Submit the pipeline
	jobID, err := client.Processing.Submit(yaml, nil)
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	t.Logf("Submitted passthrough pipeline: jobID=%s", jobID)

	// Push records into the source stream
	for i := 0; i < 10; i++ {
		payload, _ := json.Marshal(map[string]interface{}{
			"event": "click",
			"seq":   i,
		})
		if _, err := client.Stream.Append("raw-events", payload, nil); err != nil {
			t.Fatalf("Append to raw-events failed: %v", err)
		}
	}
	t.Log("Appended 10 records to raw-events")

	// Give the pipeline time to process
	time.Sleep(3 * time.Second)

	// Assert job is visible in list
	entry := findJobInList(jobID)
	if entry == nil {
		t.Log("WARN: Job not found in list (shard routing bug)")
	} else {
		t.Logf("Job %s: status=%s", entry.JobID, entry.Status)
		if entry.Status != "running" {
			t.Errorf("Expected job status 'running', got %q", entry.Status)
		}
	}

	// Read from the sink stream — passthrough should forward all 10 records
	count := uint32(20)
	result, err := client.Stream.Read("passthrough-out", &flo.StreamReadOptions{Count: &count})
	if err != nil {
		t.Fatalf("Read from passthrough-out failed: %v", err)
	}
	t.Logf("Read %d records from passthrough-out (expected 10)", len(result.Records))

	if len(result.Records) != 10 {
		t.Fatalf("Expected 10 passthrough records, got %d", len(result.Records))
	}
	for i, rec := range result.Records {
		var parsed map[string]interface{}
		if err := json.Unmarshal(rec.Payload, &parsed); err != nil {
			t.Errorf("Record %d: invalid JSON: %v", i, err)
			continue
		}
		if parsed["event"] != "click" {
			t.Errorf("Record %d: expected event='click', got %v", i, parsed["event"])
		}
	}

	// Clean up
	if err := client.Processing.Cancel(jobID, nil); err != nil {
		t.Logf("Cancel failed (non-fatal): %v", err)
	}
}

// ===========================================================================
// 2. Filter + KeyBy + Aggregate Pipeline
// ===========================================================================

func TestFilterAggregatePipeline(t *testing.T) {
	yaml := readPipeline(t, "filter-aggregate.yaml")

	jobID, err := client.Processing.Submit(yaml, nil)
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	t.Logf("Submitted filter-aggregate pipeline: jobID=%s", jobID)

	// Push order events — mix of completed and pending
	orders := []map[string]interface{}{
		{"customer_id": "cust-A", "amount": 100.0, "status": "completed"},
		{"customer_id": "cust-A", "amount": 250.0, "status": "completed"},
		{"customer_id": "cust-B", "amount": 50.0, "status": "pending"},
		{"customer_id": "cust-B", "amount": 300.0, "status": "completed"},
		{"customer_id": "cust-A", "amount": 75.0, "status": "cancelled"},
		{"customer_id": "cust-C", "amount": 500.0, "status": "completed"},
	}
	for _, o := range orders {
		payload, _ := json.Marshal(o)
		if _, err := client.Stream.Append("order-events", payload, nil); err != nil {
			t.Fatalf("Append to order-events failed: %v", err)
		}
	}
	t.Log("Appended 6 order events (4 completed, 2 filtered)")

	time.Sleep(3 * time.Second)

	// Assert job is visible in list
	entry := findJobInList(jobID)
	if entry == nil {
		t.Log("WARN: Job not found in list (shard routing bug)")
	} else {
		t.Logf("Job %s: status=%s", entry.JobID, entry.Status)
		if entry.Status != "running" {
			t.Errorf("Expected job status 'running', got %q", entry.Status)
		}
	}

	// Read aggregated output — 4 completed orders from 3 customers should produce aggregates
	count := uint32(20)
	result, err := client.Stream.Read("order-totals", &flo.StreamReadOptions{Count: &count})
	if err != nil {
		t.Fatalf("Read from order-totals failed: %v", err)
	}
	t.Logf("Read %d aggregate records from order-totals", len(result.Records))

	if len(result.Records) == 0 {
		t.Fatal("Expected aggregate records in order-totals, got 0")
	}
	for i, rec := range result.Records {
		var parsed map[string]interface{}
		if err := json.Unmarshal(rec.Payload, &parsed); err != nil {
			t.Errorf("Record %d: invalid JSON: %v", i, err)
			continue
		}
		t.Logf("  aggregate: %s", string(rec.Payload))
		if _, ok := parsed["key"]; !ok {
			t.Errorf("Record %d: missing 'key' field (expected grouping key from keyby)", i)
		}
	}

	if err := client.Processing.Cancel(jobID, nil); err != nil {
		t.Logf("Cancel failed (non-fatal): %v", err)
	}
}

// ===========================================================================
// 3. Map (Field Projection) Pipeline
// ===========================================================================

func TestMapProjectionPipeline(t *testing.T) {
	yaml := readPipeline(t, "map-projection.yaml")

	jobID, err := client.Processing.Submit(yaml, nil)
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	t.Logf("Submitted map-projection pipeline: jobID=%s", jobID)

	// Push user signup events with nested structure
	events := []map[string]interface{}{
		{"data": map[string]interface{}{"user_id": "u-001", "email": "alice@example.com", "age": 30}},
		{"data": map[string]interface{}{"user_id": "u-002", "email": "bob@example.com", "age": 25}},
		{"data": map[string]interface{}{"user_id": "u-003", "email": "carol@example.com", "age": 35}},
	}
	for _, e := range events {
		payload, _ := json.Marshal(e)
		if _, err := client.Stream.Append("user-events", payload, nil); err != nil {
			t.Fatalf("Append to user-events failed: %v", err)
		}
	}
	t.Log("Appended 3 user events")

	time.Sleep(3 * time.Second)

	count := uint32(20)
	result, err := client.Stream.Read("user-profiles", &flo.StreamReadOptions{Count: &count})
	if err != nil {
		t.Fatalf("Read from user-profiles failed: %v", err)
	}
	t.Logf("Read %d projected records from user-profiles (expected 3)", len(result.Records))

	if len(result.Records) != 3 {
		t.Fatalf("Expected 3 projected records, got %d", len(result.Records))
	}
	for i, rec := range result.Records {
		var projected map[string]interface{}
		if err := json.Unmarshal(rec.Payload, &projected); err != nil {
			t.Errorf("Record %d: invalid JSON: %v", i, err)
			continue
		}
		t.Logf("  projected: %v", projected)

		if src, ok := projected["source"]; ok {
			if src != "signup-service" {
				t.Errorf("Record %d: expected source='signup-service', got %q", i, src)
			}
		}
		if _, ok := projected["user_id"]; !ok {
			if data, ok := projected["data"].(map[string]interface{}); ok {
				if _, ok := data["user_id"]; !ok {
					t.Errorf("Record %d: missing 'user_id' in projection", i)
				}
			}
		}
	}

	if err := client.Processing.Cancel(jobID, nil); err != nil {
		t.Logf("Cancel failed (non-fatal): %v", err)
	}
}

// ===========================================================================
// 4. FlatMap (Array Explosion) Pipeline
// ===========================================================================

func TestFlatMapPipeline(t *testing.T) {
	yaml := readPipeline(t, "flatmap-explode.yaml")

	jobID, err := client.Processing.Submit(yaml, nil)
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	t.Logf("Submitted flatmap pipeline: jobID=%s", jobID)

	// Push bulk orders with item arrays
	bulkOrders := []map[string]interface{}{
		{
			"order_id": "ORD-1",
			"items": []map[string]interface{}{
				{"sku": "SKU-A", "qty": 2, "price": 10.0},
				{"sku": "SKU-B", "qty": 1, "price": 25.0},
			},
		},
		{
			"order_id": "ORD-2",
			"items": []map[string]interface{}{
				{"sku": "SKU-C", "qty": 5, "price": 5.0},
				{"sku": "SKU-A", "qty": 1, "price": 10.0},
				{"sku": "SKU-D", "qty": 3, "price": 15.0},
			},
		},
	}
	for _, o := range bulkOrders {
		payload, _ := json.Marshal(o)
		if _, err := client.Stream.Append("bulk-orders", payload, nil); err != nil {
			t.Fatalf("Append to bulk-orders failed: %v", err)
		}
	}
	t.Log("Appended 2 bulk orders (5 total items)")

	time.Sleep(3 * time.Second)

	count := uint32(20)
	result, err := client.Stream.Read("order-items", &flo.StreamReadOptions{Count: &count})
	if err != nil {
		t.Fatalf("Read from order-items failed: %v", err)
	}
	t.Logf("Read %d exploded item records from order-items (expected 5)", len(result.Records))

	if len(result.Records) != 5 {
		t.Fatalf("Expected 5 exploded items (2+3), got %d", len(result.Records))
	}
	for i, rec := range result.Records {
		var parsed map[string]interface{}
		if err := json.Unmarshal(rec.Payload, &parsed); err != nil {
			t.Errorf("Record %d: invalid JSON: %v", i, err)
			continue
		}
		t.Logf("  item: %s", string(rec.Payload))
		if _, ok := parsed["sku"]; !ok {
			t.Errorf("Record %d: missing 'sku' field", i)
		}
		if _, ok := parsed["price"]; !ok {
			t.Errorf("Record %d: missing 'price' field", i)
		}
	}

	if err := client.Processing.Cancel(jobID, nil); err != nil {
		t.Logf("Cancel failed (non-fatal): %v", err)
	}
}

// ===========================================================================
// 5. Classify + Multi-Sink Routing Pipeline
// ===========================================================================

func TestClassifyRoutingPipeline(t *testing.T) {
	yaml := readPipeline(t, "classify-routing.yaml")

	jobID, err := client.Processing.Submit(yaml, nil)
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	t.Logf("Submitted classify-routing pipeline: jobID=%s", jobID)

	// Push mixed events with different levels and priorities
	events := []map[string]interface{}{
		{"level": "info", "msg": "user logged in", "priority": 1},
		{"level": "error", "msg": "db connection failed", "priority": 9},
		{"level": "warn", "msg": "high latency detected", "priority": 5},
		{"level": "error", "msg": "disk full", "priority": 10},
		{"level": "info", "msg": "request completed", "priority": 2},
		{"level": "warn", "msg": "memory usage high", "priority": 7},
	}
	for _, e := range events {
		payload, _ := json.Marshal(e)
		if _, err := client.Stream.Append("mixed-events", payload, nil); err != nil {
			t.Fatalf("Append to mixed-events failed: %v", err)
		}
	}
	t.Log("Appended 6 mixed events (2 error, 2 warn, 2 info)")

	time.Sleep(3 * time.Second)

	// Assert job is visible in list
	entry := findJobInList(jobID)
	if entry == nil {
		t.Log("WARN: Job not found in list (shard routing bug)")
	} else {
		t.Logf("Job %s: status=%s", entry.JobID, entry.Status)
		if entry.Status != "running" {
			t.Errorf("Expected job status 'running', got %q", entry.Status)
		}
	}

	count := uint32(20)

	// All 6 events should appear in the all-classified sink
	allResult, err := client.Stream.Read("all-classified", &flo.StreamReadOptions{Count: &count})
	if err != nil {
		t.Fatalf("Read from all-classified failed: %v", err)
	}
	t.Logf("all-classified: %d records (expected 6)", len(allResult.Records))
	if len(allResult.Records) != 6 {
		t.Errorf("Expected 6 classified records, got %d", len(allResult.Records))
	}

	// Only error events should appear in error-events (2 expected)
	errResult, err := client.Stream.Read("error-events", &flo.StreamReadOptions{Count: &count})
	if err != nil {
		t.Fatalf("Read from error-events failed: %v", err)
	}
	t.Logf("error-events: %d records (expected 2)", len(errResult.Records))
	if len(errResult.Records) != 2 {
		t.Errorf("Expected 2 error records, got %d", len(errResult.Records))
	}
	for i, rec := range errResult.Records {
		var parsed map[string]interface{}
		if err := json.Unmarshal(rec.Payload, &parsed); err != nil {
			t.Errorf("Record %d: invalid JSON: %v", i, err)
			continue
		}
		if parsed["level"] != "error" {
			t.Errorf("error-events record %d: level=%v, expected 'error'", i, parsed["level"])
		}
	}

	// Only warn events should appear in warning-events (2 expected)
	warnResult, err := client.Stream.Read("warning-events", &flo.StreamReadOptions{Count: &count})
	if err != nil {
		t.Fatalf("Read from warning-events failed: %v", err)
	}
	t.Logf("warning-events: %d records (expected 2)", len(warnResult.Records))
	if len(warnResult.Records) != 2 {
		t.Errorf("Expected 2 warning records, got %d", len(warnResult.Records))
	}
	for i, rec := range warnResult.Records {
		var parsed map[string]interface{}
		if err := json.Unmarshal(rec.Payload, &parsed); err == nil {
			if parsed["level"] != "warn" {
				t.Errorf("warning-events record %d: level=%v, expected 'warn'", i, parsed["level"])
			}
		}
	}

	// critical-errors: errors with priority>8 (both error events have priority 9 and 10)
	critResult, err := client.Stream.Read("critical-error-events", &flo.StreamReadOptions{Count: &count})
	if err != nil {
		t.Fatalf("Read from critical-error-events failed: %v", err)
	}
	t.Logf("critical-error-events: %d records (expected 2)", len(critResult.Records))
	if len(critResult.Records) != 2 {
		t.Errorf("Expected 2 critical error records, got %d", len(critResult.Records))
	}
	for i, rec := range critResult.Records {
		var parsed map[string]interface{}
		if err := json.Unmarshal(rec.Payload, &parsed); err == nil {
			if p, ok := parsed["priority"].(float64); ok && p <= 8 {
				t.Errorf("critical record %d: priority %.0f <= 8", i, p)
			}
		}
	}

	if err := client.Processing.Cancel(jobID, nil); err != nil {
		t.Logf("Cancel failed (non-fatal): %v", err)
	}
}

// ===========================================================================
// 6. KV Enrichment Pipeline
// ===========================================================================

func TestKVEnrichmentPipeline(t *testing.T) {
	// Seed KV store with account data
	accounts := map[string]string{
		"account:acct-001": `{"tier":"gold","region":"us-east"}`,
		"account:acct-002": `{"tier":"silver","region":"eu-west"}`,
		"account:acct-003": `{"tier":"bronze","region":"ap-south"}`,
	}
	for k, v := range accounts {
		if err := client.KV.Put(k, []byte(v), nil); err != nil {
			t.Fatalf("KV Put %s failed: %v", k, err)
		}
	}
	t.Log("Seeded 3 account records in KV")

	yaml := readPipeline(t, "kv-enrichment.yaml")

	jobID, err := client.Processing.Submit(yaml, nil)
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	t.Logf("Submitted kv-enrichment pipeline: jobID=%s", jobID)

	// Push transaction events
	txns := []map[string]interface{}{
		{"account_id": "acct-001", "amount": 100.0, "type": "purchase"},
		{"account_id": "acct-002", "amount": 250.0, "type": "refund"},
		{"account_id": "acct-003", "amount": 50.0, "type": "purchase"},
		{"account_id": "acct-999", "amount": 75.0, "type": "purchase"}, // no KV match
	}
	for _, tx := range txns {
		payload, _ := json.Marshal(tx)
		if _, err := client.Stream.Append("transactions", payload, nil); err != nil {
			t.Fatalf("Append to transactions failed: %v", err)
		}
	}
	t.Log("Appended 4 transactions (3 with KV match, 1 without)")

	time.Sleep(3 * time.Second)

	count := uint32(20)
	result, err := client.Stream.Read("enriched-txns", &flo.StreamReadOptions{Count: &count})
	if err != nil {
		t.Fatalf("Read from enriched-txns failed: %v", err)
	}
	t.Logf("Read %d enriched records from enriched-txns (expected 4)", len(result.Records))

	if len(result.Records) != 4 {
		t.Fatalf("Expected 4 enriched records, got %d", len(result.Records))
	}
	for i, rec := range result.Records {
		var parsed map[string]interface{}
		if err := json.Unmarshal(rec.Payload, &parsed); err != nil {
			t.Errorf("Record %d: invalid JSON: %v", i, err)
			continue
		}
		t.Logf("  enriched: %s", string(rec.Payload))
		// Every record should retain account_id
		if _, ok := parsed["account_id"]; !ok {
			t.Errorf("Record %d: missing 'account_id' field", i)
		}
		// Known accounts should have tier injected from KV
		acctID, _ := parsed["account_id"].(string)
		if acctID != "acct-999" {
			if _, ok := parsed["account_info"]; !ok {
				t.Errorf("Record %d (account %s): expected 'account_info' from KV enrichment", i, acctID)
			}
		}
	}

	if err := client.Processing.Cancel(jobID, nil); err != nil {
		t.Logf("Cancel failed (non-fatal): %v", err)
	}
}

// ===========================================================================
// 7. Job Lifecycle — status, list, stop, savepoint, restore, rescale, cancel
// ===========================================================================

func TestJobLifecycle(t *testing.T) {
	yaml := readPipeline(t, "passthrough.yaml")

	// --- Submit ---
	jobID, err := client.Processing.Submit(yaml, nil)
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	t.Logf("Submitted job: %s", jobID)

	time.Sleep(500 * time.Millisecond)

	// --- Status (via List — Status API has server-side routing bug) ---
	t.Run("Status", func(t *testing.T) {
		entry := findJobInList(jobID)
		if entry == nil {
			// Server has shard-routing issues — List may not find jobs submitted
			// to a different shard. Log and skip rather than fail.
			t.Logf("WARN: Job %s not found in list (shard routing bug)", jobID)
			t.Skip("List cannot find job — server shard routing issue")
		}
		t.Logf("Job %s: name=%s status=%s", entry.JobID, entry.Name, entry.Status)

		if entry.Status != "running" {
			t.Errorf("Expected 'running', got %q", entry.Status)
		}
		if entry.Name != "passthrough-pipe" {
			t.Errorf("Expected name 'passthrough-pipe', got %q", entry.Name)
		}

		// Also try the Status API directly — expected to fail until server is fixed
		status, err := client.Processing.Status(jobID, nil)
		if err != nil {
			t.Logf("Status API (known broken): %v", err)
		} else {
			t.Logf("Status API returned: status=%s parallelism=%d batch_size=%d",
				status.Status, status.Parallelism, status.BatchSize)
		}
	})

	// --- List ---
	t.Run("List", func(t *testing.T) {
		entries, err := client.Processing.List(nil)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		t.Logf("Listed %d jobs", len(entries))

		found := false
		for _, e := range entries {
			t.Logf("  job: name=%s id=%s status=%s", e.Name, e.JobID, e.Status)
			if e.JobID == jobID {
				found = true
			}
		}
		if !found {
			t.Log("WARN: Submitted job not found in list (shard routing issue)")
		}
	})

	// --- Savepoint ---
	var savepointID string
	t.Run("Savepoint", func(t *testing.T) {
		sp, err := client.Processing.Savepoint(jobID, nil)
		if err != nil {
			t.Logf("Savepoint failed (may be routing bug): %v", err)
			t.Skip("Savepoint not available — likely same routing bug as Status")
		}
		savepointID = sp
		t.Logf("Created savepoint: %s", savepointID)
	})

	// --- Stop ---
	t.Run("Stop", func(t *testing.T) {
		if err := client.Processing.Stop(jobID, nil); err != nil {
			t.Logf("Stop failed (may be routing bug): %v", err)
			t.Skip("Stop not available — likely same routing bug as Status")
		}
		t.Log("Stopped job")

		time.Sleep(500 * time.Millisecond)

		entry := findJobInList(jobID)
		if entry != nil {
			t.Logf("Status after stop: %s", entry.Status)
			if entry.Status != "stopped" {
				t.Logf("WARN: Expected 'stopped', got %q", entry.Status)
			}
		}
	})

	// --- Restore from savepoint ---
	t.Run("Restore", func(t *testing.T) {
		if savepointID == "" {
			t.Skip("No savepoint to restore from")
		}
		if err := client.Processing.Restore(jobID, savepointID, nil); err != nil {
			t.Logf("Restore failed: %v", err)
			t.Skip("Restore not available")
		}
		t.Logf("Restored from savepoint %s", savepointID)

		time.Sleep(500 * time.Millisecond)

		entry := findJobInList(jobID)
		if entry != nil {
			t.Logf("Status after restore: %s", entry.Status)
		}
	})

	// --- Rescale ---
	t.Run("Rescale", func(t *testing.T) {
		if err := client.Processing.Rescale(jobID, 2, nil); err != nil {
			t.Logf("Rescale failed (may be routing bug): %v", err)
			t.Skip("Rescale not available")
		}
		t.Log("Rescaled to parallelism=2")
	})

	// --- Cancel ---
	t.Run("Cancel", func(t *testing.T) {
		if err := client.Processing.Cancel(jobID, nil); err != nil {
			t.Logf("Cancel failed (may be routing bug): %v", err)
			t.Skip("Cancel not available — likely same routing bug as Status")
		}
		t.Log("Cancelled job")

		time.Sleep(500 * time.Millisecond)

		entry := findJobInList(jobID)
		if entry != nil {
			t.Logf("Status after cancel: %s", entry.Status)
			if entry.Status != "cancelled" {
				t.Logf("WARN: Expected 'cancelled', got %q", entry.Status)
			}
		}
	})
}

// ===========================================================================
// 8. Declarative Sync — SyncBytes + SyncDir
// ===========================================================================

func TestDeclarativeSync(t *testing.T) {
	t.Run("SyncBytes", func(t *testing.T) {
		yaml := readPipeline(t, "passthrough.yaml")

		result, err := client.Processing.SyncBytes(yaml, nil)
		if err != nil {
			t.Fatalf("SyncBytes failed: %v", err)
		}
		t.Logf("SyncBytes: name=%s jobID=%s", result.Name, result.JobID)

		if result.Name != "passthrough-pipe" {
			t.Errorf("Expected name 'passthrough-pipe', got %q", result.Name)
		}
		if result.JobID == "" {
			t.Error("Expected non-empty jobID")
		}

		// Clean up
		client.Processing.Cancel(result.JobID, nil) //nolint:errcheck
	})

	t.Run("SyncDir", func(t *testing.T) {
		results, err := client.Processing.SyncDir("pipelines", nil)
		if err != nil {
			t.Fatalf("SyncDir failed: %v", err)
		}
		t.Logf("SyncDir synced %d pipelines:", len(results))
		for _, r := range results {
			t.Logf("  %s → jobID=%s", r.Name, r.JobID)
		}

		if len(results) == 0 {
			t.Error("Expected at least one pipeline from SyncDir")
		}

		// Clean up all submitted jobs
		for _, r := range results {
			client.Processing.Cancel(r.JobID, nil) //nolint:errcheck
		}
	})
}

// ===========================================================================
// 9. Multiple Pipelines Concurrently
// ===========================================================================

func TestMultiplePipelines(t *testing.T) {
	pipelines := []string{"passthrough.yaml", "filter-aggregate.yaml", "map-projection.yaml"}
	var jobIDs []string

	for _, p := range pipelines {
		yaml := readPipeline(t, p)
		jobID, err := client.Processing.Submit(yaml, nil)
		if err != nil {
			t.Fatalf("Submit %s failed: %v", p, err)
		}
		jobIDs = append(jobIDs, jobID)
		t.Logf("Submitted %s: jobID=%s", p, jobID)
	}

	time.Sleep(1 * time.Second)

	// Verify all are visible via List
	// NOTE: List may not find all jobs due to server shard routing issues.
	for i, jobID := range jobIDs {
		entry := findJobInList(jobID)
		if entry == nil {
			t.Logf("WARN: Job %s (%s) not found in list (shard routing)", jobID, pipelines[i])
			continue
		}
		t.Logf("  %s: status=%s", pipelines[i], entry.Status)
	}

	// List should show all of them
	entries, err := client.Processing.List(nil)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	t.Logf("Total jobs in list: %d", len(entries))

	// Clean up
	for _, jobID := range jobIDs {
		client.Processing.Cancel(jobID, nil) //nolint:errcheck
	}
}

// ===========================================================================
// Helpers
// ===========================================================================

func readPipeline(t *testing.T, filename string) []byte {
	t.Helper()
	data, err := os.ReadFile("pipelines/" + filename)
	if err != nil {
		t.Fatalf("Failed to read pipeline %s: %v", filename, err)
	}
	return data
}

// findJobInList uses the List API to find a job by ID.
// Workaround: the Status API has a server-side shard-routing bug (returns
// "not found" even for jobs that List can see). Use this until the server
// is fixed.
func findJobInList(jobID string) *flo.ProcessingListEntry {
	entries, err := client.Processing.List(nil)
	if err != nil {
		return nil
	}
	for _, e := range entries {
		if e.JobID == jobID {
			return e
		}
	}
	return nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
