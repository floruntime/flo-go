// Example: Basic usage of the Flo Go SDK
package main

import (
	"fmt"
	"log"

	flo "github.com/floruntime/flo-go"
)

func main() {
	// Create a client with default options
	client := flo.NewClient("localhost:9000",
		flo.WithNamespace("myapp"),
		flo.WithDebug(true),
	)

	// Connect to the server
	if err := client.Connect(); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	// =========================================================================
	// KV Operations
	// =========================================================================

	fmt.Println("\n=== KV Operations ===")

	// Put a value
	putResult, err := client.KV.Put("user:123", []byte("John Doe"), nil)
	if err != nil {
		log.Fatalf("Put failed: %v", err)
	}
	fmt.Printf("Put user:123 (version=%d)\n", putResult.Version)

	// Get the value
	got, err := client.KV.Get("user:123", nil)
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	fmt.Printf("Get user:123 = %s (version=%d)\n", got.Value, got.Version)

	// Put with TTL
	ttl := uint64(3_600_000) // 1 hour, in milliseconds
	if _, err := client.KV.Put("session:abc", []byte("session-data"), &flo.PutOptions{
		TTLMs: &ttl,
	}); err != nil {
		log.Fatalf("Put with TTL failed: %v", err)
	}
	fmt.Println("Put session:abc with TTL")

	// Put with CAS (optimistic locking)
	casVersion := uint64(1)
	_, err = client.KV.Put("counter", []byte("2"), &flo.PutOptions{
		CASVersion: &casVersion,
	})
	if err != nil {
		if flo.IsConflict(err) {
			fmt.Println("CAS conflict - value was modified")
		} else {
			log.Fatalf("Put with CAS failed: %v", err)
		}
	}

	// Scan keys with prefix
	result, err := client.KV.Scan("user:", nil)
	if err != nil {
		log.Fatalf("Scan failed: %v", err)
	}
	fmt.Printf("Scan found %d keys\n", len(result.Keys))
	for _, key := range result.Keys {
		fmt.Printf("  %s\n", key)
	}

	// Paginated scan
	limit := uint32(100)
	result, err = client.KV.Scan("user:", &flo.ScanOptions{Limit: &limit})
	if err != nil {
		log.Fatalf("Paginated scan failed: %v", err)
	}
	for result.Cursor != nil {
		// Continue scanning with cursor
		result, err = client.KV.Scan("user:", &flo.ScanOptions{Limit: &limit, Cursor: result.Cursor})
		if err != nil {
			log.Fatalf("Cursor scan failed: %v", err)
		}
	}

	// Delete a key
	existed, err := client.KV.Delete("user:123", nil)
	if err != nil {
		log.Fatalf("Delete failed: %v", err)
	}
	fmt.Printf("Deleted user:123 (existed=%v)\n", existed)

	// Get returns nil for non-existent key
	missing, err := client.KV.Get("nonexistent", nil)
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	if missing == nil {
		fmt.Println("Key 'nonexistent' not found (as expected)")
	}

	// =========================================================================
	// Queue Operations
	// =========================================================================

	fmt.Println("\n=== Queue Operations ===")

	// Enqueue a message
	seq, err := client.Queue.Enqueue("tasks", []byte(`{"task": "process"}`), nil)
	if err != nil {
		log.Fatalf("Enqueue failed: %v", err)
	}
	fmt.Printf("Enqueued message with seq=%d\n", seq)

	// Enqueue with priority (lower is taken first)
	seq, err = client.Queue.Enqueue("tasks", []byte(`{"task": "urgent"}`), &flo.EnqueueOptions{
		Priority: 10,
	})
	if err != nil {
		log.Fatalf("Enqueue with priority failed: %v", err)
	}
	fmt.Printf("Enqueued priority message with seq=%d\n", seq)

	// Dequeue messages: they're leased to this consumer until completed
	dequeueResult, err := client.Queue.Dequeue("tasks", 10, &flo.DequeueOptions{Consumer: "example"})
	if err != nil {
		log.Fatalf("Dequeue failed: %v", err)
	}
	fmt.Printf("Dequeued %d messages\n", len(dequeueResult.Messages))
	for _, msg := range dequeueResult.Messages {
		fmt.Printf("  Processing message seq=%d: %s\n", msg.Seq, msg.Payload)
	}
	// Complete them within the lease, or they're delivered again
	if len(dequeueResult.Messages) > 0 {
		report, err := client.Queue.Complete("tasks", dequeueResult.Lease, dequeueResult.Items(), nil)
		if err != nil {
			log.Fatalf("Complete failed: %v", err)
		}
		fmt.Printf("Completed %d messages\n", report.Applied)
	}

	// Dequeue with long polling (wait for messages)
	blockMS := uint32(5000) // 5 seconds
	dequeueResult, err = client.Queue.Dequeue("tasks", 10, &flo.DequeueOptions{
		Consumer: "example",
		BlockMS:  &blockMS,
	})
	if err != nil {
		log.Fatalf("Dequeue with blocking failed: %v", err)
	}
	fmt.Printf("Dequeued %d messages (with blocking)\n", len(dequeueResult.Messages))

	// Fail hands a message back: ready again, or dead once its attempts are spent
	if len(dequeueResult.Messages) > 0 {
		msg := dequeueResult.Messages[0]
		if _, err := client.Queue.Fail("tasks", dequeueResult.Lease, []flo.QueueItem{msg.Item()}, &flo.QueueFailOptions{Error: "example failure"}); err != nil {
			log.Fatalf("Fail failed: %v", err)
		}
		fmt.Printf("Failed message seq=%d\n", msg.Seq)
	}

	// List DLQ messages: those failed or expired until their attempts ran out
	dlqResult, err := client.Queue.DLQList("tasks", nil)
	if err != nil {
		log.Fatalf("DLQ list failed: %v", err)
	}
	fmt.Printf("DLQ contains %d messages\n", len(dlqResult.Messages))

	fmt.Println("\n=== Done ===")
}
