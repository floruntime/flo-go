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
	ttl := uint64(3600) // 1 hour
	if _, err := client.KV.Put("session:abc", []byte("session-data"), &flo.PutOptions{
		TTLSeconds: &ttl,
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
	fmt.Printf("Scan found %d entries\n", len(result.Entries))
	for _, entry := range result.Entries {
		fmt.Printf("  %s = %s\n", entry.Key, entry.Value)
	}

	// Paginated scan
	limit := uint32(100)
	result, err = client.KV.Scan("user:", &flo.ScanOptions{Limit: &limit})
	if err != nil {
		log.Fatalf("Paginated scan failed: %v", err)
	}
	if result.HasMore {
		// Continue scanning with cursor
		result, err = client.KV.Scan("user:", &flo.ScanOptions{Cursor: result.Cursor})
		if err != nil {
			log.Fatalf("Cursor scan failed: %v", err)
		}
	}

	// Delete a key
	if err := client.KV.Delete("user:123", nil); err != nil {
		log.Fatalf("Delete failed: %v", err)
	}
	fmt.Println("Deleted user:123")

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

	// Enqueue with priority
	seq, err = client.Queue.Enqueue("tasks", []byte(`{"task": "urgent"}`), &flo.EnqueueOptions{
		Priority: 10,
	})
	if err != nil {
		log.Fatalf("Enqueue with priority failed: %v", err)
	}
	fmt.Printf("Enqueued priority message with seq=%d\n", seq)

	// Enqueue with delay
	delayMS := uint64(60000) // 1 minute
	seq, err = client.Queue.Enqueue("tasks", []byte(`{"task": "delayed"}`), &flo.EnqueueOptions{
		DelayMS: &delayMS,
	})
	if err != nil {
		log.Fatalf("Enqueue with delay failed: %v", err)
	}
	fmt.Printf("Enqueued delayed message with seq=%d\n", seq)

	// Dequeue messages
	dequeueResult, err := client.Queue.Dequeue("tasks", 10, nil)
	if err != nil {
		log.Fatalf("Dequeue failed: %v", err)
	}
	fmt.Printf("Dequeued %d messages\n", len(dequeueResult.Messages))

	for _, msg := range dequeueResult.Messages {
		fmt.Printf("  Processing message seq=%d: %s\n", msg.Seq, msg.Payload)

		// Acknowledge successful processing
		if err := client.Queue.Ack("tasks", []uint64{msg.Seq}, nil); err != nil {
			log.Fatalf("Ack failed: %v", err)
		}
	}

	// Dequeue with long polling (wait for messages)
	blockMS := uint32(5000) // 5 seconds
	dequeueResult, err = client.Queue.Dequeue("tasks", 10, &flo.DequeueOptions{
		BlockMS: &blockMS,
	})
	if err != nil {
		log.Fatalf("Dequeue with blocking failed: %v", err)
	}
	fmt.Printf("Dequeued %d messages (with blocking)\n", len(dequeueResult.Messages))

	// Example: Nack a message (retry later)
	if len(dequeueResult.Messages) > 0 {
		msg := dequeueResult.Messages[0]
		if err := client.Queue.Nack("tasks", []uint64{msg.Seq}, nil); err != nil {
			log.Fatalf("Nack failed: %v", err)
		}
		fmt.Printf("Nacked message seq=%d for retry\n", msg.Seq)
	}

	// Example: Send to DLQ (don't retry)
	if len(dequeueResult.Messages) > 1 {
		msg := dequeueResult.Messages[1]
		if err := client.Queue.Nack("tasks", []uint64{msg.Seq}, &flo.NackOptions{
			ToDLQ: true,
		}); err != nil {
			log.Fatalf("Nack to DLQ failed: %v", err)
		}
		fmt.Printf("Sent message seq=%d to DLQ\n", msg.Seq)
	}

	// List DLQ messages
	dlqResult, err := client.Queue.DLQList("tasks", nil)
	if err != nil {
		log.Fatalf("DLQ list failed: %v", err)
	}
	fmt.Printf("DLQ contains %d messages\n", len(dlqResult.Messages))

	// Requeue DLQ messages
	if len(dlqResult.Messages) > 0 {
		seqs := make([]uint64, len(dlqResult.Messages))
		for i, msg := range dlqResult.Messages {
			seqs[i] = msg.Seq
		}
		if err := client.Queue.DLQRequeue("tasks", seqs, nil); err != nil {
			log.Fatalf("DLQ requeue failed: %v", err)
		}
		fmt.Printf("Requeued %d DLQ messages\n", len(seqs))
	}

	fmt.Println("\n=== Done ===")
}
