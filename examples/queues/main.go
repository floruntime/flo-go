// Example: Queues API usage with the Flo Go SDK
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	flo "github.com/floruntime/flo-go"
)

func main() {
	// Create and connect client
	client := flo.NewClient(getEnv("FLO_ENDPOINT", "localhost:3000"),
		flo.WithNamespace("myapp"),
		flo.WithDebug(true),
	)

	if err := client.Connect(); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	queue := client.Queue

	fmt.Println("=== Queue Operations ===")

	// Enqueue messages
	fmt.Println("\n--- Enqueueing messages ---")

	// Simple enqueue
	task1 := map[string]interface{}{
		"task":    "send-email",
		"to":      "user@example.com",
		"subject": "Welcome!",
	}
	payload1, _ := json.Marshal(task1)
	seq, err := queue.Enqueue("tasks", payload1, nil)
	if err != nil {
		log.Fatalf("Enqueue failed: %v", err)
	}
	fmt.Printf("Enqueued task: seq=%d\n", seq)

	// Enqueue with priority (lower is taken first)
	task2 := map[string]interface{}{
		"task": "urgent-alert",
		"msg":  "Server is down!",
	}
	payload2, _ := json.Marshal(task2)
	seq, err = queue.Enqueue("tasks", payload2, &flo.EnqueueOptions{
		Priority: 1, // Taken before higher numbers
	})
	if err != nil {
		log.Fatalf("Priority enqueue failed: %v", err)
	}
	fmt.Printf("Enqueued urgent task with priority 1: seq=%d\n", seq)

	// Peek at messages without consuming
	fmt.Println("\n--- Peeking at queue ---")
	peekResult, err := queue.Peek("tasks", 5, nil)
	if err != nil {
		log.Fatalf("Peek failed: %v", err)
	}
	fmt.Printf("Peeked %d messages (without consuming):\n", len(peekResult.Messages))
	for _, msg := range peekResult.Messages {
		fmt.Printf("  seq=%d payload=%s\n", msg.Seq, string(msg.Payload))
	}

	// Dequeue messages for processing. They're leased to this consumer:
	// complete each within the lease, or it's delivered again.
	fmt.Println("\n--- Dequeuing messages ---")
	dequeueResult, err := queue.Dequeue("tasks", 10, &flo.DequeueOptions{Consumer: "worker-1"})
	if err != nil {
		log.Fatalf("Dequeue failed: %v", err)
	}
	fmt.Printf("Dequeued %d messages:\n", len(dequeueResult.Messages))

	var processed, failed []flo.QueueItem
	for i, msg := range dequeueResult.Messages {
		fmt.Printf("  Processing seq=%d (delivery %d): %s\n", msg.Seq, msg.Deliveries, string(msg.Payload))

		// Simulate processing - some succeed, some fail
		if i%3 == 2 {
			failed = append(failed, msg.Item())
		} else {
			processed = append(processed, msg.Item())
		}
	}

	if len(processed) > 0 {
		report, err := queue.Complete("tasks", dequeueResult.Lease, processed, nil)
		if err != nil {
			log.Fatalf("Complete failed: %v", err)
		}
		fmt.Printf("Completed %d messages\n", report.Applied)
	}

	// Failed messages are ready again after the delay, or dead once their
	// attempts are spent
	if len(failed) > 0 {
		if _, err := queue.Fail("tasks", dequeueResult.Lease, failed, &flo.QueueFailOptions{DelayMS: 1000, Error: "simulated failure"}); err != nil {
			log.Fatalf("Fail failed: %v", err)
		}
		fmt.Printf("Failed %d messages\n", len(failed))
	}

	// Dequeue with blocking (long polling), completing as they're taken
	fmt.Println("\n--- Blocking dequeue with ack (5 second timeout) ---")
	blockMS := uint32(5000)
	dequeueResult, err = queue.Dequeue("tasks", 5, &flo.DequeueOptions{
		BlockMS: &blockMS,
		Ack:     true,
	})
	if err != nil {
		log.Fatalf("Blocking dequeue failed: %v", err)
	}
	fmt.Printf("Blocking dequeue returned %d messages\n", len(dequeueResult.Messages))

	// Dead Letter Queue (DLQ): messages failed or expired until their
	// attempts ran out
	fmt.Println("\n--- DLQ operations ---")
	dlqResult, err := queue.DLQList("tasks", nil)
	if err != nil {
		log.Fatalf("DLQ list failed: %v", err)
	}
	fmt.Printf("DLQ contains %d messages\n", len(dlqResult.Messages))

	fmt.Println("\n=== Done ===")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
