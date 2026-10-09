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

	// Dequeue messages for processing
	fmt.Println("\n--- Dequeuing messages ---")
	dequeueResult, err := queue.Dequeue("tasks", 10, nil)
	if err != nil {
		log.Fatalf("Dequeue failed: %v", err)
	}
	fmt.Printf("Dequeued %d messages:\n", len(dequeueResult.Messages))

	var processedSeqs []uint64
	var failedSeqs []uint64

	for i, msg := range dequeueResult.Messages {
		fmt.Printf("  Processing seq=%d: %s\n", msg.Seq, string(msg.Payload))

		// Simulate processing - some succeed, some fail
		if i%3 == 2 {
			// Simulate failure
			failedSeqs = append(failedSeqs, msg.Seq)
		} else {
			// Success
			processedSeqs = append(processedSeqs, msg.Seq)
		}
	}

	// A dequeue consumes its messages; queues are currently at-most-once, so Ack and Nack have no effect on a dequeued message.
	if len(processedSeqs) > 0 {
		if err := queue.Ack("tasks", processedSeqs, nil); err != nil {
			log.Fatalf("Ack failed: %v", err)
		}
		fmt.Printf("Acknowledged %d messages\n", len(processedSeqs))
	}

	// Nack has no effect either: these messages are not retried
	if len(failedSeqs) > 0 {
		if err := queue.Nack("tasks", failedSeqs, nil); err != nil {
			log.Fatalf("Nack failed: %v", err)
		}
		fmt.Printf("Nacked %d messages\n", len(failedSeqs))
	}

	// Dequeue with blocking (long polling)
	fmt.Println("\n--- Blocking dequeue (5 second timeout) ---")
	blockMS := uint32(5000)
	dequeueResult, err = queue.Dequeue("tasks", 5, &flo.DequeueOptions{
		BlockMS: &blockMS,
	})
	if err != nil {
		log.Fatalf("Blocking dequeue failed: %v", err)
	}
	fmt.Printf("Blocking dequeue returned %d messages\n", len(dequeueResult.Messages))

	if len(dequeueResult.Messages) > 0 {
		queue.Ack("tasks", []uint64{dequeueResult.Messages[0].Seq}, nil)
	}

	// Dead Letter Queue (DLQ). Queues are currently at-most-once, so a
	// dequeued message does not reach the DLQ in normal use; it is usually empty.
	fmt.Println("\n--- DLQ operations ---")

	// List DLQ messages
	dlqResult, err := queue.DLQList("tasks", nil)
	if err != nil {
		log.Fatalf("DLQ list failed: %v", err)
	}
	fmt.Printf("DLQ contains %d messages\n", len(dlqResult.Messages))

	// Requeue DLQ messages back to main queue
	if len(dlqResult.Messages) > 0 {
		dlqSeqs := make([]uint64, len(dlqResult.Messages))
		for i, msg := range dlqResult.Messages {
			dlqSeqs[i] = msg.Seq
		}
		if err := queue.DLQRequeue("tasks", dlqSeqs, nil); err != nil {
			log.Fatalf("DLQ requeue failed: %v", err)
		}
		fmt.Printf("Requeued %d messages from DLQ\n", len(dlqSeqs))
	}

	fmt.Println("\n=== Done ===")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
