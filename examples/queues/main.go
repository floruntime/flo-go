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
		"task":   "send-email",
		"to":     "user@example.com",
		"subject": "Welcome!",
	}
	payload1, _ := json.Marshal(task1)
	seq, err := queue.Enqueue("tasks", payload1, nil)
	if err != nil {
		log.Fatalf("Enqueue failed: %v", err)
	}
	fmt.Printf("Enqueued task: seq=%d\n", seq)

	// Enqueue with priority (higher priority = processed first)
	task2 := map[string]interface{}{
		"task": "urgent-alert",
		"msg":  "Server is down!",
	}
	payload2, _ := json.Marshal(task2)
	seq, err = queue.Enqueue("tasks", payload2, &flo.EnqueueOptions{
		Priority: 100, // High priority
	})
	if err != nil {
		log.Fatalf("Priority enqueue failed: %v", err)
	}
	fmt.Printf("Enqueued urgent task with priority 100: seq=%d\n", seq)

	// Enqueue with delay
	task3 := map[string]interface{}{
		"task": "scheduled-report",
		"type": "daily",
	}
	payload3, _ := json.Marshal(task3)
	delayMS := uint64(60000) // 1 minute delay
	seq, err = queue.Enqueue("tasks", payload3, &flo.EnqueueOptions{
		DelayMS: &delayMS,
	})
	if err != nil {
		log.Fatalf("Delayed enqueue failed: %v", err)
	}
	fmt.Printf("Enqueued delayed task (1 min): seq=%d\n", seq)

	// Enqueue with deduplication key
	task4 := map[string]interface{}{
		"task":    "process-order",
		"orderID": "ORD-12345",
	}
	payload4, _ := json.Marshal(task4)
	seq, err = queue.Enqueue("tasks", payload4, &flo.EnqueueOptions{
		DedupKey: "order-ORD-12345", // Prevents duplicate processing
	})
	if err != nil {
		log.Fatalf("Dedup enqueue failed: %v", err)
	}
	fmt.Printf("Enqueued with dedup key: seq=%d\n", seq)

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

	// Acknowledge successful messages
	if len(processedSeqs) > 0 {
		if err := queue.Ack("tasks", processedSeqs, nil); err != nil {
			log.Fatalf("Ack failed: %v", err)
		}
		fmt.Printf("Acknowledged %d messages\n", len(processedSeqs))
	}

	// Nack failed messages (will be retried)
	if len(failedSeqs) > 0 {
		if err := queue.Nack("tasks", failedSeqs, nil); err != nil {
			log.Fatalf("Nack failed: %v", err)
		}
		fmt.Printf("Nacked %d messages for retry\n", len(failedSeqs))
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

	// Touch to extend lease on long-running tasks
	if len(dequeueResult.Messages) > 0 {
		msg := dequeueResult.Messages[0]
		fmt.Printf("\n--- Extending lease for seq=%d ---\n", msg.Seq)
		if err := queue.Touch("tasks", []uint64{msg.Seq}, nil); err != nil {
			log.Fatalf("Touch failed: %v", err)
		}
		fmt.Println("Lease extended by 30 seconds")

		// Ack after processing
		queue.Ack("tasks", []uint64{msg.Seq}, nil)
	}

	// Send to Dead Letter Queue (DLQ)
	fmt.Println("\n--- DLQ operations ---")

	// Enqueue a message that will "fail"
	badTask, _ := json.Marshal(map[string]string{"task": "will-fail"})
	seq, _ = queue.Enqueue("tasks", badTask, nil)

	// Dequeue and send to DLQ
	result, _ := queue.Dequeue("tasks", 1, nil)
	if len(result.Messages) > 0 {
		// Nack with ToDLQ flag
		if err := queue.Nack("tasks", []uint64{result.Messages[0].Seq}, &flo.NackOptions{
			ToDLQ: true,
		}); err != nil {
			log.Fatalf("Nack to DLQ failed: %v", err)
		}
		fmt.Println("Sent message to DLQ")
	}

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
