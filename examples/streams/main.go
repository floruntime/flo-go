// Example: Streams API usage with the Flo Go SDK
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
	// Create and connect client
	client := flo.NewClient(getEnv("FLO_ENDPOINT", "localhost:3000"),
		flo.WithNamespace("myapp"),
		flo.WithDebug(true),
	)

	if err := client.Connect(); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	// Get stream client
	stream := client.Stream

	fmt.Println("=== Stream Operations ===")

	// Append records to a stream
	fmt.Println("\n--- Appending records ---")
	for i := 0; i < 5; i++ {
		event := map[string]interface{}{
			"event_type": "user.action",
			"user_id":    fmt.Sprintf("user-%d", i),
			"action":     "login",
			"timestamp":  time.Now().UnixMilli(),
		}
		payload, _ := json.Marshal(event)

		result, err := stream.Append("events", payload, nil)
		if err != nil {
			log.Fatalf("Append failed: %v", err)
		}
		fmt.Printf("Appended record: id=%s\n", result.ID)
	}

	// Get stream info
	fmt.Println("\n--- Stream info ---")
	info, err := stream.Info("events", nil)
	if err != nil {
		log.Fatalf("Info failed: %v", err)
	}
	fmt.Printf("Stream 'events': first=%s, last=%s, count=%d, bytes=%d\n",
		info.FirstID, info.LastID, info.Count, info.Bytes)

	// Read records from the beginning (default behavior)
	fmt.Println("\n--- Reading from start ---")
	readResult, err := stream.Read("events", &flo.StreamReadOptions{
		Count: flo.Uint32Ptr(10),
	})
	if err != nil {
		log.Fatalf("Read failed: %v", err)
	}
	fmt.Printf("Read %d records\n", len(readResult.Records))
	for _, rec := range readResult.Records {
		fmt.Printf("  id=%s payload=%s\n", rec.ID, string(rec.Payload))
	}

	// Read on from the last record: Start is exclusive, so pass its id
	fmt.Println("\n--- Reading continuation ---")
	if len(readResult.Records) > 0 {
		last := readResult.Records[len(readResult.Records)-1]
		readResult, err = stream.Read("events", &flo.StreamReadOptions{
			Start: &last.ID,
			Count: flo.Uint32Ptr(3),
		})
		if err != nil {
			log.Fatalf("Read from cursor failed: %v", err)
		}
		fmt.Printf("Read %d records from cursor\n", len(readResult.Records))
	}

	// Read from tail (latest)
	fmt.Println("\n--- Reading from tail ---")
	readResult, err = stream.Read("events", &flo.StreamReadOptions{
		Tail:  true,
		Count: flo.Uint32Ptr(2),
	})
	if err != nil {
		log.Fatalf("Read from tail failed: %v", err)
	}
	fmt.Printf("Read %d records from tail\n", len(readResult.Records))

	// Consumer groups
	fmt.Println("\n--- Consumer group operations ---")

	// Read from a consumer group: the records are leased to worker-1 until
	// acked. The first read creates the group.
	groupResult, err := stream.GroupRead("events", "processors", "worker-1", &flo.StreamGroupReadOptions{
		Count: flo.Uint32Ptr(5),
	})
	if err != nil {
		log.Fatalf("GroupRead failed: %v", err)
	}
	fmt.Printf("Group read: %d records\n", len(groupResult.Records))

	// Ack the records within the lease, naming the read's delivery; one not
	// acked in time is delivered again.
	if len(groupResult.Records) > 0 {
		report, err := stream.GroupAck("events", "processors", groupResult.Lease, groupResult.Items(), nil)
		if err != nil {
			log.Fatalf("GroupAck failed: %v", err)
		}
		fmt.Printf("Acknowledged %d records\n", report.Applied)
	}

	// Trim stream (keep only last 10 records)
	fmt.Println("\n--- Trimming stream ---")
	trimmed, err := stream.Trim("events", &flo.StreamTrimOptions{
		MaxLen: flo.Uint64Ptr(10),
	})
	if err != nil {
		log.Fatalf("Trim failed: %v", err)
	}
	fmt.Printf("Trimmed %d records\n", trimmed.Removed)

	fmt.Println("\n=== Done ===")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
