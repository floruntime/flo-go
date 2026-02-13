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
	stream := client.Stream()

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
		fmt.Printf("Appended record: sequence=%d timestamp_ms=%d\n", result.Sequence, result.TimestampMs)
	}

	// Get stream info
	fmt.Println("\n--- Stream info ---")
	info, err := stream.Info("events", nil)
	if err != nil {
		log.Fatalf("Info failed: %v", err)
	}
	fmt.Printf("Stream 'events': first=%d, last=%d, count=%d, bytes=%d\n",
		info.FirstSeq, info.LastSeq, info.Count, info.Bytes)

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
		fmt.Printf("  seq=%d ts=%d payload=%s\n", rec.Sequence, rec.TimestampMs, string(rec.Payload))
	}

	// Read using continuation (use last record's sequence + 1)
	fmt.Println("\n--- Reading continuation ---")
	if len(readResult.Records) > 0 {
		last := readResult.Records[len(readResult.Records)-1]
		startID := flo.NewStreamIDFromSequence(last.Sequence + 1)
		readResult, err = stream.Read("events", &flo.StreamReadOptions{
			Start: &startID,
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

	// Join a consumer group
	if err := stream.GroupJoin("events", "processors", "worker-1", nil); err != nil {
		log.Fatalf("GroupJoin failed: %v", err)
	}
	fmt.Println("Joined consumer group 'processors' as 'worker-1'")

	// Read from consumer group
	groupResult, err := stream.GroupRead("events", "processors", "worker-1", &flo.StreamGroupReadOptions{
		Count: flo.Uint32Ptr(5),
	})
	if err != nil {
		log.Fatalf("GroupRead failed: %v", err)
	}
	fmt.Printf("Group read: %d records\n", len(groupResult.Records))

	// Acknowledge processed records
	if len(groupResult.Records) > 0 {
		seqs := make([]uint64, len(groupResult.Records))
		for i, rec := range groupResult.Records {
			seqs[i] = rec.Sequence
		}
		if err := stream.GroupAck("events", "processors", seqs, nil); err != nil {
			log.Fatalf("GroupAck failed: %v", err)
		}
		fmt.Printf("Acknowledged %d records\n", len(seqs))
	}

	// Leave consumer group
	if err := stream.GroupLeave("events", "processors", "worker-1", nil); err != nil {
		log.Fatalf("GroupLeave failed: %v", err)
	}
	fmt.Println("Left consumer group")

	// Trim stream (keep only last 10 records)
	fmt.Println("\n--- Trimming stream ---")
	if err := stream.Trim("events", &flo.StreamTrimOptions{
		MaxLen: flo.Uint64Ptr(10),
	}); err != nil {
		log.Fatalf("Trim failed: %v", err)
	}
	fmt.Println("Trimmed stream to max 10 records")

	fmt.Println("\n=== Done ===")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
