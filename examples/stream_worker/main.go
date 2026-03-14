// Example: StreamWorker API usage with the Flo Go SDK
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	flo "github.com/floruntime/flo-go"
)

func main() {
	client := flo.NewClient(getEnv("FLO_ENDPOINT", "localhost:3000"),
		flo.WithNamespace(getEnv("FLO_NAMESPACE", "myapp")),
	)
	if err := client.Connect(); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	// Create a stream worker that processes records from "events" stream with "default" group
	sw, err := client.NewStreamWorker(flo.StreamWorkerOptions{Stream: "events"}, processEvent)
	if err != nil {
		log.Fatalf("Failed to create stream worker: %v", err)
	}
	defer sw.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down...")
		sw.Stop()
	}()

	log.Println("Stream worker starting...")
	if err := sw.Start(ctx); err != nil && err != context.Canceled {
		log.Fatalf("Stream worker error: %v", err)
	}
}

type Event struct {
	Type    string `json:"type"`
	UserID  string `json:"user_id"`
	Payload string `json:"payload"`
}

func processEvent(sctx *flo.StreamContext) error {
	var event Event
	if err := sctx.Into(&event); err != nil {
		return fmt.Errorf("invalid event: %w", err)
	}

	log.Printf("Processing event id=%s type=%s user=%s",
		sctx.StreamID(), event.Type, event.UserID)

	return nil // auto-ack
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
