# Flo Go SDK

Go client SDK for the [Flo](https://github.com/floruntime/flo) distributed systems platform.

## Installation

```bash
go get github.com/floruntime/flo-go
```

## Quick Start

```go
package main

import (
    "fmt"
    "log"

    flo "github.com/floruntime/flo-go"
)

func main() {
    // Create and connect client
    client := flo.NewClient("localhost:9000",
        flo.WithNamespace("myapp"),
    )
    if err := client.Connect(); err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // KV operations
    client.KV.Put("user:123", []byte("John Doe"), nil)
    value, _ := client.KV.Get("user:123", nil)
    fmt.Printf("Got: %s\n", value)

    // Queue operations
    client.Queue.Enqueue("tasks", []byte(`{"task":"process"}`), nil)
    result, _ := client.Queue.Dequeue("tasks", 10, nil)
    for _, msg := range result.Messages {
        fmt.Printf("Processing: %s\n", msg.Payload)
        client.Queue.Ack("tasks", []uint64{msg.Seq}, nil)
    }

    // Stream operations
    client.Stream.Append("events", []byte(`{"event":"login"}`), nil)
    records, _ := client.Stream.Read("events", nil)
    for _, rec := range records.Records {
        fmt.Printf("Event: %s\n", rec.Payload)
    }
}
```

## API Reference

### Client

```go
// Create a new client
client := flo.NewClient("localhost:9000",
    flo.WithNamespace("default"),    // Default namespace for operations
    flo.WithTimeout(5 * time.Second), // Connection/operation timeout
    flo.WithDebug(true),              // Enable debug logging
)

// Connect to server
err := client.Connect()

// Close connection
client.Close()

// Check connection status
if client.IsConnected() { ... }
```

### KV Operations

#### Get

```go
// Simple get
value, err := client.KV.Get("key", nil)
if value == nil {
    // Key not found
}

// Get with namespace override
value, err := client.KV.Get("key", &flo.GetOptions{
    Namespace: "other-namespace",
})

// Get with blocking (long polling - wait for key to appear)
blockMS := uint32(5000) // 5 seconds
value, err := client.KV.Get("key", &flo.GetOptions{
    BlockMS: &blockMS,
})
```

#### Put

```go
// Simple put
err := client.KV.Put("key", []byte("value"), nil)

// Put with TTL (expires in 1 hour)
ttl := uint64(3_600_000) // milliseconds
err := client.KV.Put("key", []byte("value"), &flo.PutOptions{
    TTLMs: &ttl,
})

// Put with CAS (optimistic locking)
version := uint64(1)
err := client.KV.Put("key", []byte("new-value"), &flo.PutOptions{
    CASVersion: &version,
})
if flo.IsConflict(err) {
    // Version mismatch - value was modified by another client
}

// Put only if key doesn't exist
err := client.KV.Put("key", []byte("value"), &flo.PutOptions{
    IfNotExists: true,
})

// Put only if key exists
err := client.KV.Put("key", []byte("value"), &flo.PutOptions{
    IfExists: true,
})
```

#### Delete

```go
// Delete succeeds even if key doesn't exist
err := client.KV.Delete("key", nil)
```

#### Scan

```go
// Scan all keys with prefix
result, err := client.KV.Scan("user:", nil)
for _, entry := range result.Entries {
    fmt.Printf("%s = %s\n", entry.Key, entry.Value)
}

// Paginated scan
limit := uint32(100)
result, err := client.KV.Scan("user:", &flo.ScanOptions{Limit: &limit})
for result.HasMore {
    result, err = client.KV.Scan("user:", &flo.ScanOptions{Cursor: result.Cursor})
    // Process result.Entries...
}
```

#### History

```go
// Get version history
entries, err := client.KV.History("key", nil)
for _, entry := range entries {
    fmt.Printf("v%d at %d: %s\n", entry.Version, entry.Timestamp, entry.Value)
}

// Limit history entries
limit := uint32(10)
entries, err := client.KV.History("key", &flo.HistoryOptions{Limit: &limit})
```

### Queue Operations

#### Enqueue

```go
// Simple enqueue
seq, err := client.Queue.Enqueue("tasks", []byte(`{"task":"process"}`), nil)

// Enqueue with priority (lower is taken first)
seq, err := client.Queue.Enqueue("tasks", payload, &flo.EnqueueOptions{
    Priority: 10,
})
```

#### Dequeue

```go
// Dequeue up to 10 messages
result, err := client.Queue.Dequeue("tasks", 10, nil)
for _, msg := range result.Messages {
    // Process message
    fmt.Printf("seq=%d payload=%s\n", msg.Seq, msg.Payload)
}

// Long polling (wait up to 30 seconds for messages)
blockMS := uint32(30000)
result, err := client.Queue.Dequeue("tasks", 10, &flo.DequeueOptions{
    BlockMS: &blockMS,
})
```

#### Ack/Nack

A dequeue consumes its messages; queues are currently at-most-once, so Ack and Nack have no effect on a dequeued message and a dequeued message does not reach the DLQ.

```go
err := client.Queue.Ack("tasks", []uint64{msg.Seq}, nil)
err := client.Queue.Nack("tasks", []uint64{msg.Seq}, nil)
```

#### DLQ Operations

```go
// List DLQ messages
result, err := client.Queue.DLQList("tasks", nil)

// Requeue messages from DLQ back to main queue
seqs := []uint64{msg1.Seq, msg2.Seq}
err := client.Queue.DLQRequeue("tasks", seqs, nil)
```

#### Peek

```go
// Peek at messages without creating leases (no visibility timeout)
// Messages remain available for other consumers
result, err := client.Queue.Peek("tasks", 10, nil)
for _, msg := range result.Messages {
    fmt.Printf("Peeking: seq=%d payload=%s\n", msg.Seq, msg.Payload)
}
```

## Error Handling

```go
// Check specific error types
value, err := client.KV.Get("key", nil)
if err != nil {
    if flo.IsNotFound(err) {
        // Key doesn't exist
    } else if flo.IsConflict(err) {
        // CAS version mismatch
    } else if flo.IsBadRequest(err) {
        // Invalid request parameters
    } else if flo.IsUnauthorized(err) {
        // Authentication failed
    } else if flo.IsOverloaded(err) {
        // Server is overloaded, retry later
    } else if flo.IsInternal(err) {
        // Internal server error
    }
}

// Use errors.Is for error matching
if errors.Is(err, flo.ErrNotConnected) {
    // Client not connected
}
if errors.Is(err, flo.ErrConnectionFailed) {
    // Connection failed
}
```

## Thread Safety

The client uses a mutex to ensure thread-safe access to the connection. Multiple goroutines can safely use the same client instance. After any I/O error, including a timeout, calls return `ErrNotConnected` until `Reconnect`.

## Example: Worker Pattern

```go
// Create and connect client
client := flo.NewClient("localhost:9000",
    flo.WithNamespace("myapp"),
)
client.Connect()
defer client.Close()

// Create a worker from the client
w, err := client.NewWorker(flo.WorkerOptions{
    Concurrency: 10,
})
if err != nil {
    log.Fatal(err)
}
defer w.Close()

// Register action handlers
w.MustRegisterAction("process-order", func(actx *flo.ActionContext) ([]byte, error) {
    var input map[string]interface{}
    actx.Into(&input)
    // Process the order...
    return actx.Bytes(map[string]string{"status": "done"})
})

// Start the worker (blocks until context is cancelled)
w.Start(ctx)
```

## License

MIT License - see [LICENSE](LICENSE) for details.
