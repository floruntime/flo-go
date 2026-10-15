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
    flo.WithRetry(flo.DefaultRetryPolicy), // Retries of refused, not-run requests (the default)
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

// Wait up to 5 seconds for the key to exist (answers at once if it does)
value, err := client.KV.Get("key", &flo.GetOptions{
    WaitMs: 5000,
})

// Wait up to 5 seconds for the key's next version
value, err := client.KV.Get("key", &flo.GetOptions{
    WaitMs:  5000,
    WaitFor: flo.WaitForChange,
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
    Condition: flo.ConditionIfAbsent,
})

// Put only if key exists
err := client.KV.Put("key", []byte("value"), &flo.PutOptions{
    Condition: flo.ConditionIfPresent,
})
```

#### Delete

```go
// Deleting a missing key is not an error; existed says whether it was there
existed, err := client.KV.Delete("key", nil)

// Delete only if the key is still at the version you hold
existed, err := client.KV.Delete("lock", &flo.DeleteOptions{CASVersion: &version})
```

#### MGet

```go
// One entry per key, in order; Found is false for a missing key.
// Up to flo.MaxMGetKeys keys; the SDK follows up until every key is answered.
entries, err := client.KV.MGet([]string{"a", "b", "c"}, nil)
for _, e := range entries {
    fmt.Printf("%s found=%v v%d %s\n", e.Key, e.Found, e.Version, e.Value)
}
```

#### Scan

```go
// Scan returns key names, one page at a time (at most and by default 1024)
limit := uint32(100)
result, err := client.KV.Scan("user:", &flo.ScanOptions{Limit: &limit})
for {
    for _, key := range result.Keys {
        fmt.Println(key)
    }
    if result.Cursor == nil { // last page
        break
    }
    result, err = client.KV.Scan("user:", &flo.ScanOptions{Limit: &limit, Cursor: result.Cursor})
}
```

#### History

```go
// Versions newest first (default 10, at most 65); a missing key has none
entries, err := client.KV.History("key", nil)
for _, entry := range entries {
    fmt.Printf("v%d at %d ms: %s\n", entry.Version, entry.AtMs, entry.Value)
}

limit := uint32(5)
entries, err := client.KV.History("key", &flo.HistoryOptions{Limit: &limit})
```

#### Incr

```go
// Add 1 (or Delta) to a counter; a missing key starts at the delta
res, err := client.KV.Incr("hits", nil)
res, err := client.KV.Incr("hits", &flo.KVIncrOptions{Delta: flo.Int64Ptr(-5)})
fmt.Println(res.Value, res.Version)
```

#### Expire and Exists

```go
// Set an existing key's TTL in milliseconds; 0 clears it
err := client.KV.Expire("session:abc", 60_000, nil)
err := client.KV.Expire("session:abc", 0, nil)

// Renew a lease only if you still own it
err := client.KV.Expire("lease", 30_000, &flo.KVExpireOptions{CASVersion: &version})

ok, err := client.KV.Exists("session:abc", nil)
```

#### JSON documents

```go
// Path "" or "$" is the whole document
put, err := client.KV.JsonSet("user:1", "$", []byte(`{"name":"Ada","age":36}`), nil)
doc, err := client.KV.JsonGet("user:1", "$.name", nil) // nil if key or path is missing
put, err = client.KV.JsonSet("user:1", "$.age", []byte(`37`), nil)
put, err = client.KV.JsonDel("user:1", "$.age", nil) // the new version
put, err = client.KV.JsonDel("user:1", "$", nil)     // deletes the key; put is nil
```

#### Transactions

```go
// A transaction is pinned to the shard that owns its routing key; every key
// in it must route there.
txn, err := client.KV.Begin("user:1", nil)
if err != nil {
    return err
}
defer txn.Rollback() // no-op after Commit

txn.Put("user:1:name", []byte("Ada"), nil)
txn.Incr("user:1:logins", 1)
txn.Expire("user:1:session", 60_000, nil)
txn.Delete("user:1:old", &flo.KVTxnCASOptions{CASVersion: &version})
res, err := txn.Commit()
```

Scan, MGet, History and the JSON ops are not available inside a transaction.

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
        // Server is overloaded, and the client's retries ran out
    } else if flo.IsUnavailable(err) {
        // No leader or the shard isn't taking writes, and the client's
        // retries ran out (the message says whether an operator must act first)
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

### Retries

When the server refuses a request as `overloaded` or `unavailable` and says it
didn't run it (`ServerError.Ran == flo.RanNo`), the client sends it again
itself, as a new request, after a jittered wait. Because the server never ran
it, this is safe for writes too. Any other refusal, or one that may have run,
comes straight back.

| `RetryPolicy` field | Default | |
|---|---|---|
| `MaxAttempts` | 4 | Tries in all, the first included; 1 turns retrying off |
| `InitialBackoff` | 50 ms | Ceiling of the first wait; it doubles each retry |
| `MaxBackoff` | 1 s | Largest ceiling |

Each wait is uniform between 0 and its ceiling, so with the defaults a refused
request is given back after at most about 350 ms of waiting. The request goes
again on the same connection; the server routes it afresh.

The action and stream workers wait the same way between failed polls, from
100 ms up to 5 s, starting again from 100 ms after a poll that succeeds. A
stream worker retries a group join refused this way instead of stopping.

```go
// Up to 8 tries, waiting at most 2 s between them
client := flo.NewClient("localhost:9000", flo.WithRetry(flo.RetryPolicy{
    MaxAttempts: 8, InitialBackoff: 100 * time.Millisecond, MaxBackoff: 2 * time.Second,
}))

// No retries
client := flo.NewClient("localhost:9000", flo.WithRetry(flo.RetryPolicy{MaxAttempts: 1}))
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

Delivery is at-least-once: a handler may see a task or record again, for
example when a dropped connection loses its result, so make handlers safe to
run twice.

## License

Apache-2.0 - see [LICENSE](LICENSE) for details.
