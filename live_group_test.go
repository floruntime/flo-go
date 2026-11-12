package flo

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A group read leases records until acked: an acked record isn't read
// again, one whose lease runs out is delivered again with deliveries 2, a
// report naming the old delivery applies nothing, and a nack past
// MaxDeliver leaves the record dead.
func TestLiveGroupLeasesRecordsUntilAcked(t *testing.T) {
	c := liveClient(t)
	lease, attempts := uint32(MinGroupAckTimeoutMS), uint8(2)
	made, err := c.Stream.GroupCreate("orders", "billing", &StreamGroupCreateOptions{AckTimeoutMS: &lease, MaxDeliver: &attempts})
	if err != nil || !made {
		t.Fatalf("create = %v, %v", made, err)
	}
	if _, err := c.Stream.GroupCreate("orders", "billing", nil); !IsConflict(err) {
		t.Fatalf("create with other settings: %v, want a conflict", err)
	}
	if _, err := c.Stream.Append("orders", []byte("a"), nil); err != nil {
		t.Fatal(err)
	}

	got, err := c.Stream.GroupRead("orders", "billing", "c1", &StreamGroupReadOptions{Count: Uint32Ptr(10)})
	if err != nil || len(got.Records) != 1 || string(got.Records[0].Payload) != "a" || got.Records[0].Deliveries != 1 {
		t.Fatalf("first read: %+v, %v; want a, first delivery", got, err)
	}
	if got.ExpiresAtMS <= time.Now().UnixMilli() {
		t.Errorf("lease runs out at %d, before now", got.ExpiresAtMS)
	}
	rep, err := c.Stream.GroupAck("orders", "billing", got.Lease, got.Items(), nil)
	if err != nil || rep.Applied != 1 || len(rep.Refused) != 0 {
		t.Fatalf("ack: %+v, %v", rep, err)
	}
	again, err := c.Stream.GroupRead("orders", "billing", "c1", nil)
	if err != nil || len(again.Records) != 0 {
		t.Fatalf("read after the ack: %+v, %v; want nothing", again, err)
	}

	// Left to run out, a lease is delivered again.
	if _, err := c.Stream.Append("orders", []byte("b"), nil); err != nil {
		t.Fatal(err)
	}
	first, err := c.Stream.GroupRead("orders", "billing", "c1", nil)
	if err != nil || len(first.Records) != 1 || string(first.Records[0].Payload) != "b" {
		t.Fatalf("read b: %+v, %v", first, err)
	}
	block := uint32(30000)
	second, err := c.Stream.GroupRead("orders", "billing", "c2", &StreamGroupReadOptions{BlockMS: &block})
	if err != nil || len(second.Records) != 1 || string(second.Records[0].Payload) != "b" || second.Records[0].Deliveries != 2 {
		t.Fatalf("read after the lease ran out: %+v, %v; want b, deliveries 2", second, err)
	}

	// The first delivery's ack is stale: it applies nothing, and says why.
	stale, err := c.Stream.GroupAck("orders", "billing", first.Lease, first.Items(), nil)
	if err != nil || stale.Applied != 0 || len(stale.Refused) != 1 || stale.Refused[0].Why != QueueDeliveredAgain {
		t.Fatalf("stale ack: %+v, %v; want refused delivered_again", stale, err)
	}

	// The second nack is its last attempt: the record is dead.
	rep, err = c.Stream.GroupNack("orders", "billing", second.Lease, second.Items(), &StreamGroupNackOptions{Error: "bad"})
	if err != nil || rep.Applied != 1 {
		t.Fatalf("nack: %+v, %v", rep, err)
	}
	info, err := c.Stream.GroupInfo("orders", "billing", nil)
	if err != nil || info.Dead != 1 || info.Ready != 0 || info.Leased != 0 {
		t.Fatalf("info after the last nack: %+v, %v; want one dead", info, err)
	}
	if after, err := c.Stream.GroupRead("orders", "billing", "c1", nil); err != nil || len(after.Records) != 0 {
		t.Fatalf("read after it died: %+v, %v; want nothing", after, err)
	}
	if n, err := c.Stream.GroupDelete("orders", "billing", nil); err != nil || n != 1 {
		t.Fatalf("delete = %d, %v; want the dead record dropped", n, err)
	}
	if _, err := c.Stream.GroupInfo("orders", "billing", nil); !IsNotFound(err) {
		t.Fatalf("info of a deleted group: %v, want not found", err)
	}
}

// The removed ops are refused, naming what replaces them.
func TestLiveRemovedGroupOpsAreRefused(t *testing.T) {
	c := liveClient(t)
	if _, err := c.Stream.Append("s", []byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	w, _ := groupValue("g", nil, 0)
	for op, names := range map[OpCode]string{0x151: "read to take some", 0x152: "read to take some", 0x155: "a read takes expired and nacked records", 0x157: "set when it is created"} {
		_, err := c.sendAndCheck(op, c.Namespace(), []byte("s"), w.buf, nil, false)
		var se *ServerError
		if !IsBadRequest(err) || !errors.As(err, &se) || !strings.Contains(se.Message, names) {
			t.Errorf("op 0x%x: %v, want a bad request naming its replacement", op, err)
		}
	}
}

// A touch keeps a lease, and an ack from another consumer is refused.
func TestLiveGroupTouchAndAnotherHoldersAck(t *testing.T) {
	c := liveClient(t)
	lease := uint32(MinGroupAckTimeoutMS)
	if _, err := c.Stream.GroupCreate("s", "g", &StreamGroupCreateOptions{AckTimeoutMS: &lease}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Stream.Append("s", []byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	got, err := c.Stream.GroupRead("s", "g", "c1", nil)
	if err != nil || len(got.Records) != 1 {
		t.Fatalf("read: %+v, %v", got, err)
	}
	time.Sleep(3 * time.Second) // past half the lease, so a touch renews it
	rep, err := c.Stream.GroupTouch("s", "g", got.Lease, got.Items(), nil)
	if err != nil || rep.Applied != 1 || rep.ExpiresAtMS <= got.ExpiresAtMS {
		t.Fatalf("touch: %+v, %v; want the lease past %d", rep, err, got.ExpiresAtMS)
	}
	other := got.Lease
	other.Consumer = "c2"
	if rep, err := c.Stream.GroupAck("s", "g", other, got.Items(), nil); err != nil || rep.Applied != 0 || rep.Refused[0].Why != QueueOtherHolder {
		t.Fatalf("another consumer's ack: %+v, %v; want refused other_holder", rep, err)
	}
	if rep, err := c.Stream.GroupAck("s", "g", got.Lease, got.Items(), nil); err != nil || rep.Applied != 1 {
		t.Fatalf("ack after the touch: %+v, %v", rep, err)
	}
}

// A stream worker acks what it handles: a second worker on the group finds
// nothing left.
func TestLiveStreamWorkerAcksWhatItHandles(t *testing.T) {
	c := liveClient(t)
	for _, p := range []string{"1", "2", "3"} {
		if _, err := c.Stream.Append("jobs", []byte(p), nil); err != nil {
			t.Fatal(err)
		}
	}
	var handled atomic.Int32
	sw, err := c.NewStreamWorker(StreamWorkerOptions{Stream: "jobs", Group: "w", BlockMS: 500, AutoTouch: true, Logger: &captureLogger{}},
		func(*StreamContext) error { handled.Add(1); return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer sw.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sw.Start(ctx) }()
	deadline := time.Now().Add(10 * time.Second)
	for handled.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Start = %v", err)
	}
	if n := handled.Load(); n != 3 {
		t.Fatalf("handled %d records, want 3", n)
	}
	info, err := c.Stream.GroupInfo("jobs", "w", nil)
	if err != nil || info.Pending != 0 || info.Dead != 0 {
		t.Fatalf("info: %+v, %v; want everything acked", info, err)
	}
}

// Keys sharing a {tag} commit together; a key without it is refused.
func TestLiveKVTransactionOverATag(t *testing.T) {
	c := liveClient(t)
	txn, err := c.KV.Begin("{t}a", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Put("{t}a", []byte("1"), nil); err != nil {
		t.Fatal(err)
	}
	if err := txn.Incr("{t}b", 5); err != nil {
		t.Fatal(err)
	}
	if res, err := txn.Commit(); err != nil || res == nil {
		t.Fatalf("commit: %+v, %v", res, err)
	}
	if got, err := c.KV.Get("{t}a", nil); err != nil || got == nil || string(got.Value) != "1" {
		t.Fatalf("{t}a after commit: %+v, %v", got, err)
	}
	if got, err := c.KV.Incr("{t}b", &KVIncrOptions{Delta: new(int64)}); err != nil || got.Value != 5 {
		t.Fatalf("{t}b after commit: %+v, %v", got, err)
	}
	entries, err := c.KV.MGet([]string{"{t}a", "plain-missing"}, nil)
	if err != nil || len(entries) != 2 || !entries[0].Found || entries[1].Found {
		t.Fatalf("mget across shards: %+v, %v", entries, err)
	}

	txn, err = c.KV.Begin("{t}a", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Rollback()
	_, err = txn.Put("plain", []byte("x"), nil)
	var se *ServerError
	if !IsBadRequest(err) || !errors.As(err, &se) || se.Reason != ReasonKeysNotColocated || se.Ran != RanNo {
		t.Fatalf("put of an untagged key: %v, want keys_not_colocated", err)
	}
}
