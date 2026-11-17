package flo

import (
	"testing"
	"time"
)

// A steal takes an idle holder's record with a new delivery and lease,
// which reports then name; the old holder's report is stale; a steal
// within the idle time named is refused not_idle.
func TestLiveGroupSteal(t *testing.T) {
	c := liveClient(t)
	if _, err := c.Stream.Append("orders", []byte("a"), nil); err != nil {
		t.Fatal(err)
	}
	held, err := c.Stream.GroupRead("orders", "billing", "c1", nil)
	if err != nil || len(held.Records) != 1 {
		t.Fatalf("read: %+v, %v; want the one record", held, err)
	}
	time.Sleep(20 * time.Millisecond)

	// Held 20 ms: not idle for an hour.
	busy, err := c.Stream.GroupSteal("orders", "billing", "rescue", 3_600_000, held.Items(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(busy.Stolen) != 0 || busy.ExpiresAtMS != 0 || len(busy.Refused) != 1 ||
		busy.Refused[0].ID != held.Records[0].ID || busy.Refused[0].Why != QueueNotIdle || busy.Refused[0].Reason == "" {
		t.Fatalf("steal of a busy record: %+v; want it refused not_idle", busy)
	}

	lease := uint64(60_000)
	got, err := c.Stream.GroupSteal("orders", "billing", "rescue", 10, held.Items(), &StreamGroupStealOptions{LeaseMS: &lease})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	if len(got.Stolen) != 1 || got.Stolen[0].ID != held.Records[0].ID || got.Stolen[0].Deliveries != 2 || len(got.Refused) != 0 ||
		got.Lease.Consumer != "rescue" || got.Lease.Term == 0 || got.Lease.Index == 0 || got.ExpiresAtMS <= now || got.ExpiresAtMS > now+60_000 {
		t.Fatalf("steal: %+v; want the record at deliveries 2, leased 60 s to rescue", got)
	}
	members, err := c.Stream.GroupMembers("orders", "billing", nil)
	if err != nil || len(members.Members) != 1 || members.Members[0].ID != "rescue" {
		t.Fatalf("members after the steal: %+v, %v; want rescue alone", members, err)
	}

	// The old holder's delivery is over.
	stale, err := c.Stream.GroupAck("orders", "billing", held.Lease, held.Items(), nil)
	if err != nil || stale.Applied != 0 || len(stale.Refused) != 1 || stale.Refused[0].Why == QueueGone {
		t.Fatalf("the old holder's ack: %+v, %v; want refused as stale", stale, err)
	}
	// Stealing the same delivery again names one that moved on.
	again, err := c.Stream.GroupSteal("orders", "billing", "other", 1, held.Items(), nil)
	if err != nil || len(again.Stolen) != 0 || len(again.Refused) != 1 {
		t.Fatalf("steal of a moved-on delivery: %+v, %v; want it refused", again, err)
	}
	rep, err := c.Stream.GroupAck("orders", "billing", got.Lease, got.Stolen, nil)
	if err != nil || rep.Applied != 1 || len(rep.Refused) != 0 {
		t.Fatalf("ack of the stolen record: %+v, %v", rep, err)
	}

	if _, err := c.Stream.GroupSteal("orders", "nope", "rescue", 1, held.Items(), nil); !IsNotFound(err) {
		t.Fatalf("steal from an unknown group: %v, want not found", err)
	}
}

// deadRecords appends n records and kills each in group (MaxDeliver 1, a
// nack each), returning their ids in order.
func deadRecords(t *testing.T, c *Client, stream, group string, n int) []StreamID {
	t.Helper()
	attempts := uint8(1)
	if _, err := c.Stream.GroupCreate(stream, group, &StreamGroupCreateOptions{MaxDeliver: &attempts}); err != nil {
		t.Fatal(err)
	}
	var ids []StreamID
	for i := 0; i < n; i++ {
		app, err := c.Stream.Append(stream, []byte{byte('a' + i)}, nil)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, app.ID)
		got, err := c.Stream.GroupRead(stream, group, "c1", nil)
		if err != nil || len(got.Records) != 1 {
			t.Fatalf("read %d: %+v, %v", i, got, err)
		}
		rep, err := c.Stream.GroupNack(stream, group, got.Lease, got.Items(), &StreamGroupNackOptions{Error: "bad input"})
		if err != nil || rep.Applied != 1 {
			t.Fatalf("nack %d: %+v, %v", i, rep, err)
		}
	}
	return ids
}

// A group's dead letters list a page at a time, oldest dead first; a
// requeue makes one ready again with its failures reset; a delete (dry run
// first) removes them; one not dead is refused not_dead.
func TestLiveGroupDeadLetters(t *testing.T) {
	c := liveClient(t)
	ids := deadRecords(t, c, "orders", "billing", 3)

	var dead []StreamGroupDeadLetter
	opts := &StreamGroupDLQListOptions{Limit: 2}
	for pages := 1; ; pages++ {
		page, err := c.Stream.GroupDLQList("orders", "billing", opts)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Dead) > 2 || pages > 3 {
			t.Fatalf("page %d: %+v", pages, page)
		}
		dead = append(dead, page.Dead...)
		if page.Cursor == nil {
			break
		}
		opts.After = page.Cursor
	}
	if len(dead) != 3 {
		t.Fatalf("listed %+v, want 3", dead)
	}
	for i, d := range dead {
		if d.ID != ids[i] || d.Deliveries != 1 || d.Failures != 1 || d.Error != "bad input" || d.DeadAtMS <= 0 {
			t.Errorf("dead letter %d: %+v, want %v, 1 delivery, 1 failure, its error", i, d, ids[i])
		}
	}

	rep, err := c.Stream.GroupDLQRequeue("orders", "billing", ids[:1], nil)
	if err != nil || rep.Applied != 1 || len(rep.Refused) != 0 || rep.More {
		t.Fatalf("requeue: %+v, %v", rep, err)
	}
	back, err := c.Stream.GroupRead("orders", "billing", "c2", nil)
	if err != nil || len(back.Records) != 1 || back.Records[0].ID != ids[0] || back.Records[0].Deliveries != 2 {
		t.Fatalf("read after the requeue: %+v, %v; want %v at deliveries 2", back, err, ids[0])
	}
	// Leased, not dead: a requeue or delete of it is refused not_dead.
	rep, err = c.Stream.GroupDLQRequeue("orders", "billing", ids[:1], nil)
	if err != nil || rep.Applied != 0 || len(rep.Refused) != 1 || rep.Refused[0].ID != ids[0] || rep.Refused[0].Why != QueueNotDead {
		t.Fatalf("requeue of a leased record: %+v, %v; want refused not_dead", rep, err)
	}
	rep, err = c.Stream.GroupDLQDelete("orders", "billing", ids[:1], &StreamGroupDLQDeleteOptions{DryRun: true})
	if err != nil || rep.Applied != 0 || len(rep.Refused) != 1 || rep.Refused[0].Why != QueueNotDead {
		t.Fatalf("dry-run delete of a leased record: %+v, %v; want refused not_dead", rep, err)
	}

	rep, err = c.Stream.GroupDLQDelete("orders", "billing", nil, &StreamGroupDLQDeleteOptions{All: true, DryRun: true})
	if err != nil || rep.Applied != 2 || len(rep.Refused) != 0 || rep.More {
		t.Fatalf("dry-run delete of all: %+v, %v; want 2", rep, err)
	}
	rep, err = c.Stream.GroupDLQDelete("orders", "billing", nil, &StreamGroupDLQDeleteOptions{All: true, Limit: 1})
	if err != nil || rep.Applied != 1 || !rep.More {
		t.Fatalf("delete one of all: %+v, %v; want 1 with more", rep, err)
	}
	rep, err = c.Stream.GroupDLQDelete("orders", "billing", nil, &StreamGroupDLQDeleteOptions{All: true})
	if err != nil || rep.Applied != 1 || rep.More {
		t.Fatalf("delete the rest: %+v, %v; want 1, no more", rep, err)
	}
	page, err := c.Stream.GroupDLQList("orders", "billing", nil)
	if err != nil || len(page.Dead) != 0 || page.Cursor != nil {
		t.Fatalf("list after the deletes: %+v, %v; want empty", page, err)
	}
	info, err := c.Stream.GroupInfo("orders", "billing", nil)
	if err != nil || info.Dead != 0 || info.Leased != 1 {
		t.Fatalf("info: %+v, %v; want none dead, one leased", info, err)
	}
	if _, err := c.Stream.GroupDLQList("orders", "nope", nil); !IsNotFound(err) {
		t.Fatalf("dead letters of an unknown group: %v, want not found", err)
	}
	if _, err := c.Stream.GroupDLQRequeue("orders", "billing", nil, nil); err == nil {
		t.Fatal("a requeue naming nothing was sent")
	}
}

// A queue's dead letters list a page at a time with their payloads and
// errors; a requeue makes one ready again; a delete removes them; one not
// dead is refused not_dead.
func TestLiveQueueDeadLetters(t *testing.T) {
	c := liveClient(t)
	var seqs []uint64
	for _, p := range []string{"a", "b", "c"} {
		seq, err := c.Queue.Enqueue("jobs", []byte(p), &EnqueueOptions{Priority: 3})
		if err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, seq)
	}
	got, err := c.Queue.Dequeue("jobs", 3, &DequeueOptions{Consumer: "c1"})
	if err != nil || len(got.Messages) != 3 {
		t.Fatalf("dequeue: %+v, %v", got, err)
	}
	rep, err := c.Queue.Fail("jobs", got.Lease, got.Items(), &QueueFailOptions{Dead: true, Error: "bad input"})
	if err != nil || rep.Applied != 3 {
		t.Fatalf("fail dead: %+v, %v", rep, err)
	}

	var dead []DeadLetter
	opts := &DLQListOptions{Limit: 2}
	for pages := 1; ; pages++ {
		page, err := c.Queue.DLQList("jobs", opts)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Messages) > 2 || pages > 3 {
			t.Fatalf("page %d: %+v", pages, page)
		}
		dead = append(dead, page.Messages...)
		if page.Cursor == nil {
			break
		}
		opts.After = page.Cursor
	}
	if len(dead) != 3 {
		t.Fatalf("listed %+v, want 3", dead)
	}
	seen := map[uint64]bool{}
	for _, d := range dead {
		seen[d.Seq] = true
		if d.Priority != 3 || d.Deliveries != 1 || d.Failures != 1 || d.Error != "bad input" || d.Size != 1 || len(d.Payload) != 1 ||
			d.EnqueuedAtMS <= 0 || d.DeadAtMS < d.EnqueuedAtMS {
			t.Errorf("dead letter %+v", d)
		}
	}
	for _, s := range seqs {
		if !seen[s] {
			t.Errorf("seq %d not listed", s)
		}
	}

	rep2, err := c.Queue.DLQRequeue("jobs", seqs[:1], nil)
	if err != nil || rep2.Applied != 1 || len(rep2.Refused) != 0 {
		t.Fatalf("requeue: %+v, %v", rep2, err)
	}
	// Ready, not dead: refused not_dead.
	rep2, err = c.Queue.DLQDelete("jobs", seqs[:1], nil)
	if err != nil || rep2.Applied != 0 || len(rep2.Refused) != 1 || rep2.Refused[0].Seq != seqs[0] || rep2.Refused[0].Why != QueueNotDead || rep2.Refused[0].Reason == "" {
		t.Fatalf("delete of a ready message: %+v, %v; want refused not_dead", rep2, err)
	}
	back, err := c.Queue.Dequeue("jobs", 1, &DequeueOptions{Consumer: "c2", Ack: true})
	if err != nil || len(back.Messages) != 1 || back.Messages[0].Seq != seqs[0] || back.Messages[0].Deliveries != 2 {
		t.Fatalf("dequeue after the requeue: %+v, %v; want seq %d at deliveries 2", back, err, seqs[0])
	}
	// Completed and gone.
	rep2, err = c.Queue.DLQRequeue("jobs", seqs[:1], nil)
	if err != nil || rep2.Applied != 0 || len(rep2.Refused) != 1 || rep2.Refused[0].Why != QueueGone {
		t.Fatalf("requeue of a completed message: %+v, %v; want refused gone", rep2, err)
	}

	rep2, err = c.Queue.DLQDelete("jobs", nil, &DLQDeleteOptions{All: true, DryRun: true})
	if err != nil || rep2.Applied != 2 || rep2.More {
		t.Fatalf("dry-run delete: %+v, %v; want 2", rep2, err)
	}
	rep2, err = c.Queue.DLQDelete("jobs", nil, &DLQDeleteOptions{All: true, Limit: 1})
	if err != nil || rep2.Applied != 1 || !rep2.More {
		t.Fatalf("delete one of all: %+v, %v; want 1 with more", rep2, err)
	}
	rep2, err = c.Queue.DLQRequeue("jobs", nil, &DLQRequeueOptions{All: true})
	if err != nil || rep2.Applied != 1 || rep2.More {
		t.Fatalf("requeue the rest: %+v, %v; want 1, no more", rep2, err)
	}
	page, err := c.Queue.DLQList("jobs", nil)
	if err != nil || len(page.Messages) != 0 || page.Cursor != nil {
		t.Fatalf("list after: %+v, %v; want empty", page, err)
	}
	if _, err := c.Queue.DLQList("nope", nil); !IsNotFound(err) {
		t.Fatalf("dead letters of an unknown queue: %v, want not found", err)
	}
}

// An alter on a stream an append made works, and info shows it; after it a
// create must name the settings the stream has, or it is a conflict.
func TestLiveStreamAlterAnAppendMadeStream(t *testing.T) {
	c := liveClient(t)
	if _, err := c.Stream.Append("made", []byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Stream.Alter("made", &StreamAlterOptions{RetentionCount: 50, RetentionAgeS: 3600}); err != nil {
		t.Fatalf("alter of an append-made stream: %v", err)
	}
	info, err := c.Stream.Info("made", nil)
	if err != nil || info.RetentionCount != 50 || info.RetentionAgeS != 3600 || info.PartitionCount != 1 || info.Count != 1 {
		t.Fatalf("info after the alter: %+v, %v; want retention 50 records, 3600 s", info, err)
	}
	if err := c.Stream.Create("made", nil); !IsConflict(err) {
		t.Fatalf("create with other settings after the alter: %v, want a conflict", err)
	}
	if err := c.Stream.Create("made", &StreamCreateOptions{RetentionCount: 50, RetentionAgeS: 3600}); err != nil {
		t.Fatalf("create with the stream's settings: %v", err)
	}
	if err := c.Stream.Alter("never", &StreamAlterOptions{RetentionCount: 5}); !IsNotFound(err) {
		t.Fatalf("alter of a stream that doesn't exist: %v, want not found", err)
	}

	// A create of a stream an append made, never altered, gives it the
	// settings asked.
	if _, err := c.Stream.Append("plain", []byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Stream.Create("plain", &StreamCreateOptions{RetentionCount: 7}); err != nil {
		t.Fatalf("create of an append-made stream: %v", err)
	}
	if info, err := c.Stream.Info("plain", nil); err != nil || info.RetentionCount != 7 {
		t.Fatalf("info after the create: %+v, %v", info, err)
	}
}
