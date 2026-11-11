package flo

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// live: action round trip. A worker's await, touch, complete and fail, as
// the server answers each: a retried await gets the same delivery, a report
// naming another delivery is a stale report, a resent complete is ok, and a
// fail either counts an attempt or, abandoned, doesn't.
func TestLiveActionRoundTrip(t *testing.T) {
	c := liveClient(t)
	lease, attempts := uint32(5000), uint32(3)
	if err := c.Action.Register("echo", ActionTypeUser, &ActionRegisterOptions{TimeoutMS: &lease, MaxRetries: &attempts}); err != nil {
		t.Fatal(err)
	}
	run, err := c.Action.Invoke("echo", []byte("ping"), nil)
	if err != nil {
		t.Fatal(err)
	}
	wc := c.workerClient("w1")
	block := uint32(5000)
	await := func(key string) *Task {
		t.Helper()
		task, err := wc.Await([]string{"echo"}, &WorkerAwaitOptions{BlockMS: &block, RequestKey: key})
		if err != nil {
			t.Fatalf("await %s: %v", key, err)
		}
		if task == nil {
			t.Fatalf("await %s: no task in %d ms", key, block)
		}
		return task
	}

	task := await("rk-1")
	if task.RunID != run.RunID || task.Namespace != c.Namespace() || task.Deliveries != 1 || task.Action != "echo" ||
		task.CreatedAtMS == 0 || task.Attempt != 1 || task.LeaseMS != 5000 || string(task.Input) != "ping" || task.Term == 0 || task.Index == 0 {
		t.Fatalf("task %+v, want run %s's first delivery", task, run.RunID)
	}
	if again := await("rk-1"); again.Delivery != task.Delivery {
		t.Fatalf("await with the same request key got %+v, want %+v", again.Delivery, task.Delivery)
	}

	expires, err := wc.Touch(task.Delivery, nil)
	if err != nil {
		t.Fatal(err)
	}
	if now := time.Now().UnixMilli(); expires <= now || expires > now+5000 {
		t.Fatalf("touch: expires at %d, now %d with a 5000 ms lease", expires, now)
	}
	if again, err := wc.Touch(task.Delivery, nil); err != nil || again != expires {
		t.Fatalf("second touch: %d, %v; with most of the lease left want %d unchanged", again, err, expires)
	}

	stale := task.Delivery
	stale.Deliveries = 2
	err = wc.Complete(stale, []byte("pong"), nil)
	if !IsStaleReport(err) || !strings.HasPrefix(err.(*StaleReportError).Message, "stale report:") {
		t.Fatalf("complete naming deliveries 2: %v, want a stale report", err)
	}

	if err := wc.Complete(task.Delivery, []byte("pong"), nil); err != nil {
		t.Fatal(err)
	}
	if err := wc.Complete(task.Delivery, []byte("pong"), nil); err != nil {
		t.Fatalf("resent complete: %v, want ok", err)
	}
	err = wc.Complete(task.Delivery, []byte("other"), nil)
	if !IsStaleReport(err) || err.(*StaleReportError).Message != "stale report: the run was completed with another result" {
		t.Fatalf("complete with another result: %v", err)
	}

	st, err := c.Action.Status(run.RunID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != RunStatusCompleted || !bytes.Equal(st.Output, []byte("pong")) || st.Outcome != "" {
		t.Fatalf("status %+v, want completed with pong and no outcome", st)
	}

	// The fail path, on a second run.
	second, err := c.Action.Invoke("echo", []byte("ping"), nil)
	if err != nil {
		t.Fatal(err)
	}
	task = await("rk-2")
	if task.RunID != second.RunID || task.Deliveries != 1 || task.Attempt != 1 {
		t.Fatalf("second run's task %+v", task)
	}
	if err := wc.Fail(task.Delivery, "boom", &WorkerFailOptions{Retry: true}); err != nil {
		t.Fatal(err)
	}
	task = await("rk-3")
	if task.RunID != second.RunID || task.Deliveries != 2 || task.Attempt != 2 {
		t.Fatalf("after a fail: %+v, want deliveries 2, attempt 2", task)
	}
	if err := wc.Fail(task.Delivery, "", &WorkerFailOptions{Retry: true, Abandoned: true}); err != nil {
		t.Fatal(err)
	}
	task = await("rk-4")
	if task.RunID != second.RunID || task.Deliveries != 3 || task.Attempt != 2 {
		t.Fatalf("after an abandon: %+v, want deliveries 3, attempt still 2", task)
	}
}

// Action.Info shows the registration and counts the runs.
func TestLiveActionInfoCountsRuns(t *testing.T) {
	c := liveClient(t)
	lease := uint32(5000)
	if err := c.Action.Register("count", ActionTypeUser, &ActionRegisterOptions{TimeoutMS: &lease, Owner: "billing"}); err != nil {
		t.Fatal(err)
	}
	for range [2]struct{}{} {
		if _, err := c.Action.Invoke("count", nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	info, err := c.Action.Info("count", nil)
	if err != nil {
		t.Fatal(err)
	}
	if info.Owner != "billing" || info.Version != 1 || info.TimeoutMS != 5000 || info.MaxAttempts != 3 || info.Ready != 2 || info.ReadyCapped {
		t.Fatalf("info %+v", info)
	}
	if _, err := c.Action.Info("nope", nil); !IsNotFound(err) {
		t.Fatalf("info of an unknown action: %v, want not found", err)
	}
}

// Peek shows ready messages without taking them; the DLQ of a queue that
// lost none is an empty last page.
func TestLiveQueuePeekAndAnEmptyDLQ(t *testing.T) {
	c := liveClient(t)
	for _, p := range []string{"a", "b", "c"} {
		if _, err := c.Queue.Enqueue("peeked", []byte(p), nil); err != nil {
			t.Fatal(err)
		}
	}
	got, err := c.Queue.Peek("peeked", 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Messages) != 2 || string(got.Messages[0].Payload) != "a" || got.Messages[0].Size != 1 || !got.More {
		t.Fatalf("peek %+v, want a and b with more", got)
	}
	res, err := c.Queue.Dequeue("peeked", 3, &DequeueOptions{Ack: true})
	if err != nil || len(res.Messages) != 3 {
		t.Fatalf("dequeue after peek: %+v, %v; want all three still there", res, err)
	}
	page, err := c.Queue.DLQList("peeked", &DLQListOptions{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Messages) != 0 || page.Total != 0 || page.HasMore {
		t.Fatalf("DLQ page %+v, want empty", page)
	}
}

// GroupList and GroupMembers show a group a consumer joined.
func TestLiveStreamGroupListAndMembers(t *testing.T) {
	c := liveClient(t)
	if _, err := c.Stream.Append("events", []byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Stream.GroupJoin("events", "workers", "c1", nil); err != nil {
		t.Fatal(err)
	}
	groups, err := c.Stream.GroupList("events", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups.Groups) != 1 || groups.Groups[0].Name != "workers" || groups.Groups[0].Members != 1 || groups.More {
		t.Fatalf("groups %+v", groups)
	}
	members, err := c.Stream.GroupMembers("events", "workers", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(members.Members) != 1 || members.Members[0].ID != "c1" || members.Members[0].Leaving || members.More {
		t.Fatalf("members %+v", members)
	}
	if _, err := c.Stream.GroupMembers("events", "nope", nil); !IsNotFound(err) {
		t.Fatalf("members of an unknown group: %v, want not found", err)
	}
}
