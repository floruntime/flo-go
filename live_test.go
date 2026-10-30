package flo

// Live tests against a real flo server: FLO_ENDPOINT names it (CI starts the
// commit pinned in .github/flo-ref.env), and FLO_BINARY the binary, for the
// version check. FLO_EXPIRY_ENDPOINT names a second one whose config removes a
// worker after a second of silence ([workers] heartbeat_timeout_ms and
// remove_after_ms = 1000). Without their endpoint they skip, except under
// CI=1, where a missing server is a failure, not a skip.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func liveEndpoint(t *testing.T) string {
	t.Helper()
	return liveEnv(t, "FLO_ENDPOINT")
}

func liveEnv(t *testing.T, name string) string {
	t.Helper()
	endpoint := os.Getenv(name)
	if endpoint == "" {
		if os.Getenv("CI") == "1" {
			t.Fatalf("CI=1 but %s is not set", name)
		}
		t.Skipf("%s is not set", name)
	}
	return endpoint
}

// liveClient is a connected client in a namespace of its own, so tests on
// one server don't see each other's keys, streams or actions.
func liveClient(t *testing.T) *Client {
	t.Helper()
	return liveClientAt(t, liveEndpoint(t))
}

func liveClientAt(t *testing.T, endpoint string) *Client {
	t.Helper()
	suffix := make([]byte, 4)
	_, _ = rand.Read(suffix)
	c := NewClient(endpoint, WithNamespace("live-"+hex.EncodeToString(suffix)), WithTimeout(10*time.Second))
	if err := c.Connect(); err != nil {
		t.Fatalf("connect %s: %v", endpoint, err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestLiveServerSpeaksThisSDKsProtocolAndTable(t *testing.T) {
	liveEndpoint(t)
	binary := os.Getenv("FLO_BINARY")
	if binary == "" {
		// The one test that checks the pin mustn't vanish quietly in CI.
		if os.Getenv("CI") == "1" {
			t.Fatal("CI=1 but FLO_BINARY is not set")
		}
		t.Skip("FLO_BINARY is not set")
	}
	cmd := exec.Command(binary, "version", "--verbose")
	cmd.Env = append(os.Environ(), "HOME="+t.TempDir())
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("%s version --verbose: %v", binary, err)
	}
	m := regexp.MustCompile(`protocol (\d+), table 0x([0-9a-f]{16})`).FindSubmatch(out)
	if m == nil {
		t.Fatalf("no protocol and table in %q", out)
	}
	protocol, _ := strconv.Atoi(string(m[1]))
	table, _ := strconv.ParseUint(string(m[2]), 16, 64)
	if protocol != int(Version) || table != TableHash {
		t.Fatalf("server protocol %d table 0x%016x; SDK %d 0x%016x", protocol, table, Version, TableHash)
	}
}

func TestLiveKVPutGetAndMGet(t *testing.T) {
	c := liveClient(t)
	if _, err := c.KV.Put("a", []byte("1"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.KV.Put("b", []byte("2"), nil); err != nil {
		t.Fatal(err)
	}
	got, err := c.KV.Get("a", nil)
	if err != nil || got == nil || string(got.Value) != "1" {
		t.Fatalf("get a = %+v, %v", got, err)
	}
	entries, err := c.KV.MGet([]string{"a", "missing", "b"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []MGetEntry{{Key: "a", Value: []byte("1"), Found: true}, {Key: "missing"}, {Key: "b", Value: []byte("2"), Found: true}}
	if len(entries) != len(want) {
		t.Fatalf("mget = %+v", entries)
	}
	for i, e := range entries {
		w := want[i]
		if e.Key != w.Key || e.Found != w.Found || !bytes.Equal(e.Value, w.Value) || (e.Found && e.Version == 0) {
			t.Errorf("mget[%d] = %+v, want %+v (with a version when found)", i, e, w)
		}
	}
}

func TestLiveKVGetShowsTheExpiry(t *testing.T) {
	c := liveClient(t)
	ttl := uint64(60000)
	before := time.Now().UnixMilli()
	if _, err := c.KV.Put("ttl", []byte("v"), &PutOptions{TTLMs: &ttl}); err != nil {
		t.Fatal(err)
	}
	after := time.Now().UnixMilli()
	got, err := c.KV.Get("ttl", nil)
	if err != nil || got == nil || got.ExpiresAtMs == nil {
		t.Fatalf("get = %+v, %v; want an expiry", got, err)
	}
	if at := *got.ExpiresAtMs; at < before+int64(ttl) || at > after+int64(ttl) {
		t.Errorf("expires at %d, want within [%d, %d]", at, before+int64(ttl), after+int64(ttl))
	}
	if _, err := c.KV.Put("plain", []byte("v"), nil); err != nil {
		t.Fatal(err)
	}
	if got, err := c.KV.Get("plain", nil); err != nil || got == nil || got.ExpiresAtMs != nil {
		t.Errorf("get without a TTL = %+v, %v; want no expiry", got, err)
	}
}

func TestLiveKVHistoryMarksTheDelete(t *testing.T) {
	c := liveClient(t)
	if _, err := c.KV.Put("k", []byte("one"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.KV.Delete("k", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.KV.Put("k", []byte("two"), nil); err != nil {
		t.Fatal(err)
	}
	page, err := c.KV.History("k", nil)
	if err != nil {
		t.Fatal(err)
	}
	hist := page.Versions
	if len(hist) != 3 || page.More || hist[0].Deleted || !hist[1].Deleted || hist[2].Deleted ||
		string(hist[0].Value) != "two" || string(hist[2].Value) != "one" {
		t.Fatalf("history = %+v; want two, a delete, one", hist)
	}
}

// Five versions read two at a time: three pages, newest first, each seen
// once; before the oldest there is nothing.
func TestLiveKVHistoryPages(t *testing.T) {
	c := liveClient(t)
	var written []uint64
	for i := 0; i < 5; i++ {
		r, err := c.KV.Put("k", []byte(strconv.Itoa(i)), nil)
		if err != nil {
			t.Fatal(err)
		}
		written = append([]uint64{r.Version}, written...)
	}
	limit := uint32(2)
	var seen []uint64
	var before *uint64
	for pages := 1; ; pages++ {
		page, err := c.KV.History("k", &HistoryOptions{Limit: &limit, BeforeVersion: before})
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range page.Versions {
			seen = append(seen, v.Version)
		}
		if !page.More {
			if pages != 3 {
				t.Errorf("%d pages, want 3", pages)
			}
			break
		}
		if pages == 5 || len(page.Versions) == 0 {
			t.Fatalf("still paging at page %d: %+v", pages, page)
		}
		last := page.Versions[len(page.Versions)-1].Version
		before = &last
	}
	if !slices.Equal(seen, written) {
		t.Errorf("versions %v, want %v", seen, written)
	}
	oldest := written[len(written)-1]
	page, err := c.KV.History("k", &HistoryOptions{BeforeVersion: &oldest})
	if err != nil || len(page.Versions) != 0 || page.More {
		t.Errorf("before the oldest: %+v, %v; want an empty last page", page, err)
	}
}

func TestLiveStreamAppendAndRead(t *testing.T) {
	c := liveClient(t)
	for _, p := range []string{"one", "two", "three"} {
		if _, err := c.Stream.Append("events", []byte(p), nil); err != nil {
			t.Fatal(err)
		}
	}
	res, err := c.Stream.Read("events", nil)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, r := range res.Records {
		got = append(got, string(r.Payload))
	}
	if !slices.Equal(got, []string{"one", "two", "three"}) {
		t.Fatalf("read %q", got)
	}
}

func TestLiveStreamDeleteNeedsForceWhenNotEmpty(t *testing.T) {
	c := liveClient(t)
	if _, err := c.Stream.Append("doomed", []byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Stream.Delete("doomed", nil); !IsConflict(err) {
		t.Fatalf("delete without force: %v, want ErrConflict", err)
	}
	if err := c.Stream.Delete("doomed", &StreamDeleteOptions{Force: true}); err != nil {
		t.Fatalf("delete with force: %v", err)
	}
	res, err := c.Stream.Read("doomed", nil)
	if err != nil || len(res.Records) != 0 {
		t.Fatalf("read after delete: %+v, %v; want no records", res, err)
	}
	if err := c.Stream.Delete("doomed", nil); err != nil {
		t.Fatalf("deleting a missing stream: %v, want success", err)
	}
}

func TestLiveQueueEnqueueDequeueAck(t *testing.T) {
	c := liveClient(t)
	if _, err := c.Queue.Enqueue("jobs", []byte("job-1"), nil); err != nil {
		t.Fatal(err)
	}
	res, err := c.Queue.Dequeue("jobs", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Messages) != 1 || string(res.Messages[0].Payload) != "job-1" {
		t.Fatalf("dequeue = %+v", res.Messages)
	}
	if err := c.Queue.Ack("jobs", []uint64{res.Messages[0].Seq}, nil); err != nil {
		t.Fatal(err)
	}
	again, err := c.Queue.Peek("jobs", 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Messages) != 0 {
		t.Fatalf("acked message still queued: %+v", again.Messages)
	}
}

// A run id the server never minted is not found.
func TestLiveActionStatusOfAnUnknownRunIsNotFound(t *testing.T) {
	c := liveClient(t)
	st, err := c.Action.Status("act-0-0", nil)
	if !IsNotFound(err) {
		t.Fatalf("status = %+v, %v; want not found", st, err)
	}
}

// The worker runs an invoked action with its input, and the run's status
// then says it completed, with the worker's output.
func TestLiveActionWorkerCompletesAnInvokedRun(t *testing.T) {
	c := liveClient(t)
	logger := &captureLogger{}
	w, err := c.NewActionWorker(ActionWorkerOptions{BlockMS: 1000, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	got := make(chan []byte, 1)
	w.MustRegisterAction("echo", func(actx *ActionContext) (interface{}, error) {
		got <- append([]byte(nil), actx.Input()...)
		return []byte("ok"), nil
	}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()
	defer func() {
		w.Stop()
		<-done
	}()

	run, err := c.Action.Invoke("echo", []byte("hi"), nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case input := <-got:
		if !bytes.Equal(input, []byte("hi")) {
			t.Fatalf("handler got %q", input)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the worker never ran the invoked action")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := c.Action.Status(run.RunID, nil)
		if err != nil {
			t.Fatal(err)
		}
		if st.Status == RunStatusCompleted {
			if string(st.Output) != "ok" || st.Attempt != 1 || st.CompletedAt == nil || st.WorkerID != w.config.WorkerID {
				t.Fatalf("completed run: %+v", st)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("run still %v; log: %q", st.Status, logger.all())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A handler that runs past its action's timeout is run once when the worker
// touches it: without AutoTouch the server would take the run back after 1 s
// and hand it out again.
func TestLiveAutoTouchKeepsALongHandlerFromRunningTwice(t *testing.T) {
	c := liveClient(t)
	logger := &captureLogger{}
	w, err := c.NewActionWorker(ActionWorkerOptions{BlockMS: 500, AutoTouch: true, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var runs atomic.Int32
	timeout := uint32(1000)
	w.MustRegisterAction("slow", func(*ActionContext) (interface{}, error) {
		runs.Add(1)
		time.Sleep(2500 * time.Millisecond)
		return []byte("ok"), nil
	}, &ActionRegisterOptions{TimeoutMS: &timeout})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()
	defer func() {
		w.Stop()
		<-done
	}()

	if _, err := c.Action.Invoke("slow", nil, nil); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for !logger.has("Action completed: slow") {
		if time.Now().After(deadline) {
			t.Fatalf("complete not accepted; log: %q", logger.all())
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(1500 * time.Millisecond) // a re-run would have started by now
	if n := runs.Load(); n != 1 {
		t.Fatalf("handler ran %d times, want 1", n)
	}
}

// A drain the server announces mid-run lets the running action complete
// once, then ends Start.
func TestLiveServerDrainLetsTheRunningActionComplete(t *testing.T) {
	c := liveClient(t)
	hb := heartbeatInterval
	heartbeatInterval = 100 * time.Millisecond
	defer func() { heartbeatInterval = hb }()
	logger := &captureLogger{}
	w, err := c.NewActionWorker(ActionWorkerOptions{BlockMS: 500, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	running := make(chan struct{}, 1)
	var runs atomic.Int32
	w.MustRegisterAction("slow", func(actx *ActionContext) (interface{}, error) {
		runs.Add(1)
		running <- struct{}{}
		select {
		case <-time.After(time.Second):
			return []byte("ok"), nil
		case <-actx.Context().Done():
			return nil, actx.Context().Err()
		}
	}, nil)
	done := make(chan error, 1)
	go func() { done <- w.Start(context.Background()) }()

	run, err := c.Action.Invoke("slow", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-running:
	case <-time.After(15 * time.Second):
		t.Fatal("the worker never ran the invoked action")
	}
	if err := c.workerClient(w.config.WorkerID).Drain(nil); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start = %v after a drain; log: %q", err, logger.all())
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("Start did not return after a drain; log: %q", logger.all())
	}
	st, err := c.Action.Status(run.RunID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != RunStatusCompleted || st.Attempt != 1 || runs.Load() != 1 {
		t.Fatalf("run %+v after %d runs, want completed once; log: %q", st, runs.Load(), logger.all())
	}
}

// ListWorkers pages through the namespace's workers in id order.
func TestLiveListWorkersPagesInIDOrder(t *testing.T) {
	c := liveClient(t)
	for _, id := range []string{"w-b", "w-a"} {
		err := c.workerClient(id).Register(&WorkerRegisterOptions{WorkerType: WorkerTypeStream, MaxConcurrency: 4, MachineID: "host-" + id})
		if err != nil {
			t.Fatal(err)
		}
	}
	var got []WorkerInfo
	var cursor []byte
	for pages := 0; ; pages++ {
		if pages == 5 {
			t.Fatalf("still paging after %d pages: %+v", pages, got)
		}
		page, err := c.ListWorkers(&WorkerListOptions{Limit: 1, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, page.Workers...)
		if page.Cursor == nil {
			break
		}
		cursor = page.Cursor
	}
	if len(got) != 2 || got[0].ID != "w-a" || got[1].ID != "w-b" {
		t.Fatalf("listed %+v", got)
	}
	for _, w := range got {
		if w.Type != WorkerTypeStream || w.MaxConcurrency != 4 || w.MachineID != "host-"+w.ID || w.RegisteredAtMS == 0 {
			t.Errorf("row %+v", w)
		}
	}
}

// WorkerInfo shows a worker's row, metadata and processes as registered; a
// missing worker is not found.
func TestLiveWorkerInfoShowsTheRegistration(t *testing.T) {
	c := liveClient(t)
	err := c.workerClient("w-info").Register(&WorkerRegisterOptions{
		WorkerType:     WorkerTypeAction,
		MaxConcurrency: 3,
		Processes:      []ProcessEntry{{Name: "resize", Kind: ProcessKindAction}, {Name: "events/g", Kind: ProcessKindStreamConsumer}},
		Metadata:       `{"zone":"a"}`,
		MachineID:      "host-info",
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.WorkerInfo("w-info", nil)
	if err != nil {
		t.Fatal(err)
	}
	w := got.Worker
	if w.ID != "w-info" || w.Type != WorkerTypeAction || w.MaxConcurrency != 3 || w.MachineID != "host-info" || w.RegisteredAtMS == 0 {
		t.Errorf("row %+v", w)
	}
	if got.Metadata != `{"zone":"a"}` {
		t.Errorf("metadata %q", got.Metadata)
	}
	want := []WorkerProcess{{Name: "resize", Kind: ProcessKindAction}, {Name: "events/g", Kind: ProcessKindStreamConsumer}}
	if !slices.Equal(got.Processes, want) {
		t.Errorf("processes %+v, want %+v", got.Processes, want)
	}
	if _, err := c.WorkerInfo("w-missing", nil); !IsNotFound(err) {
		t.Errorf("missing worker: %v, want not found", err)
	}
}

// The server refuses a registration with more processes than worker.info
// lists; the refusal reaches the caller whole.
func TestLiveRegisteringTooManyProcessesIsRefused(t *testing.T) {
	c := liveClient(t)
	processes := make([]ProcessEntry, 1025)
	for i := range processes {
		processes[i] = ProcessEntry{Name: "p" + strconv.Itoa(i), Kind: ProcessKindAction}
	}
	err := c.workerClient("w-many").Register(&WorkerRegisterOptions{Processes: processes})
	var se *ServerError
	if !errors.As(err, &se) || se.Status != StatusBadRequest || se.Reason != ReasonOutOfRange || se.Ran != RanNo || se.Message != "more than 1024 processes" {
		t.Fatalf("got %#v (%v), want bad_request/out_of_range, ran no, \"more than 1024 processes\"", se, err)
	}
	if _, err := c.WorkerInfo("w-many", nil); !IsNotFound(err) {
		t.Errorf("refused worker is registered: %v", err)
	}
}

// A process name or machine id past the server's bound is refused
// field_too_long.
func TestLiveRegisteringTooLongAFieldIsRefused(t *testing.T) {
	c := liveClient(t)
	for _, tc := range []struct {
		opts WorkerRegisterOptions
		msg  string
	}{
		{WorkerRegisterOptions{Processes: []ProcessEntry{{Name: strings.Repeat("p", 4097), Kind: ProcessKindAction}}}, "process name longer than 4096 bytes"},
		{WorkerRegisterOptions{MachineID: strings.Repeat("m", 256)}, "machine id longer than 255 bytes"},
	} {
		err := c.workerClient("w-long").Register(&tc.opts)
		var se *ServerError
		if !errors.As(err, &se) || se.Status != StatusBadRequest || se.Reason != ReasonFieldTooLong || se.Ran != RanNo || se.Message != tc.msg {
			t.Errorf("got %#v (%v), want bad_request/field_too_long, ran no, %q", se, err, tc.msg)
		}
	}
}

// ListWorkers with an Action lists only the workers that run that action.
func TestLiveListWorkersFiltersByAction(t *testing.T) {
	c := liveClient(t)
	for id, p := range map[string]ProcessEntry{
		"w-resize": {Name: "resize", Kind: ProcessKindAction},
		"w-thumb":  {Name: "thumb", Kind: ProcessKindAction},
		"w-stream": {Name: "resize", Kind: ProcessKindStreamConsumer},
	} {
		if err := c.workerClient(id).Register(&WorkerRegisterOptions{Processes: []ProcessEntry{p}}); err != nil {
			t.Fatal(err)
		}
	}
	page, err := c.ListWorkers(&WorkerListOptions{Action: "resize"})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Workers) != 1 || page.Workers[0].ID != "w-resize" {
		t.Errorf("filtered %+v, want w-resize only", page.Workers)
	}
	all, err := c.ListWorkers(nil)
	if err != nil || len(all.Workers) != 3 {
		t.Errorf("unfiltered %+v, %v; want all three", all, err)
	}
}

// listedWorker is worker id's row, or nil when the server doesn't list it.
func listedWorker(t *testing.T, c *Client, id string) *WorkerInfo {
	t.Helper()
	page, err := c.ListWorkers(nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range page.Workers {
		if page.Workers[i].ID == id {
			return &page.Workers[i]
		}
	}
	return nil
}

// A worker the server removes for missing its heartbeats registers again,
// with the same options, when its next heartbeat answers not_found, and
// still runs work.
func TestLiveAWorkerTheServerRemovedRegistersAgain(t *testing.T) {
	c := liveClientAt(t, liveEnv(t, "FLO_EXPIRY_ENDPOINT"))
	hb := heartbeatInterval
	heartbeatInterval = 3 * time.Second // the server removes it after 1 s
	defer func() { heartbeatInterval = hb }()
	logger := &captureLogger{}
	w, err := c.NewActionWorker(ActionWorkerOptions{BlockMS: 500, Concurrency: 3, MachineID: "host-expiry", Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	w.MustRegisterAction("echo", func(*ActionContext) (interface{}, error) { return []byte("ok"), nil }, nil)
	done := make(chan error, 1)
	go func() { done <- w.Start(context.Background()) }()
	defer func() {
		w.Stop()
		<-done
	}()

	id := w.config.WorkerID
	waitLive := func(what string, cond func(*WorkerInfo) bool) *WorkerInfo {
		t.Helper()
		for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(50 * time.Millisecond) {
			row := listedWorker(t, c, id)
			if cond(row) {
				return row
			}
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for the worker to be %s; log: %q", what, logger.all())
			}
		}
	}
	first := waitLive("registered", func(r *WorkerInfo) bool { return r != nil })
	waitLive("removed", func(r *WorkerInfo) bool { return r == nil })
	again := waitLive("registered again", func(r *WorkerInfo) bool { return r != nil })
	if !logger.has("Worker not found at heartbeat") {
		t.Errorf("registered again without a not_found heartbeat; log: %q", logger.all())
	}
	if again.Type != first.Type || again.MaxConcurrency != 3 || again.MachineID != "host-expiry" ||
		first.MaxConcurrency != 3 || first.MachineID != "host-expiry" || again.RegisteredAtMS <= first.RegisteredAtMS {
		t.Errorf("first registered as %+v, then as %+v", first, again)
	}

	run, err := c.Action.Invoke("echo", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		st, err := c.Action.Status(run.RunID, nil)
		if err != nil {
			t.Fatal(err)
		}
		if st.Status == RunStatusCompleted {
			if st.WorkerID != id {
				t.Errorf("completed by %q, want %q", st.WorkerID, id)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("run still %v; log: %q", st.Status, logger.all())
		}
	}
}
