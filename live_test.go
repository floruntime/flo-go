package flo

// Live tests against a real flo server: FLO_ENDPOINT names it (CI starts the
// commit pinned in .github/flo-ref.env), and FLO_BINARY the binary, for the
// version check. Without FLO_ENDPOINT they skip, except under CI=1, where a
// missing server is a failure, not a skip.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func liveEndpoint(t *testing.T) string {
	t.Helper()
	endpoint := os.Getenv("FLO_ENDPOINT")
	if endpoint == "" {
		if os.Getenv("CI") == "1" {
			t.Fatal("CI=1 but FLO_ENDPOINT is not set")
		}
		t.Skip("FLO_ENDPOINT is not set")
	}
	return endpoint
}

// liveClient is a connected client in a namespace of its own, so tests on
// one server don't see each other's keys, streams or actions.
func liveClient(t *testing.T) *Client {
	t.Helper()
	endpoint := liveEndpoint(t)
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
			if string(st.Output) != "ok" || st.Attempt != 1 || st.CompletedAt == nil {
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
