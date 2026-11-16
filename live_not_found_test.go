package flo

import (
	"testing"
	"time"
)

// A non-blocking read of a stream never written is not found; one past an
// existing stream's end is ok and empty; a blocking read of a missing
// stream waits for its first append rather than failing.
func TestLiveStreamReadOfAMissingStreamIsNotFound(t *testing.T) {
	c := liveClient(t)
	// The append also creates the namespace: a read in a namespace never
	// created is no_namespace, not a missing stream.
	a, err := c.Stream.Append("written", []byte("one"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := c.Stream.Read("never-written", nil); !IsNotFound(err) || IsNoNamespace(err) {
		t.Fatalf("read of a missing stream: %+v, %v; want not found", got, err)
	}
	got, err := c.Stream.Read("written", &StreamReadOptions{Start: &a.ID})
	if err != nil || len(got.Records) != 0 {
		t.Fatalf("read past the end: %+v, %v; want ok and empty", got, err)
	}

	block := uint32(300)
	start := time.Now()
	got, err = c.Stream.Read("not-yet", &StreamReadOptions{BlockMS: &block})
	if err != nil || len(got.Records) != 0 {
		t.Fatalf("blocking read of a missing stream: %+v, %v; want it to wait, then ok and empty", got, err)
	}
	if d := time.Since(start); d < 200*time.Millisecond {
		t.Errorf("the blocking read answered after %v; it didn't wait", d)
	}
}

// A ts.read of a measurement never written is not found, and so is a walk
// of it; a range with no points on a real series is ok and empty.
func TestLiveTSReadOfAMissingSeriesIsNotFound(t *testing.T) {
	c := liveClient(t)
	// The write also creates the namespace.
	const at = int64(1_700_000_000_000_000_000)
	if _, err := c.TS.Write("real", 1, &TSWriteOptions{Tags: "host=a", TimeNS: at}); err != nil {
		t.Fatal(err)
	}
	if got, err := c.TS.Read("never-written", nil); !IsNotFound(err) || IsNoNamespace(err) {
		t.Fatalf("read of a missing measurement: %+v, %v; want not found", got, err)
	}
	it := c.TS.ReadAll("never-written", nil)
	if it.Next() || !IsNotFound(it.Err()) {
		t.Fatalf("walk of a missing measurement: %v; want not found", it.Err())
	}

	from, to := at+1, at+1000
	got, err := c.TS.Read("real", &TSReadOptions{FromNS: &from, ToNS: &to})
	if err != nil || len(got.Points) != 0 || got.Cursor != nil {
		t.Fatalf("a range with no points: %+v, %v; want ok and empty", got, err)
	}
	if _, err := c.TS.Read("real", &TSReadOptions{Tags: "host=b"}); !IsNotFound(err) {
		t.Errorf("a tag filter no series matches: %v; want not found", err)
	}
}
