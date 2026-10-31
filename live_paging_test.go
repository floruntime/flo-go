package flo

import (
	"fmt"
	"slices"
	"testing"
)

// livePageLimit is small enough that every list below spans several pages,
// and on a sharded server, several shards.
const livePageLimit = 3

// pageAll reads a list page by page until the server says nothing follows,
// and checks it returned exactly want, each name once. A short page is not
// the end: only has_more (or its cursor) says so.
func pageAll(t *testing.T, want []string, page func(cursor []byte) (names []string, more bool, next []byte, err error)) {
	t.Helper()
	seen := map[string]int{}
	var got []string
	var cursor []byte
	pages := 0
	// Each page either returns a name or moves past a shard; this bounds a
	// list that never ends.
	maxPages := 4*len(want) + 64
	for {
		pages++
		if pages > maxPages {
			t.Fatalf("still paging after %d pages; %d names so far", maxPages, len(got))
		}
		names, more, next, err := page(cursor)
		if err != nil {
			t.Fatalf("page %d (cursor % x): %v", pages, cursor, err)
		}
		if len(names) > livePageLimit {
			t.Errorf("page %d has %d names, limit %d", pages, len(names), livePageLimit)
		}
		for _, n := range names {
			seen[n]++
			got = append(got, n)
		}
		if !more {
			if next != nil {
				t.Errorf("last page %d still carries cursor % x", pages, next)
			}
			break
		}
		if len(next) == 0 {
			t.Fatalf("page %d says more follow but has no cursor", pages)
		}
		cursor = next
	}
	for n, c := range seen {
		if c > 1 {
			t.Errorf("%q listed %d times", n, c)
		}
	}
	slices.Sort(got)
	got = slices.Compact(got)
	if !slices.Equal(got, want) {
		t.Errorf("listed %d distinct names %q, want %d %q", len(got), got, len(want), want)
	}
	if min := (len(want) + livePageLimit - 1) / livePageLimit; pages < min {
		t.Errorf("%d pages, want at least %d", pages, min)
	}
}

func liveNames(prefix string, n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("%s%02d", prefix, i)
	}
	slices.Sort(names)
	return names
}

func TestLiveKVScanPagesEveryKeyOnce(t *testing.T) {
	c := liveClient(t)
	want := liveNames("lp-key-", 40)
	for _, k := range want {
		if _, err := c.KV.Put(k, []byte("v"), nil); err != nil {
			t.Fatal(err)
		}
	}
	limit := uint32(livePageLimit)
	pageAll(t, want, func(cursor []byte) ([]string, bool, []byte, error) {
		r, err := c.KV.Scan("", &ScanOptions{Limit: &limit, Cursor: cursor})
		if err != nil {
			return nil, false, nil, err
		}
		// A scan answer has no has_more: its cursor is present only when
		// more follow.
		return r.Keys, r.Cursor != nil, r.Cursor, nil
	})
}

func TestLiveActionListPagesEveryActionOnce(t *testing.T) {
	c := liveClient(t)
	want := liveNames("lp-action-", 30)
	for _, n := range want {
		if err := c.Action.Register(n, ActionTypeUser, nil); err != nil {
			t.Fatal(err)
		}
	}
	pageAll(t, want, func(cursor []byte) ([]string, bool, []byte, error) {
		r, err := c.Action.List(&ActionListOptions{Limit: livePageLimit, Cursor: cursor})
		if err != nil {
			return nil, false, nil, err
		}
		return r.Actions, r.HasMore, r.Cursor, nil
	})
}

func TestLiveWorkflowListDefinitionsPagesEveryDefinitionOnce(t *testing.T) {
	c := liveClient(t)
	want := liveNames("lp-wf-", 30)
	for _, n := range want {
		def := fmt.Sprintf(`kind: Workflow
name: %s
version: 1.0.0
start:
  run: "@actions/validate"
  transitions:
    success: flo.Completed
    failure: flo.Failed
`, n)
		if err := c.Workflow.Create(n, []byte(def), nil); err != nil {
			t.Fatalf("create %s: %v", n, err)
		}
	}
	pageAll(t, want, func(cursor []byte) ([]string, bool, []byte, error) {
		r, err := c.Workflow.ListDefinitions(&WorkflowListDefinitionsOptions{Limit: livePageLimit, Cursor: cursor})
		if err != nil {
			return nil, false, nil, err
		}
		names := make([]string, len(r.Definitions))
		for i, d := range r.Definitions {
			names[i] = d.Name
		}
		return names, r.HasMore, r.Cursor, nil
	})
}

func TestLiveProcessingListPagesEveryJobOnce(t *testing.T) {
	c := liveClient(t)
	want := liveNames("lp-job-", 20)
	for _, n := range want {
		// A time-series sink: outside the default namespace a sharded
		// server refuses a stream or queue sink on another shard than the
		// job's, and this test is about the list, not where a sink lands.
		def := fmt.Sprintf(`kind: Processing
name: %s
sources:
  - stream:
      name: lp-in
sinks:
  - ts:
      measurement: lp_metric
      fields:
        value: value
`, n)
		id, err := c.Processing.Submit([]byte(def), nil)
		if err != nil {
			t.Fatalf("submit %s: %v", n, err)
		}
		t.Cleanup(func() { _ = c.Processing.Cancel(id, nil) })
	}
	pageAll(t, want, func(cursor []byte) ([]string, bool, []byte, error) {
		r, err := c.Processing.List(&ProcessingListOptions{Limit: livePageLimit, Cursor: cursor})
		if err != nil {
			return nil, false, nil, err
		}
		names := make([]string, len(r.Jobs))
		for i, j := range r.Jobs {
			names[i] = j.Name
		}
		return names, r.HasMore, r.Cursor, nil
	})
}
