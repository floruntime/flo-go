package flo

import (
	"fmt"
	"slices"
	"testing"
)

// liveWaitingWorkflow creates a workflow whose run waits for an "approval"
// signal, so a started run stays live until the test ends it.
func liveWaitingWorkflow(t *testing.T, c *Client, name string) {
	t.Helper()
	def := fmt.Sprintf(`kind: Workflow
name: %s
version: 1.0.0
terminals:
  Approved:
    status: completed
  Rejected:
    status: failed
start:
  wait_for_signal:
    type: approval
    timeout_ms: 600000
  transitions:
    success: Approved
    timeout: Rejected
`, name)
	if err := c.Workflow.Create(name, []byte(def), nil); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
}

// liveListRuns pages through ListRuns(name) with a small limit, checking
// each page's bound and that has_more and the cursor agree, and returns
// every run in the order listed.
func liveListRuns(t *testing.T, c *Client, name string) []*WorkflowRunEntry {
	t.Helper()
	var got []*WorkflowRunEntry
	opts := &WorkflowListRunsOptions{Limit: livePageLimit}
	for pages := 1; ; pages++ {
		if pages > 256 {
			t.Fatalf("list runs %q: still paging after %d pages", name, pages)
		}
		page, err := c.Workflow.ListRuns(name, opts)
		if err != nil {
			t.Fatalf("list runs %q page %d (cursor % x): %v", name, pages, opts.Cursor, err)
		}
		if len(page.Runs) > livePageLimit {
			t.Errorf("list runs %q page %d has %d runs, limit %d", name, pages, len(page.Runs), livePageLimit)
		}
		got = append(got, page.Runs...)
		if !page.HasMore {
			return got
		}
		opts.Cursor = page.Cursor
	}
}

// checkRunList checks runs is exactly want (run id -> workflow), each once,
// newest first with ties in run id order, as the server sorts them.
func checkRunList(t *testing.T, what string, runs []*WorkflowRunEntry, want map[string]string) {
	t.Helper()
	seen := map[string]int{}
	for i, r := range runs {
		seen[r.RunID]++
		if w, ok := want[r.RunID]; !ok {
			t.Errorf("%s: listed run %q (%s), not one started", what, r.RunID, r.Workflow)
		} else if r.Workflow != w {
			t.Errorf("%s: run %q listed under %q, started on %q", what, r.RunID, r.Workflow, w)
		}
		if r.Status == "" || r.CreatedAt <= 0 {
			t.Errorf("%s: run %q has status %q created_at %d", what, r.RunID, r.Status, r.CreatedAt)
		}
		if i > 0 {
			p := runs[i-1]
			if p.CreatedAt < r.CreatedAt || (p.CreatedAt == r.CreatedAt && p.RunID >= r.RunID) {
				t.Errorf("%s: %q (%d) listed before %q (%d): not newest first", what, p.RunID, p.CreatedAt, r.RunID, r.CreatedAt)
			}
		}
	}
	for id, n := range seen {
		if n > 1 {
			t.Errorf("%s: run %q listed %d times", what, id, n)
		}
	}
	for id := range want {
		if seen[id] == 0 {
			t.Errorf("%s: run %q never listed", what, id)
		}
	}
}

func TestLiveWorkflowListRunsPagesEveryRunOnceNewestFirst(t *testing.T) {
	c := liveClient(t)
	all := map[string]string{}
	byWorkflow := map[string]map[string]string{}
	for i, n := range []int{7, 5, 4} {
		wf := fmt.Sprintf("lr-wf-%d", i)
		liveWaitingWorkflow(t, c, wf)
		byWorkflow[wf] = map[string]string{}
		for j := 0; j < n; j++ {
			id, err := c.Workflow.Start(wf, []byte("{}"), nil)
			if err != nil {
				t.Fatalf("start %s: %v", wf, err)
			}
			all[id] = wf
			byWorkflow[wf][id] = wf
		}
	}
	checkRunList(t, "every workflow", liveListRuns(t, c, ""), all)
	for wf, want := range byWorkflow {
		checkRunList(t, wf, liveListRuns(t, c, wf), want)
	}
}

// A run started with a chosen id is found by it, and a start after it of the
// same id on another workflow is refused (two sent at once can both run).
func TestLiveWorkflowRunIDChosenByTheClient(t *testing.T) {
	c := liveClient(t)
	liveWaitingWorkflow(t, c, "rid-a")
	liveWaitingWorkflow(t, c, "rid-b")
	for _, id := range []string{"order-123", "run-1", "3f1c9a52-7b4e-4d2a-9c61-0e8b5f7d2a14"} {
		got, err := c.Workflow.Start("rid-a", []byte("{}"), &WorkflowStartOptions{RunID: id})
		if err != nil {
			t.Fatalf("start %q: %v", id, err)
		}
		if got != id {
			t.Fatalf("start %q answered run id %q", id, got)
		}
		st, err := c.Workflow.Status(id, nil)
		if err != nil {
			t.Fatalf("status %q: %v", id, err)
		}
		if st.RunID != id || st.Workflow != "rid-a" {
			t.Errorf("status %q: run %q workflow %q", id, st.RunID, st.Workflow)
		}
		hist, err := c.Workflow.History(id, nil)
		if err != nil {
			t.Fatalf("history %q: %v", id, err)
		}
		if len(hist) < 4 || hist[0]|hist[1]|hist[2]|hist[3] == 0 {
			t.Errorf("history %q: % x, want at least one event", id, hist)
		}
		// A signal the run isn't waiting for is kept, not refused.
		if err := c.Workflow.Signal(id, "nudge", []byte(`{}`), nil); err != nil {
			t.Errorf("signal %q: %v", id, err)
		}
		if _, err := c.Workflow.Start("rid-b", []byte("{}"), &WorkflowStartOptions{RunID: id}); !IsConflict(err) {
			t.Errorf("a later start of %q on another workflow: %v, want ErrConflict", id, err)
		}
		if err := c.Workflow.Cancel(id, nil); err != nil {
			t.Fatalf("cancel %q: %v", id, err)
		}
		st, err = c.Workflow.Status(id, nil)
		if err != nil {
			t.Fatalf("status %q after cancel: %v", id, err)
		}
		if st.Status != "cancelled" {
			t.Errorf("status %q after cancel: %q, want cancelled", id, st.Status)
		}
	}
	runs := liveListRuns(t, c, "")
	ids := make([]string, len(runs))
	for i, r := range runs {
		ids[i] = r.RunID
	}
	slices.Sort(ids)
	if want := []string{"3f1c9a52-7b4e-4d2a-9c61-0e8b5f7d2a14", "order-123", "run-1"}; !slices.Equal(ids, want) {
		t.Errorf("runs listed %q, want %q", ids, want)
	}
}
