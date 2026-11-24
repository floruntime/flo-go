package flo

import (
	"bytes"
	"errors"
	"testing"
)

// historyPage is a history answer as the server encodes it.
func historyPage(events []RunHistoryEvent, hasMore bool, cursor *uint64) []byte {
	var w bodyWriter
	w.u16(uint16(len(events)))
	for _, e := range events {
		w.bytes32([]byte(e.Type))
		w.bytes32(e.Detail)
		w.bytes16([]byte(e.Extra))
		w.u64(uint64(e.TimestampMS))
	}
	if hasMore {
		w.u8(1)
	} else {
		w.u8(0)
	}
	w.optU64(cursor)
	return w.buf
}

// A field left out is sent absent, so the server's default applies; one
// given is sent present.
func TestRunHistoryRequestSendsOnlyTheFieldsGiven(t *testing.T) {
	cursor := uint64(42)
	for _, c := range []struct {
		name string
		opts *RunHistoryOptions
		want []byte
	}{
		{"nil", nil, []byte{0, 0, 0}},
		{"defaults", &RunHistoryOptions{}, []byte{0, 0, 0}},
		{"limit", &RunHistoryOptions{Limit: 5}, cat([]byte{1, 5, 0, 0, 0}, []byte{0, 0})},
		{"cursor", &RunHistoryOptions{Cursor: &cursor}, cat([]byte{0, 1}, u64le(42), []byte{0})},
		{"newest first", &RunHistoryOptions{NewestFirst: true}, []byte{0, 0, 1, 1}},
		{"all", &RunHistoryOptions{Limit: 1000, Cursor: &cursor, NewestFirst: true}, cat([]byte{1, 0xe8, 3, 0, 0, 1}, u64le(42), []byte{1, 1})},
	} {
		for _, op := range []OpCode{OpWorkflowHistory, OpActionHistory} {
			var gotOp OpCode
			var gotKey string
			var gotBody []byte
			c2 := tableServer(t, func(o OpCode, p []byte) (StatusCode, []byte) {
				gotOp = o
				_, gotKey, gotBody = tableBody(t, p)
				return StatusOK, historyPage(nil, false, nil)
			})
			var err error
			if op == OpWorkflowHistory {
				_, err = c2.Workflow.History("run-1", c.opts)
			} else {
				_, err = c2.Action.History("run-1", c.opts)
			}
			if err != nil || gotOp != op || gotKey != "run-1" || !bytes.Equal(gotBody, c.want) {
				t.Errorf("%s op 0x%x: sent op 0x%x key %q body %x, %v; want body %x", c.name, op, gotOp, gotKey, gotBody, err, c.want)
			}
		}
	}
	if !isTableOp(OpWorkflowHistory) || !isTableOp(OpActionHistory) {
		t.Fatal("workflow.history and action.history are table ops")
	}
}

func TestRunHistoryPageDecodes(t *testing.T) {
	page, err := decodeRunHistoryPage(historyPage(nil, false, nil))
	if err != nil || len(page.Events) != 0 || page.HasMore || page.Cursor != nil {
		t.Fatalf("empty page: %+v, %v", page, err)
	}

	// A detail past a u16 length: event_type and detail are u32-length.
	big := bytes.Repeat([]byte("d"), 65536)
	want := []RunHistoryEvent{
		{Type: "action_invoked", Detail: []byte("ping"), TimestampMS: 1000},
		{Type: "action_delivered", Detail: []byte("w1"), Extra: "truncated", TimestampMS: 1001},
		{Type: "action_settled", Detail: big, Extra: "paid", TimestampMS: -1},
	}
	cursor := uint64(3)
	page, err = decodeRunHistoryPage(historyPage(want, true, &cursor))
	if err != nil || !page.HasMore || page.Cursor == nil || *page.Cursor != 3 || len(page.Events) != len(want) {
		t.Fatalf("page: %+v, %v", page, err)
	}
	for i, e := range page.Events {
		if e.Type != want[i].Type || !bytes.Equal(e.Detail, want[i].Detail) || e.Extra != want[i].Extra || e.TimestampMS != want[i].TimestampMS {
			t.Errorf("event %d: %+v, want %+v", i, e, want[i])
		}
	}
}

// A cut answer, a byte past it, a cursor without has_more (or the other
// way) and a count past a page's are refused.
func TestRunHistoryPageRefusesABadAnswer(t *testing.T) {
	cursor := uint64(9)
	whole := historyPage([]RunHistoryEvent{{Type: "workflow_started", Detail: []byte("{}"), TimestampMS: 5}}, true, &cursor)
	for n := range whole {
		if _, err := decodeRunHistoryPage(whole[:n]); !errors.Is(err, ErrIncompleteResponse) {
			t.Fatalf("cut at %d: %v", n, err)
		}
	}
	for name, b := range map[string][]byte{
		"a byte past it":      append(append([]byte{}, whole...), 0),
		"more without cursor": historyPage(nil, true, nil),
		"cursor without more": historyPage(nil, false, &cursor),
		"count past a page":   cat([]byte{0xe9, 3}, make([]byte, 1001*18), []byte{0, 0}),
	} {
		if _, err := decodeRunHistoryPage(b); !errors.Is(err, ErrIncompleteResponse) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestRunHistoryOfAnUnknownRunIsNotFound(t *testing.T) {
	c := tableServer(t, func(OpCode, []byte) (StatusCode, []byte) {
		return StatusNotFound, refusalBody(ReasonNotFound, RanNo, "not found: no such run")
	})
	if _, err := c.Action.History("nope", nil); !IsNotFound(err) {
		t.Fatalf("action history: %v, want not found", err)
	}
}
