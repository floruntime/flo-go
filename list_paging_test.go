package flo

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

// Every list op reads its request value as [limit:u32][cursor], with the
// cursor being the opaque bytes the previous page returned. The limit and
// cursor never travel as options.
func TestListRequestsEncodeLimitAndCursorInValue(t *testing.T) {
	cursor := []byte{0x03, 0x00, 'k', 'e', 'y'}
	limit := uint32(25)

	cases := []struct {
		name string
		call func(c *Client)
	}{
		{"processing list", func(c *Client) {
			c.Processing.List(&ProcessingListOptions{Limit: limit, Cursor: cursor})
		}},
		{"workflow list definitions", func(c *Client) {
			c.Workflow.ListDefinitions(&WorkflowListDefinitionsOptions{Limit: limit, Cursor: cursor})
		}},
		{"action list", func(c *Client) {
			c.Action.List(&ActionListOptions{Limit: limit, Cursor: cursor})
		}},
	}
	want := binary.LittleEndian.AppendUint32(nil, limit)
	want = append(want, cursor...)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			value, opts := captureRequest(t, tc.call)
			if !bytes.Equal(value, want) {
				t.Errorf("value = % x, want % x", value, want)
			}
			if len(opts) != 0 {
				t.Errorf("options = % x, want none", opts)
			}
		})
	}
}

// actionListPage codes an action.list answer as the server's walk does:
// each name with an empty value, then has_more and the cursor.
func actionListPage(cursor []byte, names ...string) []byte {
	w := bodyWriter{}
	w.u32(uint32(len(names)))
	for _, n := range names {
		w.bytes16([]byte(n))
		w.u32(0)
	}
	if cursor != nil {
		w.u8(1)
	} else {
		w.u8(0)
	}
	w.bytes16(cursor)
	return w.buf
}

// Action list pages: the first request has no cursor, each next one sends
// back the cursor the last page returned, and the last page has none.
func TestActionListPagesByCursor(t *testing.T) {
	cursor := []byte{0x02, 0x00}
	var values [][]byte
	c := tableServer(t, func(op OpCode, payload []byte) (StatusCode, []byte) {
		if op != OpActionList {
			return StatusBadRequest, refusalBody(ReasonUnknownOp, RanNo, "unexpected op")
		}
		value, _ := splitValueAndOptions(payload)
		values = append(values, value)
		if len(values) == 1 {
			return StatusOK, actionListPage(cursor, "a", "b")
		}
		return StatusOK, actionListPage(nil, "c")
	})
	var names []string
	opts := &ActionListOptions{Limit: 2}
	for {
		page, err := c.Action.List(opts)
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, page.Actions...)
		if !page.HasMore {
			if page.Cursor != nil {
				t.Errorf("last page carries cursor % x", page.Cursor)
			}
			break
		}
		opts.Cursor = page.Cursor
	}
	if strings.Join(names, ",") != "a,b,c" {
		t.Errorf("names = %v", names)
	}
	want := [][]byte{{2, 0, 0, 0}, {2, 0, 0, 0, 0x02, 0x00}}
	if len(values) != 2 || !bytes.Equal(values[0], want[0]) || !bytes.Equal(values[1], want[1]) {
		t.Errorf("request values = % x, want % x", values, want)
	}
}

func TestActionListAnswerIsDecodedWhole(t *testing.T) {
	good := actionListPage([]byte{1, 0}, "a")
	if _, err := decodeActionListAnswer(good); err != nil {
		t.Fatal(err)
	}
	for n := range good {
		if _, err := decodeActionListAnswer(good[:n]); !errors.Is(err, ErrIncompleteResponse) {
			t.Errorf("cut at %d: %v", n, err)
		}
	}
	if _, err := decodeActionListAnswer(append(good, 0)); !errors.Is(err, ErrIncompleteResponse) {
		t.Errorf("trailing byte: %v", err)
	}
}

// The prefix travels as the request key, the value keeping limit and cursor.
func TestActionListSendsThePrefixAsTheKey(t *testing.T) {
	p := capturePayload(t, func(c *Client) {
		c.Action.List(&ActionListOptions{Prefix: "billing.", Limit: 5})
	})
	if _, key, _ := tableBody(t, p); key != "billing." {
		t.Errorf("key = %q, want the prefix", key)
	}
	if value, _ := splitValueAndOptions(p); !bytes.Equal(value, []byte{5, 0, 0, 0}) {
		t.Errorf("value = % x, want the limit alone", value)
	}
}

// pageTrailer codes a list page's tail: has_more set exactly when a cursor
// follows.
func pageTrailer(w *bodyWriter, cursor []byte) {
	if cursor != nil {
		w.u8(1)
	} else {
		w.u8(0)
	}
	w.bytes16(cursor)
}

// processingListPage codes a processing.list answer as the server's
// encodeJobList does.
func processingListPage(cursor []byte, jobs ...ProcessingListEntry) []byte {
	w := bodyWriter{}
	w.u32(uint32(len(jobs)))
	for _, j := range jobs {
		w.bytes16([]byte(j.Name))
		w.bytes16([]byte(j.JobID))
		w.bytes16([]byte(j.Status))
		w.u64(uint64(j.CreatedAt))
	}
	pageTrailer(&w, cursor)
	return w.buf
}

// definitionListPage codes a workflow.list_definitions answer.
func definitionListPage(cursor []byte, defs ...WorkflowDefinitionEntry) []byte {
	w := bodyWriter{}
	w.u32(uint32(len(defs)))
	for _, d := range defs {
		w.bytes16([]byte(d.Name))
		w.bytes16([]byte(d.Version))
		w.u64(uint64(d.CreatedAt))
	}
	pageTrailer(&w, cursor)
	return w.buf
}

// twoPageServer answers op with first, then last, recording each request
// value.
func twoPageServer(t *testing.T, want OpCode, first, last []byte) (*Client, *[][]byte) {
	var values [][]byte
	c := tableServer(t, func(op OpCode, payload []byte) (StatusCode, []byte) {
		if op != want {
			return StatusBadRequest, refusalBody(ReasonUnknownOp, RanNo, "unexpected op")
		}
		value, _ := splitValueAndOptions(payload)
		values = append(values, value)
		if len(values) == 1 {
			return StatusOK, first
		}
		return StatusOK, last
	})
	return c, &values
}

// The server merges at most a few shards per page, so a page can be short
// while more follow. Paging must continue on has_more, not on a short page,
// and send back the cursor it was given.
func TestProcessingListPagesByCursor(t *testing.T) {
	cursor := []byte{0x03, 0x00, 'j', '1'}
	c, values := twoPageServer(t, OpProcessingList,
		processingListPage(cursor, ProcessingListEntry{"a", "j1", "running", 1}),
		processingListPage(nil, ProcessingListEntry{"b", "j2", "stopped", 2}, ProcessingListEntry{"c", "j3", "failed", 3}))

	var ids []string
	opts := &ProcessingListOptions{Limit: 10}
	for {
		page, err := c.Processing.List(opts)
		if err != nil {
			t.Fatal(err)
		}
		for _, j := range page.Jobs {
			ids = append(ids, j.JobID)
		}
		if !page.HasMore {
			if page.Cursor != nil {
				t.Errorf("last page carries cursor % x", page.Cursor)
			}
			break
		}
		opts.Cursor = page.Cursor
	}
	if strings.Join(ids, ",") != "j1,j2,j3" {
		t.Errorf("job ids = %v", ids)
	}
	want := [][]byte{{10, 0, 0, 0}, append([]byte{10, 0, 0, 0}, cursor...)}
	if len(*values) != 2 || !bytes.Equal((*values)[0], want[0]) || !bytes.Equal((*values)[1], want[1]) {
		t.Errorf("request values = % x, want % x", *values, want)
	}
}

func TestWorkflowListDefinitionsPagesByCursor(t *testing.T) {
	cursor := []byte{0x07, 0x00, 'o', 'r', 'd'}
	c, values := twoPageServer(t, OpWorkflowListDefinitions,
		definitionListPage(cursor, WorkflowDefinitionEntry{"orders", "1", 10}),
		definitionListPage(nil, WorkflowDefinitionEntry{"refunds", "2", 20}, WorkflowDefinitionEntry{"ship", "3", 30}))

	var names []string
	opts := &WorkflowListDefinitionsOptions{Limit: 10}
	for {
		page, err := c.Workflow.ListDefinitions(opts)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range page.Definitions {
			names = append(names, d.Name)
		}
		if !page.HasMore {
			if page.Cursor != nil {
				t.Errorf("last page carries cursor % x", page.Cursor)
			}
			break
		}
		opts.Cursor = page.Cursor
	}
	if strings.Join(names, ",") != "orders,refunds,ship" {
		t.Errorf("names = %v", names)
	}
	want := [][]byte{{10, 0, 0, 0}, append([]byte{10, 0, 0, 0}, cursor...)}
	if len(*values) != 2 || !bytes.Equal((*values)[0], want[0]) || !bytes.Equal((*values)[1], want[1]) {
		t.Errorf("request values = % x, want % x", *values, want)
	}
}

// One definitions page, laid out byte by byte as the server sends it.
func TestParseWorkflowDefinitionList(t *testing.T) {
	b := []byte{
		2, 0, 0, 0, // count
		6, 0, 'o', 'r', 'd', 'e', 'r', 's', 1, 0, '1', 0x10, 0, 0, 0, 0, 0, 0, 0,
		4, 0, 's', 'h', 'i', 'p', 3, 0, '2', '.', '1', 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		1,               // has_more
		3, 0, 9, 0, 'x', // cursor
	}
	got, err := parseWorkflowDefinitionList(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Definitions) != 2 ||
		*got.Definitions[0] != (WorkflowDefinitionEntry{"orders", "1", 0x10}) ||
		*got.Definitions[1] != (WorkflowDefinitionEntry{"ship", "2.1", -1}) {
		t.Errorf("definitions = %+v %+v", got.Definitions[0], got.Definitions[1])
	}
	if !got.HasMore || !bytes.Equal(got.Cursor, []byte{9, 0, 'x'}) {
		t.Errorf("has_more %v cursor % x", got.HasMore, got.Cursor)
	}
	for n := range b {
		if _, err := parseWorkflowDefinitionList(b[:n]); !errors.Is(err, ErrIncompleteResponse) {
			t.Errorf("cut at %d: %v", n, err)
		}
	}
	if _, err := parseWorkflowDefinitionList(append(b, 0)); !errors.Is(err, ErrIncompleteResponse) {
		t.Errorf("trailing byte: %v", err)
	}
}
