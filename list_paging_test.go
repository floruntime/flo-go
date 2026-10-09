package flo

import (
	"bytes"
	"encoding/binary"
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
		{"kv scan", func(c *Client) {
			c.KV.Scan("p", &ScanOptions{Limit: &limit, Cursor: cursor})
		}},
		{"processing list", func(c *Client) {
			c.Processing.List(&ProcessingListOptions{Limit: limit, Cursor: cursor})
		}},
		{"workflow list definitions", func(c *Client) {
			c.Workflow.ListDefinitions(&WorkflowListDefinitionsOptions{Limit: limit, Cursor: cursor})
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

// A scan answer ends [has_more:u8][cursor_len:u16][cursor]; the cursor must
// come back to the caller intact so the next page can be requested.
func TestScanReturnsTrailingCursor(t *testing.T) {
	answer := binary.LittleEndian.AppendUint32(nil, 1)
	answer = binary.LittleEndian.AppendUint16(answer, 1)
	answer = append(answer, 'a')
	answer = binary.LittleEndian.AppendUint32(answer, 1)
	answer = append(answer, 'v')
	answer = append(answer, 1)
	answer = binary.LittleEndian.AppendUint16(answer, 3)
	answer = append(answer, 'n', 'x', 't')

	addr := fakeServer(t, func(OpCode) []byte { return answer })
	c := NewClient(addr)
	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	res, err := c.KV.Scan("", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Entries) != 1 || string(res.Entries[0].Key) != "a" || string(res.Entries[0].Value) != "v" {
		t.Errorf("entries = %+v", res.Entries)
	}
	if !res.HasMore || string(res.Cursor) != "nxt" {
		t.Errorf("HasMore = %v, Cursor = %q; want true, \"nxt\"", res.HasMore, res.Cursor)
	}
}
