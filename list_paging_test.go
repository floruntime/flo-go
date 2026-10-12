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
