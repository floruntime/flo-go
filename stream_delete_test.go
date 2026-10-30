package flo

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
	"time"
)

// Delete sends stream_delete with the stream as the key, an empty value, and
// the force flag (tag 0x2C, length 0) only when Force is set.
func TestDeleteSendsStreamAndForceFlag(t *testing.T) {
	cases := []struct {
		name string
		opts *StreamDeleteOptions
		want []byte
	}{
		{"default", nil, nil},
		{"no force", &StreamDeleteOptions{}, nil},
		{"force", &StreamDeleteOptions{Force: true}, []byte{0x2C, 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := capturePayload(t, func(c *Client) {
				_ = c.Stream.Delete("orders", tc.opts)
			})
			off := 2 + int(binary.LittleEndian.Uint16(p))
			klen := int(binary.LittleEndian.Uint16(p[off:]))
			if key := string(p[off+2 : off+2+klen]); key != "orders" {
				t.Fatalf("key = %q, want orders", key)
			}
			value, opts := splitValueAndOptions(p)
			if len(value) != 0 {
				t.Fatalf("value = %x, want empty", value)
			}
			if !bytes.Equal(opts, tc.want) {
				t.Fatalf("options = %x, want %x", opts, tc.want)
			}
		})
	}
	if !bytes.Equal(NewOptionsBuilder().AddFlag(OptForce).Build(), []byte{0x2C, 0}) {
		t.Fatal("OptForce is not the server's force tag 0x2C")
	}
}

func TestDeleteSendsTheStreamDeleteOp(t *testing.T) {
	ops := make(chan OpCode, 1)
	addr := fakeServer(t, func(op OpCode) []byte {
		ops <- op
		return nil
	})
	c := NewClient(addr, WithTimeout(time.Second))
	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Stream.Delete("orders", nil); err != nil {
		t.Fatal(err)
	}
	if op := <-ops; op != 0x140 {
		t.Fatalf("op = %#x, want stream_delete 0x140", uint16(op))
	}
}

// A non-empty stream deleted without Force comes back as conflict.
func TestDeleteRefusalIsConflict(t *testing.T) {
	const msg = "stream is not empty; pass --force to delete"
	ln := serveAll(t, func(id uint64) []byte { return statusReply(id, StatusConflict, msg) })
	c := NewClient(ln.Addr().String(), WithTimeout(time.Second), WithRetry(RetryPolicy{MaxAttempts: 1}))
	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	err := c.Stream.Delete("orders", nil)
	var se *ServerError
	if !IsConflict(err) || !errors.As(err, &se) || se.Message != msg {
		t.Fatalf("got %v, want ErrConflict with the server's message", err)
	}
}
