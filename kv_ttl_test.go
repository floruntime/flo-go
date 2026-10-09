package flo

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

// captureRequest runs call against a fake server that reads one request and
// hangs up, and returns that request's value and options fields.
func captureRequest(t *testing.T, call func(c *Client)) (value, options []byte) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	got := make(chan []byte, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			got <- nil
			return
		}
		defer conn.Close()
		header := make([]byte, HeaderSize)
		if _, err := io.ReadFull(conn, header); err != nil {
			got <- nil
			return
		}
		payload := make([]byte, binary.LittleEndian.Uint32(header[4:8]))
		if _, err := io.ReadFull(conn, payload); err != nil {
			got <- nil
			return
		}
		got <- payload
	}()
	c := NewClient(ln.Addr().String(), WithTimeout(time.Second))
	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	call(c)
	p := <-got
	if p == nil {
		t.Fatal("fake server read no request")
	}
	off := 2 + int(binary.LittleEndian.Uint16(p)) // namespace
	off += 2 + int(binary.LittleEndian.Uint16(p[off:]))
	vlen := int(binary.LittleEndian.Uint32(p[off:]))
	value = p[off+4 : off+4+vlen]
	off += 4 + vlen
	olen := int(binary.LittleEndian.Uint16(p[off:]))
	return value, p[off+2 : off+2+olen]
}

// The server reads the KV TTL option as an 8-byte millisecond count and
// refuses any other width.
func TestPutEncodesTTLMs(t *testing.T) {
	const ttl = uint64(90_500)
	_, opts := captureRequest(t, func(c *Client) {
		v := ttl
		c.KV.Put("k", []byte("v"), &PutOptions{TTLMs: &v})
	})
	want := []byte{byte(OptTTLMs), 8}
	want = binary.LittleEndian.AppendUint64(want, ttl)
	if !bytes.HasPrefix(opts, want) {
		t.Errorf("Put options = % x, want prefix % x", opts, want)
	}
}

func TestTouchEncodesMs(t *testing.T) {
	for _, ttl := range []uint64{0, 1_500} {
		value, _ := captureRequest(t, func(c *Client) {
			c.KV.Touch("k", ttl, nil)
		})
		if len(value) != 8 || binary.LittleEndian.Uint64(value) != ttl {
			t.Errorf("Touch(%d) value = % x, want 8-byte %d", ttl, value, ttl)
		}
	}
}
