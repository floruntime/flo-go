package flo

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

// capturePayload runs call against a fake server that reads one request and
// hangs up, and returns that request's payload.
func capturePayload(t *testing.T, call func(c *Client)) []byte {
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
	return p
}

// captureRequest is capturePayload for an op still coded by hand: the
// request's value and options fields.
func captureRequest(t *testing.T, call func(c *Client)) (value, options []byte) {
	t.Helper()
	p := capturePayload(t, call)
	off := 2 + int(binary.LittleEndian.Uint16(p)) // namespace
	off += 2 + int(binary.LittleEndian.Uint16(p[off:]))
	vlen := int(binary.LittleEndian.Uint32(p[off:]))
	value = p[off+4 : off+4+vlen]
	off += 4 + vlen
	olen := int(binary.LittleEndian.Uint16(p[off:]))
	return value, p[off+2 : off+2+olen]
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
