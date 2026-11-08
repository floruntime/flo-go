package flo

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestPeekSendsItsLimitInTheBody(t *testing.T) {
	for limit, want := range map[uint32][]byte{0: {0}, 5: {1, 5, 0, 0, 0}} {
		p := capturePayload(t, func(c *Client) { c.Queue.Peek("jobs", limit, nil) })
		if _, key, body := tableBody(t, p); key != "jobs" || !bytes.Equal(body, want) {
			t.Errorf("limit %d: key %q body % x, want jobs and % x", limit, key, body, want)
		}
	}
	if _, err := NewClient("127.0.0.1:1").Queue.Peek("jobs", MaxPeekLimit+1, nil); err == nil {
		t.Error("a limit past MaxPeekLimit was accepted")
	}
}

func TestPeekAnswerIsDecodedWhole(t *testing.T) {
	w := bodyWriter{}
	w.u8(1)
	w.u64(7)
	w.u32(2)
	w.u32(1)
	w.u64(1700)
	w.u32(5000)
	w.bytes16([]byte("head"))
	w.u8(1)
	got, err := decodePeekAnswer(w.buf)
	if err != nil {
		t.Fatal(err)
	}
	want := &PeekResult{Messages: []PeekedMessage{{Seq: 7, Priority: 2, Attempts: 1, EnqueuedAtMS: 1700, Size: 5000, Payload: []byte("head")}}, More: true}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for n := range w.buf {
		if _, err := decodePeekAnswer(w.buf[:n]); !errors.Is(err, ErrIncompleteResponse) {
			t.Fatalf("cut at %d: %v", n, err)
		}
	}
}

// The limit and the seq to start after travel in the value, not as options,
// which the server ignores for this op.
func TestDLQListSendsLimitAndAfterInTheValue(t *testing.T) {
	after := uint64(41)
	for _, tc := range []struct {
		opts *DLQListOptions
		want []byte
	}{
		{nil, []byte{100, 0, 0, 0}},
		{&DLQListOptions{Limit: 10, After: &after}, cat([]byte{10, 0, 0, 0}, u64le(41))},
	} {
		p := capturePayload(t, func(c *Client) { c.Queue.DLQList("jobs", tc.opts) })
		value, opts := splitValueAndOptions(p)
		if !bytes.Equal(value, tc.want) || len(opts) != 0 {
			t.Errorf("%+v: value % x options % x, want % x and none", tc.opts, value, opts, tc.want)
		}
	}
	if _, err := NewClient("127.0.0.1:1").Queue.DLQList("jobs", &DLQListOptions{Limit: MaxDLQPage + 1}); err == nil {
		t.Error("a limit past MaxDLQPage was accepted")
	}
}

func TestDLQPageIsDecodedWhole(t *testing.T) {
	w := bodyWriter{}
	w.u32(1)
	w.u64(9)
	w.u32(3)
	w.bytes32([]byte("abc"))
	w.u64(1700)
	w.u32(4)
	w.u8(2)
	w.u64(1800)
	w.u64(30) // total
	w.u8(1)   // has_more
	w.u64(9)  // last_seq
	got, err := decodeDLQPage(w.buf)
	if err != nil {
		t.Fatal(err)
	}
	want := &DLQPage{Messages: []DeadLetter{{Seq: 9, Size: 3, Payload: []byte("abc"), EnqueuedAtMS: 1700, Attempts: 4, Priority: 2, MovedAtMS: 1800}},
		Total: 30, HasMore: true, LastSeq: 9}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for n := range w.buf {
		if _, err := decodeDLQPage(w.buf[:n]); !errors.Is(err, ErrIncompleteResponse) {
			t.Fatalf("cut at %d: %v", n, err)
		}
	}
}

func TestGroupListAndMembersAreDecodedWhole(t *testing.T) {
	g := bodyWriter{}
	g.u16(1)
	g.bytes16([]byte("workers"))
	g.u32(2)
	g.u64(5)
	g.u64(1700)
	g.u32(30000)
	g.u8(10)
	g.u64(1800)
	g.u64(44)
	g.u8(0)
	groups, err := decodeGroupListAnswer(g.buf)
	if err != nil {
		t.Fatal(err)
	}
	wantGroups := &StreamGroupList{Groups: []StreamGroup{{Name: "workers", Members: 2, Pending: 5, CreatedAtMS: 1700, AckTimeoutMS: 30000, MaxDeliver: 10, LastDeliveredMS: 1800, LastDeliveredSeq: 44}}}
	if !reflect.DeepEqual(groups, wantGroups) {
		t.Fatalf("groups %+v, want %+v", groups, wantGroups)
	}

	m := bodyWriter{}
	m.u16(2)
	m.bytes16([]byte("c1"))
	m.u8(0)
	m.u32(3)
	m.u64(1700)
	m.u8(1)
	m.u64(1900)
	m.bytes16([]byte("c2"))
	m.u8(1)
	m.u32(1)
	m.u64(1600)
	m.u8(0)
	m.u8(1)
	members, err := decodeGroupMembersAnswer(m.buf)
	if err != nil {
		t.Fatal(err)
	}
	active := int64(1900)
	wantMembers := &StreamGroupMembers{Members: []StreamGroupMember{
		{ID: "c1", Pending: 3, JoinedAtMS: 1700, LastActiveMS: &active},
		{ID: "c2", Leaving: true, Pending: 1, JoinedAtMS: 1600},
	}, More: true}
	if !reflect.DeepEqual(members, wantMembers) {
		t.Fatalf("members %+v, want %+v", members, wantMembers)
	}

	for n := range g.buf {
		if _, err := decodeGroupListAnswer(g.buf[:n]); !errors.Is(err, ErrIncompleteResponse) {
			t.Fatalf("groups cut at %d: %v", n, err)
		}
	}
	for n := range m.buf {
		if _, err := decodeGroupMembersAnswer(m.buf[:n]); !errors.Is(err, ErrIncompleteResponse) {
			t.Fatalf("members cut at %d: %v", n, err)
		}
	}
	bad := append([]byte{}, m.buf...)
	bad[6] = 2 // c1's state
	if _, err := decodeGroupMembersAnswer(bad); !errors.Is(err, ErrIncompleteResponse) {
		t.Fatalf("unknown member state: %v", err)
	}
}

func TestGroupMembersSendsTheGroupInTheBody(t *testing.T) {
	p := capturePayload(t, func(c *Client) { c.Stream.GroupMembers("events", "workers", nil) })
	if _, key, body := tableBody(t, p); key != "events" || !bytes.Equal(body, append([]byte{7, 0}, "workers"...)) {
		t.Errorf("key %q body % x", key, body)
	}
}
