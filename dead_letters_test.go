package flo

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

// noClient is a client whose every request fails to connect: a check that
// refuses before the round trip never gets that far.
func noClient() *Client { return NewClient("127.0.0.1:1") }

func TestDLQListSendsLimitAndCursorInTheBody(t *testing.T) {
	cursor := &DLQCursor{deadNS: 5_000_000, seq: 41}
	for _, tc := range []struct {
		opts *DLQListOptions
		want []byte
	}{
		{nil, []byte{0, 0}},
		{&DLQListOptions{Limit: 10, After: cursor}, cat([]byte{1, 10, 0, 0, 0, 1}, u64le(5_000_000), u64le(41))},
	} {
		p := capturePayload(t, func(c *Client) { c.Queue.DLQList("jobs", tc.opts) })
		if _, key, body := tableBody(t, p); key != "jobs" || !bytes.Equal(body, tc.want) {
			t.Errorf("%+v: key %q body % x, want jobs and % x", tc.opts, key, body, tc.want)
		}
	}
	if _, err := noClient().Queue.DLQList("jobs", &DLQListOptions{Limit: MaxDLQPage + 1}); err == nil || IsConnectionError(err) {
		t.Errorf("a limit past MaxDLQPage: %v, want refused before sending", err)
	}
}

func TestDLQPageIsDecodedWhole(t *testing.T) {
	w := bodyWriter{}
	w.u8(1)
	w.u64(9)    // seq
	w.u32(2)    // priority
	w.u32(4)    // deliveries
	w.u32(3)    // failures
	w.u64(1700) // enqueued_at_ms
	w.u64(1800) // dead_at_ms
	w.u32(5000) // size
	w.bytes16([]byte("abc"))
	w.bytes16([]byte("boom"))
	w.u8(1) // a cursor
	w.u64(1_800_000_000)
	w.u64(9)
	got, err := decodeDLQPage(w.buf)
	if err != nil {
		t.Fatal(err)
	}
	want := &DLQPage{Messages: []DeadLetter{{Seq: 9, Priority: 2, Deliveries: 4, Failures: 3, EnqueuedAtMS: 1700, DeadAtMS: 1800, Size: 5000, Payload: []byte("abc"), Error: "boom"}},
		Cursor: &DLQCursor{deadNS: 1_800_000_000, seq: 9}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for n := range w.buf {
		if _, err := decodeDLQPage(w.buf[:n]); !errors.Is(err, ErrIncompleteResponse) {
			t.Fatalf("cut at %d: %v", n, err)
		}
	}
	last, err := decodeDLQPage([]byte{0, 0})
	if err != nil || len(last.Messages) != 0 || last.Cursor != nil {
		t.Fatalf("an empty last page: %+v, %v", last, err)
	}
}

func TestDLQRequeueAndDeleteSendTheirBodies(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(c *Client)
		want []byte
	}{
		{"requeue named", func(c *Client) { c.Queue.DLQRequeue("jobs", []uint64{4, 7}, nil) },
			cat([]byte{1, 2}, u64le(4), u64le(7), []byte{1, 0, 0})},
		{"requeue all", func(c *Client) { c.Queue.DLQRequeue("jobs", nil, &DLQRequeueOptions{All: true, Limit: 50}) },
			[]byte{0, 1, 1, 1, 50, 0, 0, 0}},
		{"delete named", func(c *Client) { c.Queue.DLQDelete("jobs", []uint64{4}, nil) },
			cat([]byte{1, 1}, u64le(4), []byte{1, 0, 0, 1, 0})},
		{"delete all dry", func(c *Client) { c.Queue.DLQDelete("jobs", nil, &DLQDeleteOptions{All: true, DryRun: true}) },
			[]byte{0, 1, 1, 0, 1, 1}},
	} {
		p := capturePayload(t, tc.call)
		if _, key, body := tableBody(t, p); key != "jobs" || !bytes.Equal(body, tc.want) {
			t.Errorf("%s: key %q body % x, want jobs and % x", tc.name, key, body, tc.want)
		}
	}
	q := noClient().Queue
	many := make([]uint64, MaxDLQPage+1)
	for name, err := range map[string]error{
		"neither":  func() error { _, err := q.DLQRequeue("jobs", nil, nil); return err }(),
		"both":     func() error { _, err := q.DLQDelete("jobs", []uint64{1}, &DLQDeleteOptions{All: true}); return err }(),
		"too many": func() error { _, err := q.DLQRequeue("jobs", many, nil); return err }(),
		"big limit": func() error {
			_, err := q.DLQRequeue("jobs", nil, &DLQRequeueOptions{All: true, Limit: MaxDLQPage + 1})
			return err
		}(),
	} {
		if err == nil || IsConnectionError(err) {
			t.Errorf("%s: %v, want refused before sending", name, err)
		}
	}
}

func TestDLQReportIsDecodedWhole(t *testing.T) {
	w := bodyWriter{}
	w.u32(1)
	w.u8(2)
	w.u64(4)
	w.u8(uint8(QueueNotDead))
	w.bytes8([]byte("not dead: only a dead letter is requeued or deleted"))
	w.u64(5)
	w.u8(uint8(QueueGone))
	w.bytes8([]byte("not found: no such message in this queue"))
	w.u8(1)
	got, err := decodeDLQReport(w.buf)
	if err != nil {
		t.Fatal(err)
	}
	want := &DLQReport{Applied: 1, More: true, Refused: []QueueRefusal{
		{Seq: 4, Why: QueueNotDead, Reason: "not dead: only a dead letter is requeued or deleted"},
		{Seq: 5, Why: QueueGone, Reason: "not found: no such message in this queue"}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for n := range w.buf {
		if _, err := decodeDLQReport(w.buf[:n]); !errors.Is(err, ErrIncompleteResponse) {
			t.Fatalf("cut at %d: %v", n, err)
		}
	}
}

// groupBody is how a group op's body starts: the group's u16 length and
// name.
func groupBody(group string) []byte { return append([]byte{byte(len(group)), 0}, group...) }

func TestGroupStealSendsItsBody(t *testing.T) {
	lease := uint64(60000)
	items := []StreamGroupItem{{ID: StreamID{TimestampMS: 5, Sequence: 6}, Deliveries: 2}}
	p := capturePayload(t, func(c *Client) {
		c.Stream.GroupSteal("s", "g", "rescue", 30000, items, &StreamGroupStealOptions{LeaseMS: &lease})
	})
	want := cat(groupBody("g"), []byte{6, 0}, []byte("rescue"), u64le(30000), []byte{1}, u64le(60000),
		[]byte{1}, u64le(5), u64le(6), []byte{2, 0, 0, 0})
	if _, key, body := tableBody(t, p); key != "s" || !bytes.Equal(body, want) {
		t.Errorf("key %q body % x, want s and % x", key, body, want)
	}
	s := noClient().Stream
	short, long := uint64(MinGroupAckTimeoutMS-1), uint64(MaxGroupAckTimeoutMS+1)
	for name, err := range map[string]error{
		"no consumer": func() error { _, err := s.GroupSteal("s", "g", "", 1, items, nil); return err }(),
		"idle 0":      func() error { _, err := s.GroupSteal("s", "g", "c", 0, items, nil); return err }(),
		"idle > 12h":  func() error { _, err := s.GroupSteal("s", "g", "c", MaxGroupAckTimeoutMS+1, items, nil); return err }(),
		"short lease": func() error {
			_, err := s.GroupSteal("s", "g", "c", 1, items, &StreamGroupStealOptions{LeaseMS: &short})
			return err
		}(),
		"long lease": func() error {
			_, err := s.GroupSteal("s", "g", "c", 1, items, &StreamGroupStealOptions{LeaseMS: &long})
			return err
		}(),
		"no records": func() error { _, err := s.GroupSteal("s", "g", "c", 1, nil, nil); return err }(),
	} {
		if err == nil || IsConnectionError(err) {
			t.Errorf("%s: %v, want refused before sending", name, err)
		}
	}
}

func TestGroupStealAnswerIsDecodedWhole(t *testing.T) {
	w := bodyWriter{}
	w.u64(3)    // term
	w.u64(90)   // index
	w.u64(9000) // expires_at_ms
	w.u8(1)
	w.u64(5)
	w.u64(6)
	w.u32(3)
	w.u8(1)
	w.u64(5)
	w.u64(7)
	w.u8(uint8(QueueNotIdle))
	w.bytes8([]byte("not idle: its holder moved its lease within the idle time named"))
	c := tableServer(t, func(OpCode, []byte) (StatusCode, []byte) { return StatusOK, w.buf })
	got, err := c.Stream.GroupSteal("s", "g", "rescue", 1, []StreamGroupItem{{ID: StreamID{5, 6}, Deliveries: 2}, {ID: StreamID{5, 7}, Deliveries: 1}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := &StreamGroupStealResult{
		Stolen:      []StreamGroupItem{{ID: StreamID{5, 6}, Deliveries: 3}},
		Lease:       StreamGroupLease{Consumer: "rescue", Term: 3, Index: 90},
		ExpiresAtMS: 9000,
		Refused:     []StreamGroupRefusal{{ID: StreamID{5, 7}, Why: QueueNotIdle, Reason: "not idle: its holder moved its lease within the idle time named"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for n := range w.buf {
		if _, err := decodeStealAnswer(w.buf[:n]); !errors.Is(err, ErrIncompleteResponse) {
			t.Fatalf("cut at %d: %v", n, err)
		}
	}
	bad := append([]byte{}, w.buf...)
	bad[24+1+20+1+16] = 8 // a why past not_idle
	if _, err := decodeStealAnswer(bad); !errors.Is(err, ErrIncompleteResponse) {
		t.Fatalf("unknown why: %v", err)
	}
}

func TestGroupDLQOpsSendTheirBodies(t *testing.T) {
	cursor := &StreamGroupDLQCursor{deadNS: 7, id: StreamID{5, 6}}
	for _, tc := range []struct {
		name string
		call func(c *Client)
		want []byte
	}{
		{"list", func(c *Client) { c.Stream.GroupDLQList("s", "g", nil) }, cat(groupBody("g"), []byte{0, 0})},
		{"list after", func(c *Client) { c.Stream.GroupDLQList("s", "g", &StreamGroupDLQListOptions{Limit: 3, After: cursor}) },
			cat(groupBody("g"), []byte{1, 3, 0, 0, 0, 1}, u64le(7), u64le(5), u64le(6))},
		{"requeue named", func(c *Client) { c.Stream.GroupDLQRequeue("s", "g", []StreamID{{5, 6}}, nil) },
			cat(groupBody("g"), []byte{1, 1}, u64le(5), u64le(6), []byte{1, 0, 0})},
		{"requeue all", func(c *Client) { c.Stream.GroupDLQRequeue("s", "g", nil, &StreamGroupDLQOptions{All: true, Limit: 9}) },
			cat(groupBody("g"), []byte{0, 1, 1, 1, 9, 0, 0, 0})},
		{"delete dry", func(c *Client) {
			c.Stream.GroupDLQDelete("s", "g", []StreamID{{5, 6}}, &StreamGroupDLQDeleteOptions{DryRun: true})
		}, cat(groupBody("g"), []byte{1, 1}, u64le(5), u64le(6), []byte{1, 0, 0, 1, 1})},
	} {
		p := capturePayload(t, tc.call)
		if _, key, body := tableBody(t, p); key != "s" || !bytes.Equal(body, tc.want) {
			t.Errorf("%s: key %q body % x, want s and % x", tc.name, key, body, tc.want)
		}
	}
	s := noClient().Stream
	for name, err := range map[string]error{
		"neither": func() error { _, err := s.GroupDLQRequeue("s", "g", nil, nil); return err }(),
		"both": func() error {
			_, err := s.GroupDLQDelete("s", "g", []StreamID{{1, 1}}, &StreamGroupDLQDeleteOptions{All: true})
			return err
		}(),
		"no group": func() error { _, err := s.GroupDLQList("s", "", nil); return err }(),
		"big limit": func() error {
			_, err := s.GroupDLQList("s", "g", &StreamGroupDLQListOptions{Limit: MaxDLQPage + 1})
			return err
		}(),
	} {
		if err == nil || IsConnectionError(err) {
			t.Errorf("%s: %v, want refused before sending", name, err)
		}
	}
}

func TestGroupDLQAnswersAreDecodedWhole(t *testing.T) {
	w := bodyWriter{}
	w.u8(1)
	w.u64(5)
	w.u64(6)
	w.u32(5)    // deliveries
	w.u32(5)    // failures
	w.u64(1800) // dead_at_ms
	w.bytes16([]byte("bad input"))
	w.u8(1)
	w.u64(1_800_000_000)
	w.u64(5)
	w.u64(6)
	page, err := decodeGroupDLQPage(w.buf)
	if err != nil {
		t.Fatal(err)
	}
	want := &StreamGroupDLQPage{Dead: []StreamGroupDeadLetter{{ID: StreamID{5, 6}, Deliveries: 5, Failures: 5, DeadAtMS: 1800, Error: "bad input"}},
		Cursor: &StreamGroupDLQCursor{deadNS: 1_800_000_000, id: StreamID{5, 6}}}
	if !reflect.DeepEqual(page, want) {
		t.Fatalf("page %+v, want %+v", page, want)
	}
	for n := range w.buf {
		if _, err := decodeGroupDLQPage(w.buf[:n]); !errors.Is(err, ErrIncompleteResponse) {
			t.Fatalf("page cut at %d: %v", n, err)
		}
	}

	r := bodyWriter{}
	r.u32(0)
	r.u8(1)
	r.u64(5)
	r.u64(7)
	r.u8(uint8(QueueNotDead))
	r.bytes8([]byte("not dead"))
	r.u8(0)
	rep, err := decodeGroupDLQReport(r.buf)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rep, &StreamGroupDLQReport{Refused: []StreamGroupRefusal{{ID: StreamID{5, 7}, Why: QueueNotDead, Reason: "not dead"}}}) {
		t.Fatalf("report %+v", rep)
	}
	for n := range r.buf {
		if _, err := decodeGroupDLQReport(r.buf[:n]); !errors.Is(err, ErrIncompleteResponse) {
			t.Fatalf("report cut at %d: %v", n, err)
		}
	}
}
