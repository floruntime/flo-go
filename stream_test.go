package flo

import (
	"encoding/binary"
	"testing"
)

// buildPendingWire builds the server's PEL wire format for testing parse:
// [count:u32]([ts:u64][seq:u64][delivery_count:u32][consumer_len:u16][consumer])*
func buildPendingWire(entries []PendingEntry) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(entries)))
	for _, e := range entries {
		row := make([]byte, 8+8+4+2+len(e.Consumer))
		binary.LittleEndian.PutUint64(row[0:], e.ID.TimestampMS)
		binary.LittleEndian.PutUint64(row[8:], e.ID.Sequence)
		binary.LittleEndian.PutUint32(row[16:], e.DeliveryCount)
		binary.LittleEndian.PutUint16(row[20:], uint16(len(e.Consumer)))
		copy(row[22:], e.Consumer)
		buf = append(buf, row...)
	}
	return buf
}

func TestParsePendingEntries(t *testing.T) {
	in := []PendingEntry{
		{ID: StreamID{TimestampMS: 100, Sequence: 1}, Consumer: "worker-a", DeliveryCount: 2},
		{ID: StreamID{TimestampMS: 100, Sequence: 2}, Consumer: "worker-b", DeliveryCount: 1},
	}
	out, err := parsePendingEntries(buildPendingWire(in))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("count: got %d want 2", len(out))
	}
	if out[0].Consumer != "worker-a" || out[0].DeliveryCount != 2 || out[0].ID.Sequence != 1 {
		t.Fatalf("entry0 mismatch: %+v", out[0])
	}
	if out[1].Consumer != "worker-b" || out[1].ID.Sequence != 2 {
		t.Fatalf("entry1 mismatch: %+v", out[1])
	}

	// Empty PEL.
	empty, err := parsePendingEntries([]byte{0, 0, 0, 0})
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty: got %d entries err=%v", len(empty), err)
	}
	// Short buffer is treated as empty.
	if e, _ := parsePendingEntries([]byte{1, 2}); len(e) != 0 {
		t.Fatalf("short buffer should be empty, got %d", len(e))
	}
}

func TestParseClaimResponse(t *testing.T) {
	// Empty records blob + MAX cursor → Done, 0 records.
	maxCursor := make([]byte, 16)
	binary.LittleEndian.PutUint64(maxCursor[0:], ^uint64(0))
	binary.LittleEndian.PutUint64(maxCursor[8:], ^uint64(0))

	emptyBlob := make([]byte, 4) // count=0
	data := append(append([]byte{}, emptyBlob...), maxCursor...)
	res, err := parseClaimResponse(data)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !res.Done {
		t.Fatalf("expected Done for MAX cursor")
	}
	if len(res.Records) != 0 {
		t.Fatalf("expected 0 records, got %d", len(res.Records))
	}

	// Non-MAX cursor → not Done, cursor exposed.
	midCursor := make([]byte, 16)
	binary.LittleEndian.PutUint64(midCursor[0:], 100)
	binary.LittleEndian.PutUint64(midCursor[8:], 7)
	data2 := append(append([]byte{}, emptyBlob...), midCursor...)
	res2, err := parseClaimResponse(data2)
	if err != nil {
		t.Fatalf("parse2: %v", err)
	}
	if res2.Done {
		t.Fatalf("expected not Done for mid cursor")
	}
	if res2.NextCursor.TimestampMS != 100 || res2.NextCursor.Sequence != 7 {
		t.Fatalf("cursor mismatch: %+v", res2.NextCursor)
	}

	// Degenerate short response → Done, empty.
	short, err := parseClaimResponse([]byte{1, 2, 3})
	if err != nil || !short.Done || len(short.Records) != 0 {
		t.Fatalf("short response: done=%v records=%d err=%v", short.Done, len(short.Records), err)
	}
}

func TestBuildStreamBatchValue(t *testing.T) {
	value := buildStreamBatchValue([]byte("hello"), map[string]string{"k1": "v1"})

	if got := binary.LittleEndian.Uint32(value[0:4]); got != 1 {
		t.Fatalf("record_count: got %d want 1", got)
	}
	if got := binary.LittleEndian.Uint32(value[4:8]); got != 5 {
		t.Fatalf("payload_len: got %d want 5", got)
	}
	if string(value[8:13]) != "hello" {
		t.Fatalf("payload: got %q want hello", value[8:13])
	}
	if got := binary.LittleEndian.Uint16(value[13:15]); got != 1 {
		t.Fatalf("header_count: got %d want 1", got)
	}
}
