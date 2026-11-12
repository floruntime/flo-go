package flo

import (
	"encoding/binary"
	"errors"
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
		binary.LittleEndian.PutUint32(row[16:], e.Deliveries)
		binary.LittleEndian.PutUint16(row[20:], uint16(len(e.Consumer)))
		copy(row[22:], e.Consumer)
		buf = append(buf, row...)
	}
	return buf
}

func TestParsePendingEntries(t *testing.T) {
	in := []PendingEntry{
		{ID: StreamID{TimestampMS: 100, Sequence: 1}, Consumer: "worker-a", Deliveries: 2},
		{ID: StreamID{TimestampMS: 100, Sequence: 2}, Consumer: "worker-b", Deliveries: 1},
	}
	out, err := parsePendingEntries(buildPendingWire(in))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("count: got %d want 2", len(out))
	}
	if out[0].Consumer != "worker-a" || out[0].Deliveries != 2 || out[0].ID.Sequence != 1 {
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

// The server answers info on a stream that doesn't exist with not_found;
// Info reports it as ErrNotFound with the server's message.
func TestStreamInfoOfAnUnknownStreamIsNotFound(t *testing.T) {
	c := tableServer(t, func(op OpCode, _ []byte) (StatusCode, []byte) {
		if op != OpStreamInfo {
			return StatusBadRequest, refusalBody(ReasonUnknownOp, RanNo, "unexpected op")
		}
		return StatusNotFound, refusalBody(ReasonNotFound, RanNo, "stream not found")
	})
	info, err := c.Stream.Info("nope", nil)
	var se *ServerError
	if info != nil || !IsNotFound(err) || !errors.As(err, &se) || se.Message != "stream not found" {
		t.Fatalf("info = %+v, err = %v; want ErrNotFound with the server's message", info, err)
	}
}
