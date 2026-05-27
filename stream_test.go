package flo

import (
	"encoding/binary"
	"testing"
)

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
