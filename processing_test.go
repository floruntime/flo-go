package flo

import (
	"encoding/binary"
	"testing"
)

func appendStr(b []byte, s string) []byte {
	b = binary.LittleEndian.AppendUint16(b, uint16(len(s)))
	return append(b, s...)
}

func TestParseProcessingStatus(t *testing.T) {
	b := appendStr(nil, "job-1")
	b = appendStr(b, "pipe")
	b = append(b, 1) // stopped
	b = binary.LittleEndian.AppendUint32(b, 50)
	b = binary.LittleEndian.AppendUint64(b, 1234)
	b = binary.LittleEndian.AppendUint64(b, 99)

	got, err := parseProcessingStatus(b)
	if err != nil {
		t.Fatal(err)
	}
	want := ProcessingStatusResult{JobID: "job-1", Name: "pipe", Status: "stopped", BatchSize: 50, RecordsProcessed: 1234, CreatedAt: 99}
	if *got != want {
		t.Errorf("got %+v, want %+v", *got, want)
	}
}

func TestParseProcessingList(t *testing.T) {
	b := binary.LittleEndian.AppendUint32(nil, 2)
	for _, e := range []ProcessingListEntry{{"a", "j1", "running", 7}, {"b", "j2", "failed", 8}} {
		b = appendStr(b, e.Name)
		b = appendStr(b, e.JobID)
		b = appendStr(b, e.Status)
		b = binary.LittleEndian.AppendUint64(b, uint64(e.CreatedAt))
	}
	b = append(b, 0)    // has_more
	b = append(b, 0, 0) // cursor_len

	got, err := parseProcessingList(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || *got[0] != (ProcessingListEntry{"a", "j1", "running", 7}) || *got[1] != (ProcessingListEntry{"b", "j2", "failed", 8}) {
		t.Errorf("got %+v %+v", got[0], got[1])
	}
}
