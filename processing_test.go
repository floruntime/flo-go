package flo

import (
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func appendStr(b []byte, s string) []byte {
	b = binary.LittleEndian.AppendUint16(b, uint16(len(s)))
	return append(b, s...)
}

func processingStatusAnswer(status byte, reason string) []byte {
	b := appendStr(nil, "job-1")
	b = appendStr(b, "pipe")
	b = append(b, status)
	b = binary.LittleEndian.AppendUint32(b, 50)
	b = binary.LittleEndian.AppendUint64(b, 1234)
	b = binary.LittleEndian.AppendUint64(b, 99)
	return appendStr(b, reason)
}

func TestParseProcessingStatus(t *testing.T) {
	for _, tc := range []struct {
		status byte
		reason string
		want   ProcessingStatusResult
	}{
		{0, "", ProcessingStatusResult{JobID: "job-1", Name: "pipe", Status: "running", BatchSize: 50, RecordsProcessed: 1234, CreatedAt: 99}},
		{3, "sink unreachable", ProcessingStatusResult{JobID: "job-1", Name: "pipe", Status: "failed", BatchSize: 50, RecordsProcessed: 1234, CreatedAt: 99, FailureReason: "sink unreachable"}},
		{4, "", ProcessingStatusResult{JobID: "job-1", Name: "pipe", Status: "completed", BatchSize: 50, RecordsProcessed: 1234, CreatedAt: 99}},
	} {
		got, err := parseProcessingStatus(processingStatusAnswer(tc.status, tc.reason))
		if err != nil {
			t.Fatalf("status %d: %v", tc.status, err)
		}
		if *got != tc.want {
			t.Errorf("got %+v, want %+v", *got, tc.want)
		}
	}
}

func TestParseProcessingStatusRefusesMalformed(t *testing.T) {
	good := processingStatusAnswer(3, "boom")
	pastEnd := append([]byte{}, good...)
	binary.LittleEndian.PutUint16(pastEnd[len(pastEnd)-6:], 5) // reason_len 5, 4 bytes follow
	for name, b := range map[string][]byte{
		"reason length past the end": pastEnd,
		"trailing byte":              append(append([]byte{}, good...), 0),
		"no reason length":           good[:len(good)-6],
		"unknown status":             processingStatusAnswer(5, ""),
	} {
		if _, err := parseProcessingStatus(b); !errors.Is(err, ErrIncompleteResponse) {
			t.Errorf("%s: got %v, want ErrIncompleteResponse", name, err)
		}
	}
}

// A reason whose length runs past the end is refused by its own bounds check,
// not left to the trailing-bytes check to notice.
func TestParseProcessingStatusRefusesAReasonCutShort(t *testing.T) {
	good := processingStatusAnswer(3, "boo")
	b := append([]byte{}, good...)
	binary.LittleEndian.PutUint16(b[len(b)-5:], 10) // reason_len 10, 3 bytes follow
	_, err := parseProcessingStatus(b[:len(b):len(b)])
	if !errors.Is(err, ErrIncompleteResponse) || !strings.Contains(err.Error(), "ends before failure reason") {
		t.Errorf("got %v, want the failure reason refused as cut short", err)
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
