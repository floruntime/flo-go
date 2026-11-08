package flo

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
)

func appendStr(b []byte, s string) []byte {
	b = binary.LittleEndian.AppendUint16(b, uint16(len(s)))
	return append(b, s...)
}

// processingStatusAnswer is a processing.status answer laid out as the
// server writes it, with this node running the job.
func processingStatusAnswer(status byte, reason string) []byte {
	b := appendStr(nil, "job-1")
	b = appendStr(b, "pipe")
	b = append(b, status)
	b = binary.LittleEndian.AppendUint32(b, 50)
	b = binary.LittleEndian.AppendUint64(b, 1234)
	b = binary.LittleEndian.AppendUint64(b, 99)
	b = appendStr(b, reason)
	b = append(b, 1) // running
	for _, v := range []uint64{10, 9, 8, 7, 6, 5} {
		b = binary.LittleEndian.AppendUint64(b, v) // in, out, watermark, tick avg, tick max, uptime
	}
	b = binary.LittleEndian.AppendUint32(b, 4)
	return append(b, "name"...)
}

func TestParseProcessingStatus(t *testing.T) {
	base := ProcessingStatusResult{JobID: "job-1", Name: "pipe", BatchSize: 50, RecordsProcessed: 1234, CreatedAt: 99,
		Running: true, RecordsIn: 10, RecordsOut: 9, WatermarkMS: 8, TickAvgUS: 7, TickMaxUS: 6, UptimeMS: 5}
	for _, tc := range []struct {
		status byte
		reason string
		name   string
	}{
		{0, "", "running"},
		{0, "held here: no room", "running"},
		{3, "sink unreachable", "failed"},
		{4, "", "completed"},
	} {
		got, err := parseProcessingStatus(processingStatusAnswer(tc.status, tc.reason))
		if err != nil {
			t.Fatalf("status %d: %v", tc.status, err)
		}
		want := base
		want.Status, want.Reason = tc.name, tc.reason
		if string(got.YAML) != "name" {
			t.Errorf("yaml %q", got.YAML)
		}
		got.YAML = nil
		if !reflect.DeepEqual(*got, want) {
			t.Errorf("got %+v, want %+v", *got, want)
		}
	}
}

func TestParseProcessingStatusRefusesMalformed(t *testing.T) {
	good := processingStatusAnswer(3, "boom")
	for n := range good {
		if _, err := parseProcessingStatus(good[:n]); !errors.Is(err, ErrIncompleteResponse) {
			t.Errorf("cut at %d: got %v, want ErrIncompleteResponse", n, err)
		}
	}
	for name, b := range map[string][]byte{
		"trailing byte":  append(append([]byte{}, good...), 0),
		"unknown status": processingStatusAnswer(5, ""),
	} {
		if _, err := parseProcessingStatus(b); !errors.Is(err, ErrIncompleteResponse) {
			t.Errorf("%s: got %v, want ErrIncompleteResponse", name, err)
		}
	}
}

func TestProcessingRemoveSendsTheJobIDAsTheKey(t *testing.T) {
	p := capturePayload(t, func(c *Client) { c.Processing.Remove("job-9", nil) })
	if _, key, body := tableBody(t, p); key != "job-9" || len(body) != 0 {
		t.Errorf("key %q body % x, want job-9 and none", key, body)
	}
}

func TestParseProcessingList(t *testing.T) {
	cursor := []byte{0x05, 0x00, 'j', '2'}
	b := processingListPage(cursor,
		ProcessingListEntry{"a", "j1", "RUNNING", 7, 10, 100},
		ProcessingListEntry{"b", "j2", "FAILED: boom", 8, 20, 0})

	got, err := parseProcessingList(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Jobs) != 2 || *got.Jobs[0] != (ProcessingListEntry{"a", "j1", "RUNNING", 7, 10, 100}) || *got.Jobs[1] != (ProcessingListEntry{"b", "j2", "FAILED: boom", 8, 20, 0}) {
		t.Errorf("jobs = %+v", got.Jobs)
	}
	if !got.HasMore || !bytes.Equal(got.Cursor, cursor) {
		t.Errorf("has_more %v cursor % x, want true % x", got.HasMore, got.Cursor, cursor)
	}
	for n := range b {
		if _, err := parseProcessingList(b[:n]); !errors.Is(err, ErrIncompleteResponse) {
			t.Errorf("cut at %d: %v", n, err)
		}
	}
	if _, err := parseProcessingList(append(b, 0)); !errors.Is(err, ErrIncompleteResponse) {
		t.Errorf("trailing byte: %v", err)
	}
}
