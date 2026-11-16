package flo

import (
	"encoding/binary"
	"strings"
	"sync/atomic"
	"testing"
)

// A read of a stream or series that doesn't exist is answered not found:
// that is the not-found error, never an empty read and never a refusal
// body parsed as records. An ok empty answer stays empty.
func TestReadsOfWhatDoesntExistAreNotFound(t *testing.T) {
	const streamMsg, seriesMsg = "stream not found", "not found: 'cpu' has no series of field 'value'"
	var missing atomic.Bool
	missing.Store(true)
	c := tableServer(t, func(op OpCode, p []byte) (StatusCode, []byte) {
		switch {
		case missing.Load() && op == OpStreamRead:
			return StatusNotFound, refusalBody(ReasonNotFound, RanNo, streamMsg)
		case missing.Load() && op == OpTSRead:
			return StatusNotFound, refusalBody(ReasonNotFound, RanNo, seriesMsg)
		case op == OpStreamRead:
			return StatusOK, binary.LittleEndian.AppendUint32(nil, 0)
		}
		return StatusOK, tsPage(nil)
	})

	if got, err := c.Stream.Read("s", nil); !IsNotFound(err) || !strings.Contains(err.Error(), streamMsg) {
		t.Errorf("stream read: %+v, %v; want the not-found error", got, err)
	}
	if got, err := c.TS.Read("cpu", nil); !IsNotFound(err) || !strings.Contains(err.Error(), seriesMsg) {
		t.Errorf("ts read: %+v, %v; want the not-found error", got, err)
	}

	missing.Store(false)
	if got, err := c.Stream.Read("s", nil); err != nil || got == nil || len(got.Records) != 0 {
		t.Errorf("empty stream read: %+v, %v; want ok and empty", got, err)
	}
	if got, err := c.TS.Read("cpu", nil); err != nil || len(got.Points) != 0 || got.Cursor != nil {
		t.Errorf("empty ts read: %+v, %v; want ok and empty", got, err)
	}
	it := c.TS.ReadAll("cpu", nil)
	if it.Next() || it.Err() != nil {
		t.Errorf("ReadAll of an empty range: err %v; want no points, no error", it.Err())
	}
}
