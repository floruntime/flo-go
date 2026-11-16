package flo

import (
	"bytes"
	"encoding/binary"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

// tsPage codes a ts.read answer: points, then the cursor if any.
func tsPage(cursor []byte, points ...TSPoint) []byte {
	w := bodyWriter{}
	w.u16(uint16(len(points)))
	for _, p := range points {
		w.u64(uint64(p.TimeNS))
		w.u64(math.Float64bits(p.Value))
	}
	if cursor == nil {
		w.u8(0)
	} else {
		w.u8(1)
		w.bytes8(cursor)
	}
	return w.buf
}

// tsReadOrderAndCursor reads a ts.read request body's order and cursor.
func tsReadOrderAndCursor(t *testing.T, body []byte) (byte, []byte) {
	t.Helper()
	r := bodyReader{buf: body}
	r.present()
	r.bytes16() // field
	r.present()
	r.bytes16() // tags
	for i := 0; i < 2; i++ {
		if r.present() {
			r.u64()
		}
	}
	r.present()
	r.u32() // limit
	r.present()
	order := r.u8()
	r.present()
	cursor := r.bytes8()
	if err := r.done(); err != nil {
		t.Fatalf("body % x: %v", body, err)
	}
	return order, cursor
}

func TestTSReadSendsEveryField(t *testing.T) {
	from, to := int64(1_700_000_000_000_000_001), int64(1_700_000_000_999_999_999)
	cursor := bytes.Repeat([]byte{0xab}, 16)
	p := capturePayload(t, func(c *Client) {
		c.TS.Read("cpu", &TSReadOptions{Field: "load", Tags: "host=a,dc=x", FromNS: &from, ToNS: &to,
			Limit: 7, Order: TSNewestFirst, Cursor: cursor})
	})
	_, key, body := tableBody(t, p)
	if key != "cpu" {
		t.Errorf("key %q, want the measurement", key)
	}
	want := cat(
		[]byte{1, 4, 0}, []byte("load"),
		[]byte{1, 11, 0}, []byte("host=a,dc=x"),
		[]byte{1}, u64le(uint64(from)),
		[]byte{1}, u64le(uint64(to)),
		[]byte{1, 7, 0, 0, 0},
		[]byte{1, 1},
		[]byte{1, 16}, cursor,
	)
	if !bytes.Equal(body, want) {
		t.Errorf("body\n % x\nwant\n % x", body, want)
	}
}

// With no options every defaulted field is still sent, with its default.
func TestTSReadSendsDefaults(t *testing.T) {
	p := capturePayload(t, func(c *Client) { c.TS.Read("cpu", nil) })
	_, _, body := tableBody(t, p)
	want := cat(
		[]byte{1, 5, 0}, []byte("value"),
		[]byte{1, 0, 0},
		[]byte{0}, []byte{0},
		[]byte{1, 0, 0x10, 0, 0},
		[]byte{1, 0},
		[]byte{1, 0},
	)
	if !bytes.Equal(body, want) {
		t.Errorf("body\n % x\nwant\n % x", body, want)
	}
}

func TestTSReadRefusesBadOptionsBeforeSending(t *testing.T) {
	neg := int64(-1)
	for name, opts := range map[string]*TSReadOptions{
		"limit":       {Limit: MaxTSReadLimit + 1},
		"from":        {FromNS: &neg},
		"to":          {ToNS: &neg},
		"order":       {Order: 2},
		"long cursor": {Cursor: make([]byte, 17)},
		"long tags":   {Tags: strings.Repeat("a", MaxTSTagFilter+1)},
	} {
		if _, err := encodeTSReadBody(opts); err == nil {
			t.Errorf("%s: encoded", name)
		}
	}
}

func TestTSReadDecodesAPageWithAndWithoutACursor(t *testing.T) {
	pts := []TSPoint{{1_700_000_000_123_456_789, 1.5}, {1_700_000_000_123_456_790, -2}}
	cursor := bytes.Repeat([]byte{7}, 16)
	got, err := decodeTSReadAnswer(tsPage(cursor, pts...))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Points, pts) || !bytes.Equal(got.Cursor, cursor) {
		t.Errorf("got %+v", got)
	}
	got, err = decodeTSReadAnswer(tsPage(nil, pts[0]))
	if err != nil {
		t.Fatal(err)
	}
	if got.Cursor != nil || len(got.Points) != 1 || got.Points[0] != pts[0] {
		t.Errorf("last page %+v", got)
	}
	if got.Points[0].Time().UnixNano() != pts[0].TimeNS {
		t.Errorf("Time() lost precision: %v", got.Points[0].Time())
	}
	for name, data := range map[string][]byte{
		"truncated":    tsPage(nil, pts...)[:20],
		"extra":        append(tsPage(nil), 0),
		"empty cursor": append(tsPage(nil)[:2], 1, 0),
	} {
		if _, err := decodeTSReadAnswer(data); err == nil {
			t.Errorf("%s: decoded", name)
		}
	}
}

// ReadAll asks again while a page answers a cursor, even after a short or
// empty page, and stops at the first page without one.
func TestTSReadAllStopsOnANoneCursorNotAShortPage(t *testing.T) {
	c1, c2 := bytes.Repeat([]byte{1}, 16), bytes.Repeat([]byte{2}, 16)
	pages := [][]byte{
		tsPage(c1, TSPoint{3, 30}),
		tsPage(c2),
		tsPage(nil, TSPoint{2, 20}, TSPoint{1, 10}),
	}
	var cursors [][]byte
	var orders []byte
	c := tableServer(t, func(op OpCode, p []byte) (StatusCode, []byte) {
		if op != OpTSRead {
			t.Errorf("op %v", op)
		}
		_, _, body := tableBody(t, p)
		order, cursor := tsReadOrderAndCursor(t, body)
		cursors = append(cursors, cursor)
		orders = append(orders, order)
		page := pages[0]
		pages = pages[1:]
		return StatusOK, page
	})
	it := c.TS.ReadAll("cpu", &TSReadOptions{Limit: 2, Order: TSNewestFirst})
	var got []TSPoint
	for it.Next() {
		got = append(got, it.Point())
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	if want := []TSPoint{{3, 30}, {2, 20}, {1, 10}}; !reflect.DeepEqual(got, want) {
		t.Errorf("points %v, want %v", got, want)
	}
	if len(cursors) != 3 || len(cursors[0]) != 0 || !bytes.Equal(cursors[1], c1) || !bytes.Equal(cursors[2], c2) {
		t.Errorf("cursors sent % x", cursors)
	}
	if !bytes.Equal(orders, []byte{1, 1, 1}) {
		t.Errorf("orders sent %v, want newest_first on every page", orders)
	}
	if it.Next() {
		t.Error("Next after the end")
	}
}

// A refused page ends the walk with the server's error: a bad cursor's
// bad_request, or a missing measurement's not_found.
func TestTSReadAllEndsOnARefusal(t *testing.T) {
	for _, tc := range []struct {
		status StatusCode
		reason Reason
		msg    string
		is     func(error) bool
	}{
		{StatusBadRequest, ReasonMalformed, "bad request: a cursor is the 16 bytes a page answered", IsBadRequest},
		{StatusNotFound, ReasonNotFound, "not found", IsNotFound},
	} {
		calls := 0
		c := tableServer(t, func(op OpCode, p []byte) (StatusCode, []byte) {
			calls++
			if calls == 1 {
				return StatusOK, tsPage(bytes.Repeat([]byte{1}, 16), TSPoint{1, 1})
			}
			return tc.status, refusalBody(tc.reason, RanNo, tc.msg)
		})
		it := c.TS.ReadAll("cpu", nil)
		n := 0
		for it.Next() {
			n++
		}
		if n != 1 || !tc.is(it.Err()) || !strings.Contains(it.Err().Error(), tc.msg) {
			t.Errorf("%v: %d points, err %v", tc.status, n, it.Err())
		}
		if it.Next() || calls != 2 {
			t.Errorf("%v: asked %d times; the walk goes on after an error", tc.status, calls)
		}
	}
}

func TestTSReadRefusalIsTheServersError(t *testing.T) {
	const msg = "bad request: a cursor is the 16 bytes a page answered"
	c := tableServer(t, func(op OpCode, p []byte) (StatusCode, []byte) {
		return StatusBadRequest, refusalBody(ReasonMalformed, RanNo, msg)
	})
	_, err := c.TS.Read("cpu", &TSReadOptions{Cursor: []byte{1, 2, 3}})
	if !IsBadRequest(err) || !strings.Contains(err.Error(), msg) {
		t.Errorf("err %v", err)
	}
}

// The write's time goes out as an i64 of nanoseconds, to the nanosecond.
func TestTSWriteSendsItsTimeInNanoseconds(t *testing.T) {
	const at = int64(1_700_000_000_123_456_789)
	p := capturePayload(t, func(c *Client) {
		c.TS.Write("cpu", 0.25, &TSWriteOptions{Field: "load", Tags: "host=a", TimeNS: at})
	})
	key, value, options := requestParts(t, p)
	if key != "cpu" || !bytes.Equal(value, u64le(math.Float64bits(0.25))) {
		t.Errorf("key %q value % x", key, value)
	}
	want := cat([]byte{byte(OptTSField), 4}, []byte("load"), []byte{byte(OptTSTags), 6}, []byte("host=a"),
		[]byte{byte(OptTSTimestampNS), 8}, u64le(uint64(at)))
	if !bytes.Equal(options, want) {
		t.Errorf("options % x, want % x", options, want)
	}
	if time.Unix(0, at).UnixNano() != at {
		t.Fatal("time.Time round trip")
	}
}

// No time leaves it to the server: no option at all.
func TestTSWriteWithoutATimeSendsNoTimestamp(t *testing.T) {
	p := capturePayload(t, func(c *Client) { c.TS.Write("cpu", 1, nil) })
	if _, _, options := requestParts(t, p); len(options) != 0 {
		t.Errorf("options % x, want none", options)
	}
}

// A time before 1970 is sent as given, and the server's refusal comes back.
func TestTSWriteBefore1970IsRefusedByTheServer(t *testing.T) {
	const msg = "ts write: a point's time must not be before 1970"
	var options []byte
	c := tableServer(t, func(op OpCode, p []byte) (StatusCode, []byte) {
		_, _, options = requestParts(t, p)
		return StatusBadRequest, refusalBody(ReasonMalformed, RanNo, msg)
	})
	_, err := c.TS.Write("cpu", 1, &TSWriteOptions{TimeNS: -5})
	if !IsBadRequest(err) || !strings.Contains(err.Error(), msg) {
		t.Errorf("err %v", err)
	}
	if want := cat([]byte{byte(OptTSTimestampNS), 8}, u64le(uint64(math.MaxUint64-4))); !bytes.Equal(options, want) {
		t.Errorf("options % x, want % x", options, want)
	}
}

func TestTSWriteDecodesItsAnswer(t *testing.T) {
	const at = int64(1_700_000_000_123_456_789)
	c := tableServer(t, func(op OpCode, p []byte) (StatusCode, []byte) {
		return StatusOK, cat(u64le(42), u64le(uint64(at)), u64le(9))
	})
	got, err := c.TS.Write("cpu", 1, &TSWriteOptions{TimeNS: at})
	if err != nil {
		t.Fatal(err)
	}
	if *got != (TSWriteResult{SeriesHash: 42, TimeNS: at, Sequence: 9}) || got.Time().UnixNano() != at {
		t.Errorf("got %+v", got)
	}
}

func TestFloQLDecodesSeriesAndSurfacesTheCap(t *testing.T) {
	ans := binary.LittleEndian.AppendUint32(nil, 1)
	ans = binary.LittleEndian.AppendUint32(ans, 3)
	ans = append(ans, "cpu"...)
	ans = binary.LittleEndian.AppendUint32(ans, 5)
	ans = append(ans, "value"...)
	ans = binary.LittleEndian.AppendUint32(ans, 1)
	ans = cat(ans, u64le(1000), u64le(math.Float64bits(2.5)))
	got, err := decodeFloQLAnswer(ans)
	if err != nil {
		t.Fatal(err)
	}
	if want := []FloQLSeries{{Key: "cpu", Field: "value", Points: []FloQLPoint{{1_000_000_000, 2.5}}}}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v", got)
	}

	const msg = "bad request: the answer holds more than 4096 points; aggregate them, narrow the query, or page through them with ts read"
	var key string
	var value []byte
	c := tableServer(t, func(op OpCode, p []byte) (StatusCode, []byte) {
		key, value, _ = requestParts(t, p)
		return StatusBadRequest, refusalBody(ReasonMalformed, RanNo, msg)
	})
	_, err = c.TS.FloQL("cpu[1h]", nil)
	if !IsBadRequest(err) || !strings.Contains(err.Error(), msg) {
		t.Errorf("err %v", err)
	}
	if key != "" || string(value) != "cpu[1h]" {
		t.Errorf("sent key %q value %q; the query goes in the value", key, value)
	}
}
