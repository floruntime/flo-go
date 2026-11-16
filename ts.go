package flo

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

// TSClient writes and reads time-series points. A point belongs to a
// measurement, a field ("value" unless named) and a series, which its tags
// pick. Every time on the wire is Unix nanoseconds in an int64.
type TSClient struct {
	client *Client
}

const (
	// MaxTSReadLimit is the most points one Read page holds.
	MaxTSReadLimit = 4096
	// MaxTSTagFilter is the longest tag filter a Read takes.
	MaxTSTagFilter = 1024
	// maxTSField is the longest field name a Read takes.
	maxTSField = math.MaxUint16
	// maxTSCursor is the longest cursor a Read takes: a page answers 16 bytes.
	maxTSCursor = 16
)

// TSOrder is which way a Read goes through the points.
type TSOrder uint8

const (
	// TSOldestFirst reads in time order, then write order.
	TSOldestFirst TSOrder = 0
	// TSNewestFirst reads the newest point first.
	TSNewestFirst TSOrder = 1
)

func (o TSOrder) String() string {
	switch o {
	case TSOldestFirst:
		return "oldest_first"
	case TSNewestFirst:
		return "newest_first"
	}
	return fmt.Sprintf("TSOrder(%d)", uint8(o))
}

// TSPoint is one point a Read answers.
type TSPoint struct {
	// TimeNS is the point's time in Unix nanoseconds.
	TimeNS int64
	Value  float64
}

// Time is the point's time as a time.Time, to the nanosecond.
func (p TSPoint) Time() time.Time { return time.Unix(0, p.TimeNS) }

// TSWriteOptions are a Write's optional parts.
type TSWriteOptions struct {
	Namespace string
	// Field is the field written; "" is "value".
	Field string
	// Tags pick the series: comma-separated key=value pairs, in any order.
	Tags string
	// TimeNS is the point's time in Unix nanoseconds; 0 takes the server's
	// time. Pass t.UnixNano() for a time.Time. A time before 1970 is
	// refused.
	TimeNS int64
}

// TSWriteResult is a written point's place.
type TSWriteResult struct {
	SeriesHash uint64
	// TimeNS is the point's time in Unix nanoseconds: the one given, or
	// the server's when none was.
	TimeNS   int64
	Sequence uint64
}

// Time is the written point's time as a time.Time.
func (r TSWriteResult) Time() time.Time { return time.Unix(0, r.TimeNS) }

// Write writes one point of a measurement. The value must be finite.
func (ts *TSClient) Write(measurement string, value float64, opts *TSWriteOptions) (*TSWriteResult, error) {
	if opts == nil {
		opts = &TSWriteOptions{}
	}
	// An option's length is one byte.
	if len(opts.Field) > 255 || len(opts.Tags) > 255 {
		return nil, fmt.Errorf("flo: ts write field and tags are at most 255 bytes each")
	}
	b := NewOptionsBuilder()
	if opts.Field != "" {
		b.AddBytes(OptTSField, []byte(opts.Field))
	}
	if opts.Tags != "" {
		b.AddBytes(OptTSTags, []byte(opts.Tags))
	}
	if opts.TimeNS != 0 {
		b.AddU64(OptTSTimestampNS, uint64(opts.TimeNS))
	}
	resp, err := ts.client.sendAndCheck(OpTSWrite, ts.client.getNamespace(opts.Namespace), []byte(measurement),
		encodeTSValue(value), b.Build(), false)
	if err != nil {
		return nil, err
	}
	return decodeTSWriteAnswer(resp.Data)
}

func encodeTSValue(v float64) []byte {
	return binary.LittleEndian.AppendUint64(make([]byte, 0, 8), math.Float64bits(v))
}

// decodeTSWriteAnswer reads ts.write's answer:
// [series_hash:u64][time_ns:i64][sequence:u64].
func decodeTSWriteAnswer(data []byte) (*TSWriteResult, error) {
	r := bodyReader{buf: data}
	got := &TSWriteResult{SeriesHash: r.u64(), TimeNS: int64(r.u64()), Sequence: r.u64()}
	if err := r.done(); err != nil {
		return nil, err
	}
	return got, nil
}

// TSReadOptions pick which points a Read answers.
type TSReadOptions struct {
	Namespace string
	// Field is the field read; "" is "value".
	Field string
	// Tags keeps only series whose tags include every key=value pair of
	// this comma-separated list; "" keeps all.
	Tags string
	// FromNS and ToNS bound the points' times, inclusive, in Unix
	// nanoseconds; nil for no bound.
	FromNS *int64
	ToNS   *int64
	// Limit is the most points on the page: 1 to MaxTSReadLimit, 0 for
	// MaxTSReadLimit.
	Limit uint32
	Order TSOrder
	// Cursor is the last page's Cursor, nil to start. It is opaque.
	Cursor []byte
}

// TSReadResult is one page of points.
type TSReadResult struct {
	// Points of every matching series, merged in time order (then write
	// order), or newest first.
	Points []TSPoint
	// Cursor reads the next page; nil on the last page. A page may come
	// back short with more to follow: only a nil Cursor ends the read.
	Cursor []byte
	// Stale: the read was served by a shard behind what its group
	// committed (halted, or blocked reading the log), from its state then.
	Stale bool
}

// Read returns one page of a measurement field's points. Pass the result's
// Cursor in the next call's options to read on; a page continues strictly
// after the cursor (before it, newest first), so a point there for the
// whole read is answered exactly once. ReadAll walks every page.
func (ts *TSClient) Read(measurement string, opts *TSReadOptions) (*TSReadResult, error) {
	if opts == nil {
		opts = &TSReadOptions{}
	}
	body, err := encodeTSReadBody(opts)
	if err != nil {
		return nil, err
	}
	resp, err := ts.client.sendAndCheck(OpTSRead, ts.client.getNamespace(opts.Namespace), []byte(measurement), body, nil, false)
	if err != nil {
		return nil, err
	}
	got, err := decodeTSReadAnswer(resp.Data)
	if err != nil {
		return nil, err
	}
	got.Stale = resp.Stale
	return got, nil
}

// encodeTSReadBody codes ts.read's request: field, tags, from_ns, to_ns,
// limit, order, cursor. Every field but the two times has a default, so is
// sent present.
func encodeTSReadBody(opts *TSReadOptions) ([]byte, error) {
	field := opts.Field
	if field == "" {
		field = "value"
	}
	if len(field) > maxTSField {
		return nil, fmt.Errorf("flo: ts field is %d bytes, at most %d", len(field), maxTSField)
	}
	if len(opts.Tags) > MaxTSTagFilter {
		return nil, fmt.Errorf("flo: ts tag filter is %d bytes, at most %d", len(opts.Tags), MaxTSTagFilter)
	}
	for _, t := range []*int64{opts.FromNS, opts.ToNS} {
		if t != nil && *t < 0 {
			return nil, fmt.Errorf("flo: ts read time %d ns is before 1970", *t)
		}
	}
	limit := opts.Limit
	if limit == 0 {
		limit = MaxTSReadLimit
	}
	if limit > MaxTSReadLimit {
		return nil, fmt.Errorf("flo: ts read limit %d, want 1..%d", limit, MaxTSReadLimit)
	}
	if opts.Order > TSNewestFirst {
		return nil, fmt.Errorf("flo: ts read order %d is not one the server knows", uint8(opts.Order))
	}
	if len(opts.Cursor) > maxTSCursor {
		return nil, fmt.Errorf("flo: ts cursor is %d bytes, at most %d", len(opts.Cursor), maxTSCursor)
	}
	w := bodyWriter{buf: make([]byte, 0, 32+len(field)+len(opts.Tags))}
	w.u8(1)
	w.bytes16([]byte(field))
	w.u8(1)
	w.bytes16([]byte(opts.Tags))
	for _, t := range []*int64{opts.FromNS, opts.ToNS} {
		if t == nil {
			w.u8(0)
		} else {
			w.u8(1)
			w.u64(uint64(*t))
		}
	}
	w.u8(1)
	w.u32(limit)
	w.u8(1)
	w.u8(uint8(opts.Order))
	w.u8(1)
	w.bytes8(opts.Cursor)
	return w.buf, nil
}

// decodeTSReadAnswer reads ts.read's answer: points, each
// [time_ns:i64][value:f64], then the optional next cursor.
func decodeTSReadAnswer(data []byte) (*TSReadResult, error) {
	r := bodyReader{buf: data}
	n := int(r.u16())
	if n > MaxTSReadLimit || n*16 > len(data) {
		return nil, ErrIncompleteResponse
	}
	got := &TSReadResult{Points: make([]TSPoint, n)}
	for i := range got.Points {
		got.Points[i] = TSPoint{TimeNS: int64(r.u64()), Value: math.Float64frombits(r.u64())}
	}
	if r.present() {
		got.Cursor = r.bytes8()
		if len(got.Cursor) == 0 || len(got.Cursor) > maxTSCursor {
			return nil, ErrIncompleteResponse
		}
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	return got, nil
}

// TSPointIter walks every page of a Read:
//
//	it := c.TS.ReadAll("cpu", nil)
//	for it.Next() {
//		p := it.Point()
//	}
//	if err := it.Err(); err != nil { ... }
//
// A failed page, a missing measurement's not-found included, ends the walk
// with that error in Err.
type TSPointIter struct {
	ts          *TSClient
	measurement string
	opts        TSReadOptions
	page        []TSPoint
	at          int
	point       TSPoint
	done        bool
	err         error
}

// ReadAll walks every page of a Read with opts (whose Cursor, if set, is
// where it starts), asking for the next page until one answers no cursor.
func (ts *TSClient) ReadAll(measurement string, opts *TSReadOptions) *TSPointIter {
	it := &TSPointIter{ts: ts, measurement: measurement}
	if opts != nil {
		it.opts = *opts
	}
	return it
}

// Next moves to the next point, reading the next page when this one is
// used up. It is false at the end or on an error.
func (it *TSPointIter) Next() bool {
	for it.at >= len(it.page) {
		if it.done || it.err != nil {
			return false
		}
		got, err := it.ts.Read(it.measurement, &it.opts)
		if err != nil {
			it.err = err
			return false
		}
		it.page, it.at = got.Points, 0
		it.opts.Cursor = got.Cursor
		it.done = got.Cursor == nil
	}
	it.point = it.page[it.at]
	it.at++
	return true
}

// Point is the point Next moved to.
func (it *TSPointIter) Point() TSPoint { return it.point }

// Err is the error that ended the walk, nil if it reached the end.
func (it *TSPointIter) Err() error { return it.err }

// FloQLPoint is one point of a FloQL answer. Its time is Unix
// milliseconds: FloQL answers in ms.
type FloQLPoint struct {
	TimeMS int64
	Value  float64
}

// FloQLSeries is one series of a FloQL answer.
type FloQLSeries struct {
	Key    string
	Field  string
	Points []FloQLPoint
}

// TSFloQLOptions are a FloQL query's optional parts.
type TSFloQLOptions struct {
	Namespace string
}

// FloQL runs a FloQL query. An answer of more than MaxTSReadLimit raw
// points is refused (bad request, with the server's text): aggregate them,
// narrow the query, or page through them with Read.
func (ts *TSClient) FloQL(query string, opts *TSFloQLOptions) ([]FloQLSeries, error) {
	if opts == nil {
		opts = &TSFloQLOptions{}
	}
	resp, err := ts.client.sendAndCheck(OpTSFloQL, ts.client.getNamespace(opts.Namespace), nil, []byte(query), nil, false)
	if err != nil {
		return nil, err
	}
	return decodeFloQLAnswer(resp.Data)
}

// decodeFloQLAnswer reads a FloQL answer: [series_count:u32], each
// [key_len:u32][key][field_len:u32][field][count:u32]([time_ms:i64][value:f64])*.
func decodeFloQLAnswer(data []byte) ([]FloQLSeries, error) {
	r := bodyReader{buf: data}
	n := int(r.u32())
	// Each series takes at least 12 bytes.
	if n*12 > len(data) {
		return nil, ErrIncompleteResponse
	}
	series := make([]FloQLSeries, 0, n)
	for i := 0; i < n && !r.bad; i++ {
		s := FloQLSeries{Key: string(r.bytes32()), Field: string(r.bytes32())}
		count := int(r.u32())
		if count*16 > len(r.buf) {
			return nil, ErrIncompleteResponse
		}
		s.Points = make([]FloQLPoint, count)
		for j := range s.Points {
			s.Points[j] = FloQLPoint{TimeMS: int64(r.u64()), Value: math.Float64frombits(r.u64())}
		}
		series = append(series, s)
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	return series, nil
}
