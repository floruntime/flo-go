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
//
// A read is not found (IsNotFound) when no series of the field matches the
// tag filter: a measurement never written, and a filter that matches
// nothing, are not found, not empty. A matching series with no points in
// the range or past the cursor is ok and empty. A stale node's not-found is
// ErrStaleNotFound (IsStale, and still IsNotFound): the series may exist
// but that node hasn't seen it yet, so read again rather than treat it as
// gone.
func (ts *TSClient) Read(measurement string, opts *TSReadOptions) (*TSReadResult, error) {
	if opts == nil {
		opts = &TSReadOptions{}
	}
	body, err := encodeTSReadBody(opts)
	if err != nil {
		return nil, err
	}
	resp, err := ts.client.sendRequest(OpTSRead, ts.client.getNamespace(opts.Namespace), []byte(measurement), body, nil)
	if err != nil {
		return nil, err
	}
	if err := checkRead(resp); err != nil {
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
// A failed page ends the walk with that error in Err: no matching series
// is not found, not an empty walk, and a stale node's is ErrStaleNotFound.
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

// FloQLPoint is one point of a FloQL answer.
type FloQLPoint struct {
	// TimeNS is the point's time in Unix nanoseconds.
	TimeNS int64
	Value  float64
}

// Time is the point's time as a time.Time.
func (p FloQLPoint) Time() time.Time { return time.Unix(0, p.TimeNS) }

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

// FloQL runs a FloQL query. Its answer's times are ns; in the query text,
// a bare number in a range is Unix milliseconds. An answer of more than
// MaxTSReadLimit raw points is refused (bad request, with the server's
// text): aggregate them, narrow the query, or page through them with Read.
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
// [key_len:u32][key][field_len:u32][field][count:u32]([time_ns:i64][value:f64])*.
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
			s.Points[j] = FloQLPoint{TimeNS: int64(r.u64()), Value: math.Float64frombits(r.u64())}
		}
		series = append(series, s)
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	return series, nil
}

// TSAggregation is how a Query combines a window's points.
type TSAggregation string

const (
	TSAvg   TSAggregation = "avg"
	TSSum   TSAggregation = "sum"
	TSCount TSAggregation = "count"
	TSMin   TSAggregation = "min"
	TSMax   TSAggregation = "max"
)

// TSQueryOptions shape a Query.
type TSQueryOptions struct {
	Namespace string
	// Field is the field queried; "" is "value".
	Field string
	// Tags keeps only series whose tags include every key=value pair.
	Tags string
	// FromNS and ToNS bound the points' times, inclusive, in Unix
	// nanoseconds; 0 for no bound. A negative one is refused.
	FromNS int64
	ToNS   int64
	// Window is each bucket's width, to the nanosecond; 0 for a minute.
	// Buckets are aligned to the epoch. A negative one is refused.
	Window time.Duration
	// Aggregation combines a window's points; "" is TSAvg.
	Aggregation TSAggregation
}

// TSBucket is one window of a Query: its start in Unix nanoseconds, and
// its points' aggregate.
type TSBucket struct {
	StartNS int64
	Value   float64
}

// Start is the bucket's start as a time.Time.
func (b TSBucket) Start() time.Time { return time.Unix(0, b.StartNS) }

// TSQuerySeries is one series of a Query's answer.
type TSQuerySeries struct {
	Key     string
	Buckets []TSBucket
}

// Query aggregates a measurement field's points into windows. Only windows
// that hold points are answered, at most 1024: more is refused (bad
// request, with the server's text), so widen the window or narrow the
// range.
func (ts *TSClient) Query(measurement string, opts *TSQueryOptions) ([]TSQuerySeries, error) {
	if opts == nil {
		opts = &TSQueryOptions{}
	}
	if len(opts.Field) > 255 || len(opts.Tags) > 255 || len(opts.Aggregation) > 255 {
		return nil, fmt.Errorf("flo: ts query field, tags and aggregation are at most 255 bytes each")
	}
	resp, err := ts.client.sendAndCheck(OpTSQuery, ts.client.getNamespace(opts.Namespace), []byte(measurement), nil, encodeTSQueryOptions(opts), false)
	if err != nil {
		return nil, err
	}
	return decodeTSQueryAnswer(resp.Data)
}

// encodeTSQueryOptions codes a query's options; a zero time or window is
// left out, which the server reads as no bound or a minute.
func encodeTSQueryOptions(opts *TSQueryOptions) []byte {
	b := NewOptionsBuilder()
	if opts.FromNS != 0 {
		b.AddU64(OptTSFromNS, uint64(opts.FromNS))
	}
	if opts.ToNS != 0 {
		b.AddU64(OptTSToNS, uint64(opts.ToNS))
	}
	if opts.Window != 0 {
		b.AddU64(OptTSWindowNS, uint64(opts.Window.Nanoseconds()))
	}
	if opts.Aggregation != "" {
		b.AddBytes(OptTSAggregation, []byte(opts.Aggregation))
	}
	if opts.Field != "" {
		b.AddBytes(OptTSField, []byte(opts.Field))
	}
	if opts.Tags != "" {
		b.AddBytes(OptTSTags, []byte(opts.Tags))
	}
	return b.Build()
}

// decodeTSQueryAnswer reads a query's answer: [series_count:u32], each
// [key_len:u32][key][bucket_count:u32]([start_ns:i64][value:f64])*.
func decodeTSQueryAnswer(data []byte) ([]TSQuerySeries, error) {
	r := bodyReader{buf: data}
	n := int(r.u32())
	// Each series takes at least 8 bytes.
	if n*8 > len(data) {
		return nil, ErrIncompleteResponse
	}
	series := make([]TSQuerySeries, 0, n)
	for i := 0; i < n && !r.bad; i++ {
		s := TSQuerySeries{Key: string(r.bytes32())}
		count := int(r.u32())
		if count*16 > len(r.buf) {
			return nil, ErrIncompleteResponse
		}
		s.Buckets = make([]TSBucket, count)
		for j := range s.Buckets {
			s.Buckets[j] = TSBucket{StartNS: int64(r.u64()), Value: math.Float64frombits(r.u64())}
		}
		series = append(series, s)
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	return series, nil
}

// TSInfo is a measurement's summary.
type TSInfo struct {
	// Series counts each field of each tag set.
	Series uint32
	// FieldCount counts its distinct fields, up to 256.
	FieldCount uint32
	// FieldsCapped: it has more fields than were counted or listed.
	FieldsCapped bool
	Fields       []string
	Points       uint64
	// OldestNS and NewestNS are its oldest and newest points' times in
	// Unix nanoseconds; nil without points.
	OldestNS *int64
	NewestNS *int64
}

// TSInfoOptions are an Info's optional parts.
type TSInfoOptions struct {
	Namespace string
}

// Info summarizes a measurement. One with no series is an error that
// IsNotFound reports.
func (ts *TSClient) Info(measurement string, opts *TSInfoOptions) (*TSInfo, error) {
	if opts == nil {
		opts = &TSInfoOptions{}
	}
	resp, err := ts.client.sendAndCheck(OpTSInfo, ts.client.getNamespace(opts.Namespace), []byte(measurement), nil, nil, false)
	if err != nil {
		return nil, err
	}
	return decodeTSInfoAnswer(resp.Data)
}

// decodeTSInfoAnswer reads ts.info's answer: series, field_count,
// fields_capped, fields, points, oldest_ns, newest_ns.
func decodeTSInfoAnswer(data []byte) (*TSInfo, error) {
	r := bodyReader{buf: data}
	got := &TSInfo{Series: r.u32(), FieldCount: r.u32(), FieldsCapped: r.bool()}
	n := int(r.u16())
	if n > 256 || n*2 > len(r.buf) {
		return nil, ErrIncompleteResponse
	}
	for i := 0; i < n && !r.bad; i++ {
		got.Fields = append(got.Fields, string(r.bytes16()))
	}
	got.Points = r.u64()
	for _, t := range []**int64{&got.OldestNS, &got.NewestNS} {
		if r.present() {
			v := int64(r.u64())
			*t = &v
		}
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	return got, nil
}
