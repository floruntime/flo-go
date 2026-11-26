package flo

// workflow.history and action.history are table ops with one request and
// answer, each routed by its run id, so both read into RunHistoryPage.

const (
	// MaxRunHistoryLimit is the most events one history page holds,
	// whatever its Limit asks.
	MaxRunHistoryLimit = 1000
)

// encodeRunHistoryBody codes a history request: limit (Defaulted u32),
// cursor (Optional u64), newest_first (Defaulted bool). A limit of 0 and a
// NewestFirst of false are sent absent, so the server's defaults apply;
// an explicit 0 would be read as 1.
func encodeRunHistoryBody(opts *RunHistoryOptions) []byte {
	var w bodyWriter
	if opts.Limit == 0 {
		w.u8(0)
	} else {
		w.u8(1)
		w.u32(opts.Limit)
	}
	w.optU64(opts.Cursor)
	if opts.NewestFirst {
		w.u8(1)
		w.u8(1)
	} else {
		w.u8(0)
	}
	return w.buf
}

// decodeRunHistoryPage reads a history answer: events (a u16 count, each
// [event_type: u32 length][detail: u32 length][extra: u16 length]
// [timestamp_ms: i64]), has_more, then the cursor, present exactly when
// more follow.
func decodeRunHistoryPage(data []byte) (*RunHistoryPage, error) {
	r := bodyReader{buf: data}
	n := int(r.u16())
	if n > MaxRunHistoryLimit {
		r.bad = true
		n = 0
	}
	page := &RunHistoryPage{Events: make([]RunHistoryEvent, 0, n)}
	for i := 0; i < n && !r.bad; i++ {
		page.Events = append(page.Events, RunHistoryEvent{
			Type:        string(r.bytes32()),
			Detail:      r.bytes32(),
			Extra:       string(r.bytes16()),
			TimestampMS: int64(r.u64()),
		})
	}
	page.HasMore = r.bool()
	if r.present() {
		v := r.u64()
		page.Cursor = &v
	}
	if page.HasMore != (page.Cursor != nil) {
		r.bad = true
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	return page, nil
}

// runHistory reads one page of runID's history with op.
func (c *Client) runHistory(op OpCode, runID string, opts *RunHistoryOptions) (*RunHistoryPage, error) {
	if opts == nil {
		opts = &RunHistoryOptions{}
	}
	resp, err := c.sendAndCheck(op, c.getNamespace(opts.Namespace), []byte(runID), encodeRunHistoryBody(opts), nil, false)
	if err != nil {
		return nil, err
	}
	return decodeRunHistoryPage(resp.Data)
}

// runHistoryHoldMS is how long the server may hold a history read while
// the run's history catches up, so the read's deadline allows for it.
const runHistoryHoldMS = 5000
