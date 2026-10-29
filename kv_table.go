package flo

import (
	"encoding/binary"
	"errors"
	"fmt"
	"unicode/utf8"
)

// Every KV op is a table op: the request is [namespace][key][body], the
// body coded by the op's schema, field by field in order, and an ok answer
// is the op's response fields in order. An optional field is a presence
// byte then its value; a field with a default is sent present. A byte
// string or list has a length prefix as wide as its bound needs.

// isTableOp reports whether op's request carries a schema-coded body
// rather than a value and options.
func isTableOp(op OpCode) bool {
	switch op {
	case OpKVPut, OpKVGet, OpKVMGet, OpKVDelete, OpKVScan, OpKVHistory,
		OpKVIncr, OpKVJsonGet, OpKVJsonSet, OpKVJsonDel,
		OpKVBeginTxn, OpKVCommitTxn, OpKVRollbackTxn, OpKVExpire, OpKVExists,
		OpActionStatus, OpWorkerList, OpWorkerInfo:
		return true
	}
	return false
}

const (
	// MaxKVValueSize is the longest value kv.set takes.
	MaxKVValueSize = 253817
	// MaxTTLMs is the longest TTL a key may have.
	MaxTTLMs = 18446744073709
	// MaxScanLimit is the most names one Scan page may ask for.
	MaxScanLimit = 1024
	// MaxHistoryLimit is the most versions one History may ask for.
	MaxHistoryLimit = 65
	// MaxJSONPathSize is the longest JSON path.
	MaxJSONPathSize = 1024
	// MaxJSONDocumentSize is the longest JSON document JsonSet takes.
	MaxJSONDocumentSize = 252819

	// maxRoutingKey is the longest routing key; its length prefix is a u16.
	maxRoutingKey = 4096
	// maxMGetKey is the longest key one MGet looks up.
	maxMGetKey = 4096
	// maxMGetKeyBytes is the most one kv.mget request's encoded keys (each
	// length and bytes, not the count) may total: the request frame less
	// the count and the longest routing key.
	maxMGetKeyBytes = 253848
	// maxScanCursor is the longest scan cursor; its length prefix is a u8.
	maxScanCursor = 64
	// defaultHistoryLimit is the version count when the caller gives none.
	defaultHistoryLimit = 10
)

type bodyWriter struct{ buf []byte }

func (w *bodyWriter) u8(v uint8) { w.buf = append(w.buf, v) }
func (w *bodyWriter) u16(v uint16) {
	w.buf = binary.LittleEndian.AppendUint16(w.buf, v)
}
func (w *bodyWriter) u32(v uint32) {
	w.buf = binary.LittleEndian.AppendUint32(w.buf, v)
}
func (w *bodyWriter) u64(v uint64) {
	w.buf = binary.LittleEndian.AppendUint64(w.buf, v)
}
func (w *bodyWriter) optU64(v *uint64) {
	if v == nil {
		w.u8(0)
		return
	}
	w.u8(1)
	w.u64(*v)
}
func (w *bodyWriter) bytes16(b []byte) {
	w.u16(uint16(len(b)))
	w.buf = append(w.buf, b...)
}
func (w *bodyWriter) bytes32(b []byte) {
	w.u32(uint32(len(b)))
	w.buf = append(w.buf, b...)
}
func (w *bodyWriter) routingKey(k string) {
	if k == "" {
		w.u8(0)
		return
	}
	w.u8(1)
	w.bytes16([]byte(k))
}

// bodyReader reads an answer field by field. A read past the end, or a
// presence or bool byte that isn't 0 or 1, marks the answer bad; done
// also refuses bytes left over.
type bodyReader struct {
	buf []byte
	bad bool
}

func (r *bodyReader) take(n int) []byte {
	if r.bad || n > len(r.buf) {
		r.bad = true
		return nil
	}
	b := r.buf[:n]
	r.buf = r.buf[n:]
	return b
}
func (r *bodyReader) u8() uint8 {
	if b := r.take(1); b != nil {
		return b[0]
	}
	return 0
}
func (r *bodyReader) u16() uint16 {
	if b := r.take(2); b != nil {
		return binary.LittleEndian.Uint16(b)
	}
	return 0
}
func (r *bodyReader) u32() uint32 {
	if b := r.take(4); b != nil {
		return binary.LittleEndian.Uint32(b)
	}
	return 0
}
func (r *bodyReader) u64() uint64 {
	if b := r.take(8); b != nil {
		return binary.LittleEndian.Uint64(b)
	}
	return 0
}
func (r *bodyReader) bool() bool {
	switch r.u8() {
	case 0:
		return false
	case 1:
		return true
	}
	r.bad = true
	return false
}

// present reads an optional field's presence byte.
func (r *bodyReader) present() bool { return r.bool() }

func (r *bodyReader) bytesN(n int) []byte {
	b := r.take(n)
	if b == nil {
		return nil
	}
	out := make([]byte, n)
	copy(out, b)
	return out
}
func (r *bodyReader) bytes8() []byte  { return r.bytesN(int(r.u8())) }
func (r *bodyReader) bytes16() []byte { return r.bytesN(int(r.u16())) }
func (r *bodyReader) bytes32() []byte { return r.bytesN(int(r.u32())) }

func (r *bodyReader) done() error {
	if r.bad || len(r.buf) != 0 {
		return ErrIncompleteResponse
	}
	return nil
}

func checkRoutingKey(k string) error {
	if len(k) > maxRoutingKey {
		return fmt.Errorf("flo: routing key is %d bytes, at most %d", len(k), maxRoutingKey)
	}
	return nil
}

func checkTTL(ttl uint64) error {
	if ttl > MaxTTLMs {
		return fmt.Errorf("flo: TTL %d ms, at most %d", ttl, MaxTTLMs)
	}
	return nil
}

// absent reads a not_found answer: true when the reason is not_found
// itself. Any other not-found (no such transaction) is an error.
func absent(resp *rawResponse) (bool, error) {
	if resp.Status != StatusNotFound {
		return false, nil
	}
	err := newServerError(resp.Status, resp.Data)
	if se, ok := err.(*ServerError); !ok || se.Reason != ReasonNotFound {
		return false, err
	}
	return true, nil
}

// absentNow is absent for a read that reports a missing key as nil: a
// halted shard's not-found is ErrStaleNotFound instead, so it isn't read as
// the key being absent now.
func absentNow(resp *rawResponse) (bool, error) {
	gone, err := absent(resp)
	if gone && resp.Stale {
		return true, fmt.Errorf("%w: %w", ErrStaleNotFound, newServerError(resp.Status, resp.Data))
	}
	return gone, err
}

// encodeGetBody codes kv.get's request: wait_ms, wait_for, routing_key, txn.
func encodeGetBody(opts *GetOptions, txn *uint64) ([]byte, error) {
	if opts.WaitMs > MaxWaitMS {
		return nil, fmt.Errorf("%w: WaitMs %d, at most %d", ErrBlockTooLong, opts.WaitMs, MaxWaitMS)
	}
	if opts.WaitFor > WaitForChange {
		return nil, fmt.Errorf("flo: unknown WaitFor %d", opts.WaitFor)
	}
	if err := checkRoutingKey(opts.RoutingKey); err != nil {
		return nil, err
	}
	w := bodyWriter{buf: make([]byte, 0, 32+len(opts.RoutingKey))}
	w.u8(1)
	w.u64(opts.WaitMs)
	w.u8(1)
	w.u8(uint8(opts.WaitFor))
	w.routingKey(opts.RoutingKey)
	w.optU64(txn)
	return w.buf, nil
}

// encodeSetBody codes kv.set's request: value, ttl_ms, condition,
// cas_version, routing_key, txn.
func encodeSetBody(value []byte, opts *PutOptions, txn *uint64) ([]byte, error) {
	if len(value) > MaxKVValueSize {
		return nil, fmt.Errorf("%w: %d bytes, at most %d", ErrValueTooLarge, len(value), MaxKVValueSize)
	}
	if opts.Condition > ConditionIfPresent {
		return nil, fmt.Errorf("flo: unknown Condition %d", opts.Condition)
	}
	if err := checkRoutingKey(opts.RoutingKey); err != nil {
		return nil, err
	}
	ttl := opts.TTLMs
	if ttl != nil && *ttl == 0 {
		ttl = nil
	}
	if ttl != nil {
		if err := checkTTL(*ttl); err != nil {
			return nil, err
		}
	}
	w := bodyWriter{buf: make([]byte, 0, 48+len(value)+len(opts.RoutingKey))}
	w.bytes32(value)
	w.optU64(ttl)
	w.u8(1)
	w.u8(uint8(opts.Condition))
	w.optU64(opts.CASVersion)
	w.routingKey(opts.RoutingKey)
	w.optU64(txn)
	return w.buf, nil
}

// decodeGetAnswer reads kv.get's answer: version, value, expiry.
func decodeGetAnswer(data []byte) (*GetResult, error) {
	r := bodyReader{buf: data}
	got := &GetResult{Version: r.u64(), Value: r.bytes32()}
	if r.present() {
		at := int64(r.u64())
		got.ExpiresAtMs = &at
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	return got, nil
}

// decodeSetAnswer reads kv.set's answer: its version, absent inside a
// transaction, where the write has none until it commits.
func decodeSetAnswer(data []byte) (PutResult, error) {
	r := bodyReader{buf: data}
	var got PutResult
	if r.present() {
		got.Version = r.u64()
	}
	return got, r.done()
}

// encodeDeleteBody codes kv.delete's request: cas_version, routing_key, txn.
func encodeDeleteBody(cas *uint64, routingKey string, txn *uint64) ([]byte, error) {
	if err := checkRoutingKey(routingKey); err != nil {
		return nil, err
	}
	w := bodyWriter{buf: make([]byte, 0, 24+len(routingKey))}
	w.optU64(cas)
	w.routingKey(routingKey)
	w.optU64(txn)
	return w.buf, nil
}

// decodeDeleteAnswer reads kv.delete's answer: whether the key existed,
// nil inside a transaction, where the delete hasn't happened yet.
func decodeDeleteAnswer(data []byte) (*bool, error) {
	r := bodyReader{buf: data}
	var existed *bool
	if r.present() {
		v := r.bool()
		existed = &v
	}
	return existed, r.done()
}

// encodeScanBody codes kv.scan's request: limit, cursor.
func encodeScanBody(limit uint32, cursor []byte) ([]byte, error) {
	if limit < 1 || limit > MaxScanLimit {
		return nil, fmt.Errorf("flo: scan limit %d, want 1..%d", limit, MaxScanLimit)
	}
	if len(cursor) > maxScanCursor {
		return nil, fmt.Errorf("flo: scan cursor is %d bytes, at most %d", len(cursor), maxScanCursor)
	}
	w := bodyWriter{buf: make([]byte, 0, 6+len(cursor))}
	w.u8(1)
	w.u32(limit)
	w.u8(uint8(len(cursor)))
	w.buf = append(w.buf, cursor...)
	return w.buf, nil
}

// decodeScanAnswer reads kv.scan's answer: names, and the next page's
// cursor, nil on the last page.
func decodeScanAnswer(data []byte) (*ScanResult, error) {
	r := bodyReader{buf: data}
	n := int(r.u16())
	if n > MaxScanLimit {
		return nil, ErrIncompleteResponse
	}
	keys := make([]string, 0, n)
	for i := 0; i < n && !r.bad; i++ {
		name := r.bytes16()
		if !utf8.Valid(name) {
			r.bad = true
		}
		keys = append(keys, string(name))
	}
	got := &ScanResult{Keys: keys}
	if r.present() {
		got.Cursor = r.bytes8()
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	return got, nil
}

// encodeHistoryBody codes kv.history's request: limit, routing_key.
func encodeHistoryBody(limit uint32, before *uint64, routingKey string) ([]byte, error) {
	if limit < 1 || limit > MaxHistoryLimit {
		return nil, fmt.Errorf("flo: history limit %d, want 1..%d", limit, MaxHistoryLimit)
	}
	if err := checkRoutingKey(routingKey); err != nil {
		return nil, err
	}
	w := bodyWriter{buf: make([]byte, 0, 17+len(routingKey))}
	w.u8(1)
	w.u32(limit)
	w.optU64(before)
	w.routingKey(routingKey)
	return w.buf, nil
}

// decodeHistoryAnswer reads kv.history's answer: versions, newest first,
// and whether older ones follow.
func decodeHistoryAnswer(data []byte) (*HistoryResult, error) {
	r := bodyReader{buf: data}
	n := int(r.u8())
	if n > MaxHistoryLimit {
		return nil, ErrIncompleteResponse
	}
	out := &HistoryResult{Versions: make([]VersionEntry, 0, n)}
	for i := 0; i < n && !r.bad; i++ {
		out.Versions = append(out.Versions, VersionEntry{Version: r.u64(), AtMs: int64(r.u64()), Deleted: r.bool(), Value: r.bytes32()})
	}
	out.More = r.bool()
	if err := r.done(); err != nil {
		return nil, err
	}
	return out, nil
}

// checkMGetKeys refuses a key list one MGet can't take.
func checkMGetKeys(keys []string) error {
	if len(keys) > MaxMGetKeys {
		return fmt.Errorf("flo: mget of %d keys, at most %d", len(keys), MaxMGetKeys)
	}
	for _, k := range keys {
		if len(k) > maxMGetKey {
			return fmt.Errorf("%w: mget key of %d bytes, at most %d", ErrKeyTooLarge, len(k), maxMGetKey)
		}
	}
	return nil
}

// mgetBatch is how many of keys, from the first, fit one kv.mget request.
// Keys must have passed checkMGetKeys, so at least one always fits.
func mgetBatch(keys []string) int {
	size := 0
	for i, k := range keys {
		size += 2 + len(k)
		if size > maxMGetKeyBytes {
			return i
		}
	}
	return len(keys)
}

// encodeMGetBody codes kv.mget's request: keys, routing_key.
func encodeMGetBody(keys []string, routingKey string) ([]byte, error) {
	if err := checkMGetKeys(keys); err != nil {
		return nil, err
	}
	if err := checkRoutingKey(routingKey); err != nil {
		return nil, err
	}
	size := 0
	for _, k := range keys {
		size += 2 + len(k)
	}
	if size > maxMGetKeyBytes {
		return nil, fmt.Errorf("flo: mget keys of %d bytes, at most %d", size, maxMGetKeyBytes)
	}
	w := bodyWriter{buf: make([]byte, 0, 2+size+3+len(routingKey))}
	w.u16(uint16(len(keys)))
	for _, k := range keys {
		w.bytes16([]byte(k))
	}
	w.routingKey(routingKey)
	return w.buf, nil
}

// mgetAnswer is kv.mget's answer: entries for the leading keys, and next,
// the index of the first key not answered when the answer filled up.
type mgetAnswer struct {
	entries []MGetEntry
	next    *uint16
}

func decodeMGetAnswer(data []byte) (mgetAnswer, error) {
	r := bodyReader{buf: data}
	n := int(r.u16())
	if n > MaxMGetKeys {
		return mgetAnswer{}, ErrIncompleteResponse
	}
	got := mgetAnswer{entries: make([]MGetEntry, 0, n)}
	for i := 0; i < n && !r.bad; i++ {
		e := MGetEntry{Found: r.bool(), Version: r.u64(), Value: r.bytes32()}
		if !e.Found {
			e.Value = nil
		}
		got.entries = append(got.entries, e)
	}
	if r.present() {
		next := r.u16()
		got.next = &next
	}
	if err := r.done(); err != nil {
		return mgetAnswer{}, err
	}
	return got, nil
}

// encodeIncrBody codes kv.incr's request: delta, routing_key, txn.
func encodeIncrBody(delta int64, routingKey string, txn *uint64) ([]byte, error) {
	if err := checkRoutingKey(routingKey); err != nil {
		return nil, err
	}
	w := bodyWriter{buf: make([]byte, 0, 24+len(routingKey))}
	w.u8(1)
	w.u64(uint64(delta))
	w.routingKey(routingKey)
	w.optU64(txn)
	return w.buf, nil
}

// decodeIncrAnswer reads kv.incr's answer: version and value, both absent
// inside a transaction, where the increment hasn't happened yet.
func decodeIncrAnswer(data []byte) (*IncrResult, error) {
	r := bodyReader{buf: data}
	var version *uint64
	if r.present() {
		v := r.u64()
		version = &v
	}
	var value *int64
	if r.present() {
		v := int64(r.u64())
		value = &v
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	if version == nil || value == nil {
		if version != nil || value != nil {
			return nil, ErrIncompleteResponse
		}
		return nil, nil
	}
	return &IncrResult{Version: *version, Value: *value}, nil
}

// encodeExpireBody codes kv.expire's request: ttl_ms, cas_version,
// routing_key, txn.
func encodeExpireBody(ttl uint64, cas *uint64, routingKey string, txn *uint64) ([]byte, error) {
	if err := checkTTL(ttl); err != nil {
		return nil, err
	}
	if err := checkRoutingKey(routingKey); err != nil {
		return nil, err
	}
	w := bodyWriter{buf: make([]byte, 0, 32+len(routingKey))}
	w.u64(ttl)
	w.optU64(cas)
	w.routingKey(routingKey)
	w.optU64(txn)
	return w.buf, nil
}

// encodeExistsBody codes kv.exists's request: routing_key, txn.
func encodeExistsBody(routingKey string, txn *uint64) ([]byte, error) {
	if err := checkRoutingKey(routingKey); err != nil {
		return nil, err
	}
	w := bodyWriter{buf: make([]byte, 0, 16+len(routingKey))}
	w.routingKey(routingKey)
	w.optU64(txn)
	return w.buf, nil
}

// decodeExistsAnswer reads kv.exists's answer: exists, version.
func decodeExistsAnswer(data []byte) (ExistsResult, error) {
	r := bodyReader{buf: data}
	exists := r.bool()
	r.u64()
	return ExistsResult{Exists: exists}, r.done()
}

func jsonPath(path string) (string, error) {
	if path == "" {
		path = "$"
	}
	if len(path) > MaxJSONPathSize {
		return "", fmt.Errorf("flo: JSON path is %d bytes, at most %d", len(path), MaxJSONPathSize)
	}
	if !utf8.ValidString(path) {
		return "", errors.New("flo: JSON path is not UTF-8")
	}
	return path, nil
}

// encodeJSONPathBody codes kv.json_get's and kv.json_del's request: path,
// routing_key.
func encodeJSONPathBody(path, routingKey string) ([]byte, error) {
	path, err := jsonPath(path)
	if err != nil {
		return nil, err
	}
	if err := checkRoutingKey(routingKey); err != nil {
		return nil, err
	}
	w := bodyWriter{buf: make([]byte, 0, 6+len(path)+len(routingKey))}
	w.u8(1)
	w.bytes16([]byte(path))
	w.routingKey(routingKey)
	return w.buf, nil
}

// encodeJSONSetBody codes kv.json_set's request: path, document,
// routing_key.
func encodeJSONSetBody(path string, document []byte, routingKey string) ([]byte, error) {
	path, err := jsonPath(path)
	if err != nil {
		return nil, err
	}
	if len(document) > MaxJSONDocumentSize {
		return nil, fmt.Errorf("%w: JSON document of %d bytes, at most %d", ErrValueTooLarge, len(document), MaxJSONDocumentSize)
	}
	if err := checkRoutingKey(routingKey); err != nil {
		return nil, err
	}
	w := bodyWriter{buf: make([]byte, 0, 10+len(path)+len(document)+len(routingKey))}
	w.u8(1)
	w.bytes16([]byte(path))
	w.bytes32(document)
	w.routingKey(routingKey)
	return w.buf, nil
}

// decodeJSONGetAnswer reads kv.json_get's answer: version, document.
func decodeJSONGetAnswer(data []byte) (*GetResult, error) {
	r := bodyReader{buf: data}
	got := &GetResult{Version: r.u64(), Value: r.bytes32()}
	if err := r.done(); err != nil {
		return nil, err
	}
	return got, nil
}

// decodeJSONSetAnswer reads kv.json_set's answer: version.
func decodeJSONSetAnswer(data []byte) (*PutResult, error) {
	r := bodyReader{buf: data}
	got := &PutResult{Version: r.u64()}
	if err := r.done(); err != nil {
		return nil, err
	}
	return got, nil
}

// decodeJSONDelAnswer reads kv.json_del's answer: the new version, nil
// when the path was the root, which deleted the key.
func decodeJSONDelAnswer(data []byte) (*PutResult, error) {
	r := bodyReader{buf: data}
	var got *PutResult
	if r.present() {
		got = &PutResult{Version: r.u64()}
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	return got, nil
}

// encodeTxnBody codes kv.txn_commit's and kv.txn_rollback's request: txn.
func encodeTxnBody(txn uint64) []byte {
	return binary.LittleEndian.AppendUint64(nil, txn)
}

// decodeBeginAnswer reads kv.txn_begin's answer: txn, pinned.
func decodeBeginAnswer(data []byte) (txn, pinned uint64, err error) {
	r := bodyReader{buf: data}
	txn, pinned = r.u64(), r.u64()
	return txn, pinned, r.done()
}

// decodeCommitAnswer reads kv.txn_commit's answer: commit_index, ops.
func decodeCommitAnswer(data []byte) (*KVCommitResult, error) {
	r := bodyReader{buf: data}
	got := &KVCommitResult{CommitIndex: r.u64(), OpCount: r.u16()}
	if err := r.done(); err != nil {
		return nil, err
	}
	return got, nil
}

// decodeEmptyAnswer reads an answer with no fields.
func decodeEmptyAnswer(data []byte) error {
	if len(data) != 0 {
		return ErrIncompleteResponse
	}
	return nil
}
