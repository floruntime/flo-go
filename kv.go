package flo

import (
	"encoding/binary"
	"errors"
)

// KVClient provides KV operations for a Flo client.
type KVClient struct {
	client *Client
}

// Get retrieves the value and version for a key.
// Returns nil if the key is not found.
// Use BlockMS option for long polling (wait for key to appear).
func (kv *KVClient) Get(key string, opts *GetOptions) (*GetResult, error) {
	if opts == nil {
		opts = &GetOptions{}
	}
	namespace := kv.client.getNamespace(opts.Namespace)

	var options []byte
	if opts.BlockMS != nil {
		builder := NewOptionsBuilder()
		builder.AddU32(OptBlockMS, *opts.BlockMS)
		options = builder.Build()
	}

	resp, err := kv.client.sendAndCheck(OpKVGet, namespace, []byte(key), nil, options, true)
	if err != nil {
		return nil, err
	}

	if resp.Status == StatusNotFound {
		return nil, nil
	}

	// Wire body: [version:u64 LE][value bytes]. An empty body means a key with
	// an empty value at version 0 (e.g. a freshly written empty PUT) — this is
	// distinct from not-found, which surfaces as StatusNotFound above.
	if len(resp.Data) < 8 {
		return &GetResult{Value: nil, Version: 0}, nil
	}
	version := binary.LittleEndian.Uint64(resp.Data[0:8])
	value := resp.Data[8:]
	return &GetResult{Value: value, Version: version}, nil
}

// Put sets a key-value pair and returns the new version assigned by the server.
// On a CAS mismatch the call returns ErrConflict; on a precondition failure
// (IfNotExists / IfExists) it also returns ErrConflict.
func (kv *KVClient) Put(key string, value []byte, opts *PutOptions) (PutResult, error) {
	if opts == nil {
		opts = &PutOptions{}
	}
	namespace := kv.client.getNamespace(opts.Namespace)

	builder := NewOptionsBuilder()

	if opts.TTLSeconds != nil {
		builder.AddU64(OptTTLSeconds, *opts.TTLSeconds)
	}
	if opts.CASVersion != nil {
		builder.AddU64(OptCASVersion, *opts.CASVersion)
	}
	if opts.IfNotExists {
		builder.AddFlag(OptIfNotExists)
	}
	if opts.IfExists {
		builder.AddFlag(OptIfExists)
	}

	resp, err := kv.client.sendAndCheck(OpKVPut, namespace, []byte(key), value, builder.Build(), true)
	if err != nil {
		return PutResult{}, err
	}

	// Wire body: [version:u64 LE]. Older servers may return an empty body —
	// in that case Version is left at 0 and callers that need CAS must read it
	// back via History.
	if len(resp.Data) < 8 {
		return PutResult{Version: 0}, nil
	}
	return PutResult{Version: binary.LittleEndian.Uint64(resp.Data[0:8])}, nil
}

// MaxMGetKeys is the server-enforced limit on keys per MGet request.
const MaxMGetKeys = 256

// MGet looks up many keys in a single round trip. Keys may live on different
// shards — the server gathers results in parallel and returns one entry per
// requested key in the same order. Returns at most MaxMGetKeys entries.
func (kv *KVClient) MGet(keys []string, opts *KVMGetOptions) ([]MGetEntry, error) {
	if len(keys) == 0 {
		return []MGetEntry{}, nil
	}
	if len(keys) > MaxMGetKeys {
		return nil, errors.New("flo: mget: too many keys (max 256)")
	}
	if opts == nil {
		opts = &KVMGetOptions{}
	}
	namespace := kv.client.getNamespace(opts.Namespace)

	// Pack request: [count:u16 LE]([key_len:u16 LE][key])*
	size := 2
	for _, k := range keys {
		if len(k) > 0xFFFF {
			return nil, errors.New("flo: mget: key too long")
		}
		size += 2 + len(k)
	}
	value := make([]byte, size)
	binary.LittleEndian.PutUint16(value[0:2], uint16(len(keys)))
	off := 2
	for _, k := range keys {
		binary.LittleEndian.PutUint16(value[off:off+2], uint16(len(k)))
		off += 2
		copy(value[off:], k)
		off += len(k)
	}

	resp, err := kv.client.sendAndCheck(OpKVMGet, namespace, nil, value, nil, true)
	if err != nil {
		return nil, err
	}

	// Response: [count:u32]([status:u8][key_len:u16][key][version:u64][value_len:u32][value])*
	if len(resp.Data) < 4 {
		return nil, ErrIncompleteResponse
	}
	count := binary.LittleEndian.Uint32(resp.Data[0:4])
	out := make([]MGetEntry, 0, count)
	off2 := 4
	for i := uint32(0); i < count; i++ {
		if off2+1+2 > len(resp.Data) {
			return nil, ErrIncompleteResponse
		}
		status := resp.Data[off2]
		off2 += 1
		klen := int(binary.LittleEndian.Uint16(resp.Data[off2 : off2+2]))
		off2 += 2
		if off2+klen+8+4 > len(resp.Data) {
			return nil, ErrIncompleteResponse
		}
		key := string(resp.Data[off2 : off2+klen])
		off2 += klen
		version := binary.LittleEndian.Uint64(resp.Data[off2 : off2+8])
		off2 += 8
		vlen := int(binary.LittleEndian.Uint32(resp.Data[off2 : off2+4]))
		off2 += 4
		if off2+vlen > len(resp.Data) {
			return nil, ErrIncompleteResponse
		}
		var val []byte
		if vlen > 0 {
			val = make([]byte, vlen)
			copy(val, resp.Data[off2:off2+vlen])
		}
		off2 += vlen
		out = append(out, MGetEntry{
			Key:     key,
			Value:   val,
			Version: version,
			Found:   status == 0,
		})
	}
	return out, nil
}

// Delete removes a key.
// This operation succeeds even if the key doesn't exist.
func (kv *KVClient) Delete(key string, opts *DeleteOptions) error {
	if opts == nil {
		opts = &DeleteOptions{}
	}
	namespace := kv.client.getNamespace(opts.Namespace)

	// Delete succeeds for both OK and NOT_FOUND
	_, err := kv.client.sendAndCheck(OpKVDelete, namespace, []byte(key), nil, nil, true)
	return err
}

// Scan retrieves keys with a prefix.
func (kv *KVClient) Scan(prefix string, opts *ScanOptions) (*ScanResult, error) {
	if opts == nil {
		opts = &ScanOptions{}
	}
	namespace := kv.client.getNamespace(opts.Namespace)

	// Build TLV options (keys_only only — limit is in value now)
	builder := NewOptionsBuilder()

	if opts.KeysOnly {
		builder.AddU8(OptKeysOnly, 1)
	}

	// Value: [limit:u32][cursor...]
	limit := uint32(0) // 0 = server default
	if opts.Limit != nil {
		limit = *opts.Limit
	}
	cursor := opts.Cursor
	value := make([]byte, 4+len(cursor))
	binary.LittleEndian.PutUint32(value[0:4], limit)
	if len(cursor) > 0 {
		copy(value[4:], cursor)
	}

	resp, err := kv.client.sendAndCheck(OpKVScan, namespace, []byte(prefix), value, builder.Build(), false)
	if err != nil {
		return nil, err
	}

	return parseScanResponse(resp.Data)
}

// History retrieves the version history for a key.
func (kv *KVClient) History(key string, opts *HistoryOptions) ([]VersionEntry, error) {
	if opts == nil {
		opts = &HistoryOptions{}
	}
	namespace := kv.client.getNamespace(opts.Namespace)

	// Build TLV options
	builder := NewOptionsBuilder()

	if opts.Limit != nil {
		builder.AddU32(OptLimit, *opts.Limit)
	}

	resp, err := kv.client.sendAndCheck(OpKVHistory, namespace, []byte(key), nil, builder.Build(), false)
	if err != nil {
		return nil, err
	}

	return parseHistoryResponse(resp.Data)
}

// KVIncrOptions contains options for KV Incr operations.
type KVIncrOptions struct {
	Namespace string
	// Delta is the amount to add (may be negative). Defaults to +1 when nil.
	Delta *int64
}

// Incr atomically increments the i64 counter stored at key by Delta (default +1)
// and returns the new value. The first Incr on a missing key creates it at the
// delta value. Returns ErrBadRequest if the key already holds a non-counter value.
func (kv *KVClient) Incr(key string, opts *KVIncrOptions) (int64, error) {
	if opts == nil {
		opts = &KVIncrOptions{}
	}
	namespace := kv.client.getNamespace(opts.Namespace)

	var value []byte
	if opts.Delta != nil {
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], uint64(*opts.Delta))
		value = buf[:]
	}

	resp, err := kv.client.sendAndCheck(OpKVIncr, namespace, []byte(key), value, nil, true)
	if err != nil {
		return 0, err
	}
	// Wire body: [version:u64][counter:i64 LE]
	if len(resp.Data) < 16 {
		return 0, ErrIncompleteResponse
	}
	return int64(binary.LittleEndian.Uint64(resp.Data[8:16])), nil
}

// KVTouchOptions contains options for KV Touch / Persist operations.
type KVTouchOptions struct {
	Namespace string
}

// Touch updates the TTL on an existing key. ttlSeconds=0 clears the TTL
// (equivalent to Persist). Returns ErrNotFound if the key does not exist.
func (kv *KVClient) Touch(key string, ttlSeconds uint64, opts *KVTouchOptions) error {
	if opts == nil {
		opts = &KVTouchOptions{}
	}
	namespace := kv.client.getNamespace(opts.Namespace)
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], ttlSeconds)
	_, err := kv.client.sendAndCheck(OpKVTouch, namespace, []byte(key), buf[:], nil, true)
	return err
}

// Persist clears the TTL on an existing key, making it permanent.
// Returns ErrNotFound if the key does not exist.
func (kv *KVClient) Persist(key string, opts *KVTouchOptions) error {
	if opts == nil {
		opts = &KVTouchOptions{}
	}
	namespace := kv.client.getNamespace(opts.Namespace)
	_, err := kv.client.sendAndCheck(OpKVPersist, namespace, []byte(key), nil, nil, true)
	return err
}

// KVExistsOptions contains options for KV Exists operations.
type KVExistsOptions struct {
	Namespace string
}

// Exists returns true if the key is present in the store. Unlike Get, this
// avoids transferring the value over the wire.
func (kv *KVClient) Exists(key string, opts *KVExistsOptions) (bool, error) {
	if opts == nil {
		opts = &KVExistsOptions{}
	}
	namespace := kv.client.getNamespace(opts.Namespace)
	resp, err := kv.client.sendAndCheck(OpKVExists, namespace, []byte(key), nil, nil, true)
	if err != nil {
		return false, err
	}
	// Wire body: [version:u64][1 byte 0/1]
	if len(resp.Data) < 9 {
		return false, ErrIncompleteResponse
	}
	return resp.Data[8] == 1, nil
}

// KVJsonOptions contains options for JSON.* operations.
type KVJsonOptions struct {
	Namespace string
}

// JsonGet extracts a value at the given JSONPath from the JSON document stored
// at key. An empty path is treated as the document root ("$"). Returns nil if
// the key or path is not found. The returned GetResult carries the document's
// current version (suitable for follow-up CAS via Put/JsonSet).
func (kv *KVClient) JsonGet(key, path string, opts *KVJsonOptions) (*GetResult, error) {
	if opts == nil {
		opts = &KVJsonOptions{}
	}
	namespace := kv.client.getNamespace(opts.Namespace)
	resp, err := kv.client.sendAndCheck(OpKVJsonGet, namespace, []byte(key), []byte(path), nil, true)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if resp.Status == StatusNotFound {
		return nil, nil
	}
	// Wire body: [version:u64 LE][json bytes]
	if len(resp.Data) < 8 {
		return nil, nil
	}
	return &GetResult{
		Value:   resp.Data[8:],
		Version: binary.LittleEndian.Uint64(resp.Data[:8]),
	}, nil
}

// JsonSet writes jsonValue at path inside the JSON document stored at key.
// Path "$" replaces the whole document (and creates the key if missing).
// Sub-paths require the key to already exist; otherwise ErrNotFound is returned.
// The returned PutResult carries the new document version.
func (kv *KVClient) JsonSet(key, path string, jsonValue []byte, opts *KVJsonOptions) (*PutResult, error) {
	if opts == nil {
		opts = &KVJsonOptions{}
	}
	namespace := kv.client.getNamespace(opts.Namespace)
	if path == "" {
		path = "$"
	}
	if len(path) > 0xFFFF {
		return nil, ErrBadRequest
	}
	value := make([]byte, 2+len(path)+len(jsonValue))
	binary.LittleEndian.PutUint16(value[0:2], uint16(len(path)))
	copy(value[2:2+len(path)], path)
	copy(value[2+len(path):], jsonValue)
	resp, err := kv.client.sendAndCheck(OpKVJsonSet, namespace, []byte(key), value, nil, true)
	if err != nil {
		return nil, err
	}
	// Wire body: [version:u64 LE]
	var version uint64
	if len(resp.Data) >= 8 {
		version = binary.LittleEndian.Uint64(resp.Data[:8])
	}
	return &PutResult{Version: version}, nil
}

// JsonDel removes the value at path from the JSON document stored at key.
// Path "$" deletes the whole key (and the returned PutResult carries no
// meaningful version, since the key is gone). Sub-path deletes return the new
// document version. Returns ErrNotFound for a missing key/path.
func (kv *KVClient) JsonDel(key, path string, opts *KVJsonOptions) (*PutResult, error) {
	if opts == nil {
		opts = &KVJsonOptions{}
	}
	namespace := kv.client.getNamespace(opts.Namespace)
	if path == "" {
		path = "$"
	}
	resp, err := kv.client.sendAndCheck(OpKVJsonDel, namespace, []byte(key), []byte(path), nil, true)
	if err != nil {
		return nil, err
	}
	var version uint64
	if len(resp.Data) >= 8 {
		version = binary.LittleEndian.Uint64(resp.Data[:8])
	}
	return &PutResult{Version: version}, nil
}

// Helper functions for creating option pointers

// Uint32Ptr returns a pointer to the given uint32 value.
func Uint32Ptr(v uint32) *uint32 {
	return &v
}

// Uint64Ptr returns a pointer to the given uint64 value.
func Uint64Ptr(v uint64) *uint64 {
	return &v
}

// Int64Ptr returns a pointer to the given int64 value.
func Int64Ptr(v int64) *int64 {
	return &v
}
