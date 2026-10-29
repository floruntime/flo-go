package flo

import (
	"fmt"
)

// KVClient provides KV operations for a Flo client.
type KVClient struct {
	client *Client
}

// Get retrieves the value and version for a key. It returns nil if the
// key is not found, also when a wait (WaitMs) ends without it.
func (kv *KVClient) Get(key string, opts *GetOptions) (*GetResult, error) {
	if opts == nil {
		opts = &GetOptions{}
	}
	return kv.client.kvGet(kv.client.getNamespace(opts.Namespace), key, opts, nil)
}

func (c *Client) kvGet(namespace, key string, opts *GetOptions, txn *uint64) (*GetResult, error) {
	body, err := encodeGetBody(opts, txn)
	if err != nil {
		return nil, err
	}
	resp, err := c.sendAndCheck(OpKVGet, namespace, []byte(key), body, nil, true)
	if err != nil {
		return nil, err
	}
	if gone, err := absent(resp); gone || err != nil {
		return nil, err
	}
	got, err := decodeGetAnswer(resp.Data)
	if err != nil {
		return nil, err
	}
	got.Stale = resp.Stale
	return got, nil
}

// Put sets a key-value pair and returns the new version assigned by the
// server. A CASVersion that isn't the key's, or a Condition that doesn't
// hold, returns ErrConflict with ReasonCASMismatch or ReasonConditionFailed.
func (kv *KVClient) Put(key string, value []byte, opts *PutOptions) (PutResult, error) {
	if opts == nil {
		opts = &PutOptions{}
	}
	return kv.client.kvSet(kv.client.getNamespace(opts.Namespace), key, value, opts, nil)
}

func (c *Client) kvSet(namespace, key string, value []byte, opts *PutOptions, txn *uint64) (PutResult, error) {
	body, err := encodeSetBody(value, opts, txn)
	if err != nil {
		return PutResult{}, err
	}
	resp, err := c.sendAndCheck(OpKVPut, namespace, []byte(key), body, nil, false)
	if err != nil {
		return PutResult{}, err
	}
	return decodeSetAnswer(resp.Data)
}

// MaxMGetKeys is the most keys one MGet takes.
const MaxMGetKeys = 256

// MGet looks up many keys and returns one entry per requested key, in the
// same order. Keys may live on different shards; with a RoutingKey they
// are all looked up on its shard. Keys that don't fit one
// request frame are sent in more than one request, and when the server's
// answer fills up before every key in a request is answered, MGet asks
// again from the first key left.
func (kv *KVClient) MGet(keys []string, opts *KVMGetOptions) ([]MGetEntry, error) {
	if len(keys) == 0 {
		return []MGetEntry{}, nil
	}
	if opts == nil {
		opts = &KVMGetOptions{}
	}
	if err := checkMGetKeys(keys); err != nil {
		return nil, err
	}
	if err := checkRoutingKey(opts.RoutingKey); err != nil {
		return nil, err
	}
	namespace := kv.client.getNamespace(opts.Namespace)
	out := make([]MGetEntry, 0, len(keys))
	for len(out) < len(keys) {
		rest := keys[len(out):]
		rest = rest[:mgetBatch(rest)]
		body, err := encodeMGetBody(rest, opts.RoutingKey)
		if err != nil {
			return nil, err
		}
		resp, err := kv.client.sendAndCheck(OpKVMGet, namespace, nil, body, nil, false)
		if err != nil {
			return nil, err
		}
		got, err := decodeMGetAnswer(resp.Data)
		if err != nil {
			return nil, err
		}
		// A partial answer must make progress and say where it stopped;
		// a whole one answers every key asked.
		n := len(got.entries)
		if got.next == nil && n != len(rest) || got.next != nil && (n == 0 || int(*got.next) != n) {
			return nil, fmt.Errorf("%w: mget answered %d of %d keys", ErrIncompleteResponse, n, len(rest))
		}
		for i, e := range got.entries {
			e.Key = rest[i]
			e.Stale = resp.Stale
			out = append(out, e)
		}
	}
	return out, nil
}

// Delete removes a key and reports whether it existed; deleting a missing
// key is not an error. A CASVersion that isn't the key's returns
// ErrConflict with ReasonCASMismatch.
func (kv *KVClient) Delete(key string, opts *DeleteOptions) (bool, error) {
	if opts == nil {
		opts = &DeleteOptions{}
	}
	body, err := encodeDeleteBody(opts.CASVersion, opts.RoutingKey, nil)
	if err != nil {
		return false, err
	}
	resp, err := kv.client.sendAndCheck(OpKVDelete, kv.client.getNamespace(opts.Namespace), []byte(key), body, nil, false)
	if err != nil {
		return false, err
	}
	existed, err := decodeDeleteAnswer(resp.Data)
	if err != nil {
		return false, err
	}
	if existed == nil {
		// Only a transaction's delete leaves it unknown.
		return false, ErrIncompleteResponse
	}
	return *existed, nil
}

// Scan returns one page of the key names that start with prefix ("" for
// all). Pass the result's Cursor in the next call's ScanOptions to read the
// next page; it is nil on the last page.
func (kv *KVClient) Scan(prefix string, opts *ScanOptions) (*ScanResult, error) {
	if opts == nil {
		opts = &ScanOptions{}
	}
	limit := uint32(MaxScanLimit)
	if opts.Limit != nil {
		limit = *opts.Limit
	}
	body, err := encodeScanBody(limit, opts.Cursor)
	if err != nil {
		return nil, err
	}
	resp, err := kv.client.sendAndCheck(OpKVScan, kv.client.getNamespace(opts.Namespace), []byte(prefix), body, nil, false)
	if err != nil {
		return nil, err
	}
	got, err := decodeScanAnswer(resp.Data)
	if err != nil {
		return nil, err
	}
	got.Stale = resp.Stale
	return got, nil
}

// History returns a page of a key's versions, newest first: Limit of them
// (default 10, at most MaxHistoryLimit), older than BeforeVersion when it is
// set. When the result's More is set, older versions follow: pass the last
// version's number as the next call's BeforeVersion. A missing key has none.
func (kv *KVClient) History(key string, opts *HistoryOptions) (*HistoryResult, error) {
	if opts == nil {
		opts = &HistoryOptions{}
	}
	limit := uint32(defaultHistoryLimit)
	if opts.Limit != nil {
		limit = *opts.Limit
	}
	body, err := encodeHistoryBody(limit, opts.BeforeVersion, opts.RoutingKey)
	if err != nil {
		return nil, err
	}
	resp, err := kv.client.sendAndCheck(OpKVHistory, kv.client.getNamespace(opts.Namespace), []byte(key), body, nil, true)
	if err != nil {
		return nil, err
	}
	if gone, err := absent(resp); gone || err != nil {
		if err != nil {
			return nil, err
		}
		return &HistoryResult{Versions: []VersionEntry{}, Stale: resp.Stale}, nil
	}
	got, err := decodeHistoryAnswer(resp.Data)
	if err != nil {
		return nil, err
	}
	got.Stale = resp.Stale
	return got, nil
}

// KVIncrOptions contains options for KV Incr operations.
type KVIncrOptions struct {
	Namespace string
	// Delta is the amount to add (may be negative). Defaults to +1 when nil.
	Delta *int64
	// RoutingKey routes the request in place of the key.
	RoutingKey string
}

// Incr atomically adds Delta (default +1) to the i64 counter stored at key
// and returns its new value and version. The first Incr on a missing key
// creates it at the delta. A key holding something other than a counter
// returns a ServerError with ReasonWrongType.
func (kv *KVClient) Incr(key string, opts *KVIncrOptions) (IncrResult, error) {
	if opts == nil {
		opts = &KVIncrOptions{}
	}
	delta := int64(1)
	if opts.Delta != nil {
		delta = *opts.Delta
	}
	body, err := encodeIncrBody(delta, opts.RoutingKey, nil)
	if err != nil {
		return IncrResult{}, err
	}
	resp, err := kv.client.sendAndCheck(OpKVIncr, kv.client.getNamespace(opts.Namespace), []byte(key), body, nil, false)
	if err != nil {
		return IncrResult{}, err
	}
	got, err := decodeIncrAnswer(resp.Data)
	if err != nil {
		return IncrResult{}, err
	}
	if got == nil {
		// Only a transaction's increment leaves them unknown.
		return IncrResult{}, ErrIncompleteResponse
	}
	return *got, nil
}

// KVExpireOptions contains options for KV Expire operations.
type KVExpireOptions struct {
	Namespace string
	// CASVersion, when set, must be the key's version: only the owner
	// extends a lease.
	CASVersion *uint64
	// RoutingKey routes the request in place of the key.
	RoutingKey string
}

// Expire sets an existing key's TTL to ttlMs milliseconds (at most
// MaxTTLMs); 0 clears the TTL, making the key permanent. A missing key
// returns ErrNotFound.
func (kv *KVClient) Expire(key string, ttlMs uint64, opts *KVExpireOptions) error {
	if opts == nil {
		opts = &KVExpireOptions{}
	}
	body, err := encodeExpireBody(ttlMs, opts.CASVersion, opts.RoutingKey, nil)
	if err != nil {
		return err
	}
	resp, err := kv.client.sendAndCheck(OpKVExpire, kv.client.getNamespace(opts.Namespace), []byte(key), body, nil, false)
	if err != nil {
		return err
	}
	return decodeEmptyAnswer(resp.Data)
}

// KVExistsOptions contains options for KV Exists operations.
type KVExistsOptions struct {
	Namespace string
	// RoutingKey routes the request in place of the key.
	RoutingKey string
}

// Exists reports whether the key is present, without transferring its
// value.
func (kv *KVClient) Exists(key string, opts *KVExistsOptions) (ExistsResult, error) {
	if opts == nil {
		opts = &KVExistsOptions{}
	}
	return kv.client.kvExists(kv.client.getNamespace(opts.Namespace), key, opts.RoutingKey, nil)
}

func (c *Client) kvExists(namespace, key, routingKey string, txn *uint64) (ExistsResult, error) {
	body, err := encodeExistsBody(routingKey, txn)
	if err != nil {
		return ExistsResult{}, err
	}
	resp, err := c.sendAndCheck(OpKVExists, namespace, []byte(key), body, nil, false)
	if err != nil {
		return ExistsResult{}, err
	}
	got, err := decodeExistsAnswer(resp.Data)
	if err != nil {
		return ExistsResult{}, err
	}
	got.Stale = resp.Stale
	return got, nil
}

// KVJsonOptions contains options for JSON operations.
type KVJsonOptions struct {
	Namespace string
	// RoutingKey routes the request in place of the key.
	RoutingKey string
}

// JsonGet reads the value at a JSONPath ("" means the root, "$") inside
// the JSON document stored at key. It returns nil if the key or path is not
// found. The result carries the document's version, for a CAS Put.
func (kv *KVClient) JsonGet(key, path string, opts *KVJsonOptions) (*GetResult, error) {
	if opts == nil {
		opts = &KVJsonOptions{}
	}
	body, err := encodeJSONPathBody(path, opts.RoutingKey)
	if err != nil {
		return nil, err
	}
	resp, err := kv.client.sendAndCheck(OpKVJsonGet, kv.client.getNamespace(opts.Namespace), []byte(key), body, nil, true)
	if err != nil {
		return nil, err
	}
	if gone, err := absent(resp); gone || err != nil {
		return nil, err
	}
	got, err := decodeJSONGetAnswer(resp.Data)
	if err != nil {
		return nil, err
	}
	got.Stale = resp.Stale
	return got, nil
}

// JsonSet writes jsonValue at path ("" means the root, "$") inside the JSON
// document stored at key and returns the document's new version. The root
// replaces the whole document, creating the key if missing; a sub-path
// needs the key to exist, else ErrNotFound.
func (kv *KVClient) JsonSet(key, path string, jsonValue []byte, opts *KVJsonOptions) (*PutResult, error) {
	if opts == nil {
		opts = &KVJsonOptions{}
	}
	body, err := encodeJSONSetBody(path, jsonValue, opts.RoutingKey)
	if err != nil {
		return nil, err
	}
	resp, err := kv.client.sendAndCheck(OpKVJsonSet, kv.client.getNamespace(opts.Namespace), []byte(key), body, nil, false)
	if err != nil {
		return nil, err
	}
	return decodeJSONSetAnswer(resp.Data)
}

// JsonDel removes the value at path ("" means the root, "$") from the JSON
// document stored at key and returns the document's new version. Deleting
// the root deletes the key, and returns nil. A missing key or path returns
// ErrNotFound.
func (kv *KVClient) JsonDel(key, path string, opts *KVJsonOptions) (*PutResult, error) {
	if opts == nil {
		opts = &KVJsonOptions{}
	}
	body, err := encodeJSONPathBody(path, opts.RoutingKey)
	if err != nil {
		return nil, err
	}
	resp, err := kv.client.sendAndCheck(OpKVJsonDel, kv.client.getNamespace(opts.Namespace), []byte(key), body, nil, false)
	if err != nil {
		return nil, err
	}
	return decodeJSONDelAnswer(resp.Data)
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
