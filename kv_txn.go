package flo

import (
	"errors"
	"fmt"
)

// ErrTxnUnsupportedOp is returned when an operation is not allowed inside a
// per-shard KV transaction (e.g. Scan, MGet, Json*, History).
var ErrTxnUnsupportedOp = errors.New("operation not supported inside a KV transaction")

// errTxnFinished is returned by an op on a committed or rolled-back
// transaction.
var errTxnFinished = errors.New("flo: transaction already finished")

// KVBeginOptions contains options for KV.Begin.
type KVBeginOptions struct {
	Namespace string
}

// KVTxnCASOptions contains options for a transaction's Delete and Expire.
type KVTxnCASOptions struct {
	// CASVersion, when set, must be the key's version when the op applies.
	CASVersion *uint64
}

// KVTransaction is a per-shard KV transaction handle. All operations on the
// transaction are buffered on the server's pinned shard until Commit or
// Rollback is called. A key routes by its {tag}, the part between its first
// '{' and the '}' after it, or by the whole key when it has none. Every key
// written or read inside the transaction must route with the key passed to
// Begin, so give them all the same {tag}: "{user:1}:balance" and
// "{user:1}:history". A key that routes elsewhere is refused with
// ReasonKeysNotColocated (bad request, not run).
//
// The server caps a transaction at 64 ops and 256 KiB of keys and values
// unless its operator sets otherwise; an op past either is refused with
// ReasonTxnTooLarge.
//
// A Condition, CASVersion or Incr is decided when the commit applies, not
// when its op is buffered: a buffered op that won't hold is accepted, and
// Commit is refused, applying none of the transaction: ErrConflict with
// ReasonCASMismatch or ReasonConditionFailed, or ErrBadRequest for an Incr
// of a value that isn't a counter or that would overflow.
//
// The following operations are NOT supported inside a transaction and return
// ErrTxnUnsupportedOp without a server round-trip: Scan, MGet, JsonGet,
// JsonSet, JsonDel, History.
//
// A KVTransaction is not safe for concurrent use.
type KVTransaction struct {
	client     *Client
	namespace  string
	id         uint64
	pinnedHash uint64
	done       bool
}

// ID returns the server-assigned transaction id.
func (t *KVTransaction) ID() uint64 { return t.id }

// PinnedHash returns the partition hash this transaction is bound to.
func (t *KVTransaction) PinnedHash() uint64 { return t.pinnedHash }

// Begin opens a new per-shard transaction pinned to key's {tag}, or to the
// whole key when it has none; key must not be empty. Every key the
// transaction then names must route with it. A shard holding as many open
// transactions as it may returns a ServerError with
// ReasonTooManyTransactions.
func (kv *KVClient) Begin(key string, opts *KVBeginOptions) (*KVTransaction, error) {
	if opts == nil {
		opts = &KVBeginOptions{}
	}
	if key == "" {
		return nil, errors.New("flo: a transaction is begun with a key")
	}
	namespace := kv.client.getNamespace(opts.Namespace)
	resp, err := kv.client.sendAndCheck(OpKVBeginTxn, namespace, []byte(key), nil, nil, false)
	if err != nil {
		return nil, err
	}
	id, pinned, err := decodeBeginAnswer(resp.Data)
	if err != nil {
		return nil, err
	}
	return &KVTransaction{
		client:     kv.client,
		namespace:  namespace,
		id:         id,
		pinnedHash: pinned,
	}, nil
}

// Put buffers a put inside the transaction. Its options work as they do
// for KVClient.Put.
func (t *KVTransaction) Put(key string, value []byte, opts *PutOptions) (PutResult, error) {
	if t.done {
		return PutResult{}, errTxnFinished
	}
	o := PutOptions{}
	if opts != nil {
		o = *opts
	}
	return t.client.kvSet(t.namespace, key, value, &o, &t.id)
}

// Get reads a key inside the transaction (sees the transaction's own buffered
// writes). Returns nil if the key is not found.
func (t *KVTransaction) Get(key string) (*GetResult, error) {
	if t.done {
		return nil, errTxnFinished
	}
	return t.client.kvGet(t.namespace, key, &GetOptions{}, &t.id)
}

// Delete buffers a delete inside the transaction. Whether the key existed
// isn't known until the transaction commits.
func (t *KVTransaction) Delete(key string, opts *KVTxnCASOptions) error {
	if t.done {
		return errTxnFinished
	}
	if opts == nil {
		opts = &KVTxnCASOptions{}
	}
	resp, err := t.client.sendAndCheck(OpKVDelete, t.namespace, []byte(key), encodeDeleteBody(opts.CASVersion, &t.id), nil, false)
	if err != nil {
		return err
	}
	_, err = decodeDeleteAnswer(resp.Data)
	return err
}

// Incr buffers an atomic counter increment inside the transaction. The new
// value isn't known until the transaction commits.
func (t *KVTransaction) Incr(key string, delta int64) error {
	if t.done {
		return errTxnFinished
	}
	resp, err := t.client.sendAndCheck(OpKVIncr, t.namespace, []byte(key), encodeIncrBody(delta, &t.id), nil, false)
	if err != nil {
		return err
	}
	_, err = decodeIncrAnswer(resp.Data)
	return err
}

// Expire buffers setting an existing key's TTL to ttlMs milliseconds inside
// the transaction; 0 clears the TTL.
func (t *KVTransaction) Expire(key string, ttlMs uint64, opts *KVTxnCASOptions) error {
	if t.done {
		return errTxnFinished
	}
	if opts == nil {
		opts = &KVTxnCASOptions{}
	}
	body, err := encodeExpireBody(ttlMs, opts.CASVersion, &t.id)
	if err != nil {
		return err
	}
	resp, err := t.client.sendAndCheck(OpKVExpire, t.namespace, []byte(key), body, nil, false)
	if err != nil {
		return err
	}
	return decodeEmptyAnswer(resp.Data)
}

// Exists checks key existence inside the transaction, seeing its own
// buffered writes.
func (t *KVTransaction) Exists(key string) (ExistsResult, error) {
	if t.done {
		return ExistsResult{}, errTxnFinished
	}
	return t.client.kvExists(t.namespace, key, &t.id)
}

// Scan is not supported inside a transaction.
func (t *KVTransaction) Scan(prefix string, opts *ScanOptions) (*ScanResult, error) {
	return nil, ErrTxnUnsupportedOp
}

// MGet is not supported inside a transaction.
func (t *KVTransaction) MGet(keys []string, opts *KVMGetOptions) ([]MGetEntry, error) {
	return nil, ErrTxnUnsupportedOp
}

// JsonGet is not supported inside a transaction.
func (t *KVTransaction) JsonGet(key, path string, opts *KVJsonOptions) (*GetResult, error) {
	return nil, ErrTxnUnsupportedOp
}

// JsonSet is not supported inside a transaction.
func (t *KVTransaction) JsonSet(key, path string, jsonValue []byte, opts *KVJsonOptions) (*PutResult, error) {
	return nil, ErrTxnUnsupportedOp
}

// JsonDel is not supported inside a transaction.
func (t *KVTransaction) JsonDel(key, path string, opts *KVJsonOptions) (*PutResult, error) {
	return nil, ErrTxnUnsupportedOp
}

// History is not supported inside a transaction.
func (t *KVTransaction) History(key string, opts *HistoryOptions) (*HistoryResult, error) {
	return nil, ErrTxnUnsupportedOp
}

// Commit atomically applies all buffered operations in the transaction, or,
// when one of their conditions doesn't hold, none of them. After
// Commit returns (success or error) the transaction is closed and further
// operations return an error.
func (t *KVTransaction) Commit() (*KVCommitResult, error) {
	if t.done {
		return nil, errTxnFinished
	}
	t.done = true
	resp, err := t.client.sendAndCheck(OpKVCommitTxn, t.namespace, nil, encodeTxnBody(t.id), nil, false)
	if err != nil {
		return nil, err
	}
	got, err := decodeCommitAnswer(resp.Data)
	if err != nil {
		return nil, fmt.Errorf("flo: KV commit answer: %w", err)
	}
	return got, nil
}

// Rollback discards the buffered operations without committing. Idempotent:
// calling Rollback after Commit (or vice versa) returns nil.
func (t *KVTransaction) Rollback() error {
	if t.done {
		return nil
	}
	t.done = true
	resp, err := t.client.sendAndCheck(OpKVRollbackTxn, t.namespace, nil, encodeTxnBody(t.id), nil, false)
	if err != nil {
		return err
	}
	return decodeEmptyAnswer(resp.Data)
}
