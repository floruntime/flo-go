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
// Rollback is called. Every key written or read inside the transaction must
// hash to the same partition as the routing key passed to Begin; otherwise
// the server refuses it with ReasonWrongRoutingKey.
//
// Caps (server-enforced):
//   - 256 ops per transaction
//   - 1 MiB total payload across buffered writes
//
// The following operations are NOT supported inside a transaction and return
// ErrTxnUnsupportedOp without a server round-trip: Scan, MGet, JsonGet,
// JsonSet, JsonDel, History.
//
// A KVTransaction is not safe for concurrent use.
type KVTransaction struct {
	client     *Client
	namespace  string
	routingKey string
	id         uint64
	pinnedHash uint64
	done       bool
}

// ID returns the server-assigned transaction id.
func (t *KVTransaction) ID() uint64 { return t.id }

// PinnedHash returns the partition hash this transaction is bound to.
func (t *KVTransaction) PinnedHash() uint64 { return t.pinnedHash }

// Begin opens a new per-shard transaction pinned to the partition that
// owns routingKey, which must not be empty. Subsequent operations through
// the returned KVTransaction must use keys that hash to the same partition.
// A shard holding as many open transactions as it may returns a
// ServerError with ReasonTooManyTransactions.
func (kv *KVClient) Begin(routingKey string, opts *KVBeginOptions) (*KVTransaction, error) {
	if opts == nil {
		opts = &KVBeginOptions{}
	}
	if routingKey == "" {
		return nil, errors.New("flo: a transaction needs a routing key")
	}
	if err := checkRoutingKey(routingKey); err != nil {
		return nil, err
	}
	namespace := kv.client.getNamespace(opts.Namespace)
	resp, err := kv.client.sendAndCheck(OpKVBeginTxn, namespace, []byte(routingKey), nil, nil, false)
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
		routingKey: routingKey,
		id:         id,
		pinnedHash: pinned,
	}, nil
}

// Put buffers a put inside the transaction. Its options work as they do
// for KVClient.Put; the routing key is the transaction's.
func (t *KVTransaction) Put(key string, value []byte, opts *PutOptions) (PutResult, error) {
	if t.done {
		return PutResult{}, errTxnFinished
	}
	o := PutOptions{}
	if opts != nil {
		o = *opts
	}
	o.RoutingKey = t.routingKey
	return t.client.kvSet(t.namespace, key, value, &o, &t.id)
}

// Get reads a key inside the transaction (sees the transaction's own buffered
// writes). Returns nil if the key is not found.
func (t *KVTransaction) Get(key string) (*GetResult, error) {
	if t.done {
		return nil, errTxnFinished
	}
	return t.client.kvGet(t.namespace, key, &GetOptions{RoutingKey: t.routingKey}, &t.id)
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
	body, err := encodeDeleteBody(opts.CASVersion, t.routingKey, &t.id)
	if err != nil {
		return err
	}
	resp, err := t.client.sendAndCheck(OpKVDelete, t.namespace, []byte(key), body, nil, false)
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
	body, err := encodeIncrBody(delta, t.routingKey, &t.id)
	if err != nil {
		return err
	}
	resp, err := t.client.sendAndCheck(OpKVIncr, t.namespace, []byte(key), body, nil, false)
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
	body, err := encodeExpireBody(ttlMs, opts.CASVersion, t.routingKey, &t.id)
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
	return t.client.kvExists(t.namespace, key, t.routingKey, &t.id)
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

// Commit atomically applies all buffered operations in the transaction. After
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
