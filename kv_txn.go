package flo

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// ErrTxnUnsupportedOp is returned when an operation is not allowed inside a
// per-shard KV transaction (e.g. Scan, MGet, Json*, History).
var ErrTxnUnsupportedOp = errors.New("operation not supported inside a KV transaction")

// KVBeginOptions contains options for KV.Begin.
type KVBeginOptions struct {
	Namespace string
}

// KVTransaction is a per-shard KV transaction handle. All operations on the
// transaction are buffered on the server's pinned shard until Commit or
// Rollback is called. Every key written or read inside the transaction must
// hash to the same partition as the routing key passed to Begin; otherwise
// the server returns a "kv_txn_cross_shard" error.
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
// owns routingKey. Subsequent operations through the returned KVTransaction
// must use keys that hash to the same partition.
func (kv *KVClient) Begin(routingKey string, opts *KVBeginOptions) (*KVTransaction, error) {
	if opts == nil {
		opts = &KVBeginOptions{}
	}
	namespace := kv.client.getNamespace(opts.Namespace)

	builder := NewOptionsBuilder()
	if routingKey != "" {
		builder.AddBytes(OptRoutingKey, []byte(routingKey))
	}
	resp, err := kv.client.sendAndCheck(OpKVBeginTxn, namespace, []byte(routingKey), nil, builder.Build(), false)
	if err != nil {
		return nil, err
	}
	// Wire body: [variant:u8=0][txn_id:u64 LE][pinned_hash:u64 LE]
	if len(resp.Data) < 17 {
		return nil, fmt.Errorf("flo: short KV begin reply (%d bytes)", len(resp.Data))
	}
	return &KVTransaction{
		client:     kv.client,
		namespace:  namespace,
		routingKey: routingKey,
		id:         binary.LittleEndian.Uint64(resp.Data[1:9]),
		pinnedHash: binary.LittleEndian.Uint64(resp.Data[9:17]),
	}, nil
}

// txnOptionsBuilder returns a builder pre-populated with the routing-key and
// txn-id TLVs required for every op inside the transaction.
func (t *KVTransaction) txnOptionsBuilder() *OptionsBuilder {
	b := NewOptionsBuilder()
	if t.routingKey != "" {
		b.AddBytes(OptRoutingKey, []byte(t.routingKey))
	}
	b.AddU64(OptTxnID, t.id)
	return b
}

// Put buffers a put inside the transaction. CAS / NX / XX / TTL options are
// supported the same way as KVClient.Put.
func (t *KVTransaction) Put(key string, value []byte, opts *PutOptions) (PutResult, error) {
	if t.done {
		return PutResult{}, fmt.Errorf("flo: transaction already finished")
	}
	if opts == nil {
		opts = &PutOptions{}
	}
	builder := t.txnOptionsBuilder()
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
	resp, err := t.client.sendAndCheck(OpKVPut, t.namespace, []byte(key), value, builder.Build(), true)
	if err != nil {
		return PutResult{}, err
	}
	if len(resp.Data) < 8 {
		return PutResult{Version: 0}, nil
	}
	return PutResult{Version: binary.LittleEndian.Uint64(resp.Data[0:8])}, nil
}

// Get reads a key inside the transaction (sees the transaction's own buffered
// writes). Returns nil if the key is not found.
func (t *KVTransaction) Get(key string) (*GetResult, error) {
	if t.done {
		return nil, fmt.Errorf("flo: transaction already finished")
	}
	builder := t.txnOptionsBuilder()
	resp, err := t.client.sendAndCheck(OpKVGet, t.namespace, []byte(key), nil, builder.Build(), true)
	if err != nil {
		return nil, err
	}
	if resp.Status == StatusNotFound {
		return nil, nil
	}
	if len(resp.Data) < 8 {
		return &GetResult{Value: nil, Version: 0}, nil
	}
	return &GetResult{
		Value:   resp.Data[8:],
		Version: binary.LittleEndian.Uint64(resp.Data[0:8]),
	}, nil
}

// Delete buffers a delete inside the transaction.
func (t *KVTransaction) Delete(key string) error {
	if t.done {
		return fmt.Errorf("flo: transaction already finished")
	}
	builder := t.txnOptionsBuilder()
	_, err := t.client.sendAndCheck(OpKVDelete, t.namespace, []byte(key), nil, builder.Build(), true)
	return err
}

// Incr buffers an atomic counter increment inside the transaction.
func (t *KVTransaction) Incr(key string, delta int64) (int64, error) {
	if t.done {
		return 0, fmt.Errorf("flo: transaction already finished")
	}
	builder := t.txnOptionsBuilder()
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, uint64(delta))
	resp, err := t.client.sendAndCheck(OpKVIncr, t.namespace, []byte(key), value, builder.Build(), true)
	if err != nil {
		return 0, err
	}
	if len(resp.Data) < 8 {
		return 0, fmt.Errorf("flo: short Incr reply")
	}
	return int64(binary.LittleEndian.Uint64(resp.Data[0:8])), nil
}

// Touch updates the TTL of an existing key inside the transaction.
func (t *KVTransaction) Touch(key string, ttlSeconds uint64) error {
	if t.done {
		return fmt.Errorf("flo: transaction already finished")
	}
	builder := t.txnOptionsBuilder()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], ttlSeconds)
	_, err := t.client.sendAndCheck(OpKVTouch, t.namespace, []byte(key), buf[:], builder.Build(), true)
	return err
}

// Persist removes the TTL on a key inside the transaction.
func (t *KVTransaction) Persist(key string) error {
	if t.done {
		return fmt.Errorf("flo: transaction already finished")
	}
	builder := t.txnOptionsBuilder()
	_, err := t.client.sendAndCheck(OpKVPersist, t.namespace, []byte(key), nil, builder.Build(), true)
	return err
}

// Exists checks key existence inside the transaction.
func (t *KVTransaction) Exists(key string) (bool, error) {
	if t.done {
		return false, fmt.Errorf("flo: transaction already finished")
	}
	builder := t.txnOptionsBuilder()
	resp, err := t.client.sendAndCheck(OpKVExists, t.namespace, []byte(key), nil, builder.Build(), true)
	if err != nil {
		return false, err
	}
	// Wire body: [version:u64 LE][1 byte 0/1]
	if len(resp.Data) < 9 {
		return false, nil
	}
	return resp.Data[8] == 1, nil
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
func (t *KVTransaction) History(key string, opts *HistoryOptions) ([]VersionEntry, error) {
	return nil, ErrTxnUnsupportedOp
}

// Commit atomically applies all buffered operations in the transaction. After
// Commit returns (success or error) the transaction is closed and further
// operations return an error.
func (t *KVTransaction) Commit() (*KVCommitResult, error) {
	if t.done {
		return nil, fmt.Errorf("flo: transaction already finished")
	}
	t.done = true
	builder := NewOptionsBuilder()
	if t.routingKey != "" {
		builder.AddBytes(OptRoutingKey, []byte(t.routingKey))
	}
	builder.AddU64(OptTxnID, t.id)
	resp, err := t.client.sendAndCheck(OpKVCommitTxn, t.namespace, []byte(t.routingKey), nil, builder.Build(), false)
	if err != nil {
		return nil, err
	}
	// Wire body: [variant:u8=1][commit_index:u64 LE][op_count:u16 LE]
	if len(resp.Data) < 11 {
		return nil, fmt.Errorf("flo: short KV commit reply (%d bytes)", len(resp.Data))
	}
	return &KVCommitResult{
		CommitIndex: binary.LittleEndian.Uint64(resp.Data[1:9]),
		OpCount:     binary.LittleEndian.Uint16(resp.Data[9:11]),
	}, nil
}

// Rollback discards the buffered operations without committing. Idempotent:
// calling Rollback after Commit (or vice versa) returns nil.
func (t *KVTransaction) Rollback() error {
	if t.done {
		return nil
	}
	t.done = true
	builder := NewOptionsBuilder()
	if t.routingKey != "" {
		builder.AddBytes(OptRoutingKey, []byte(t.routingKey))
	}
	builder.AddU64(OptTxnID, t.id)
	_, err := t.client.sendAndCheck(OpKVRollbackTxn, t.namespace, []byte(t.routingKey), nil, builder.Build(), false)
	return err
}
