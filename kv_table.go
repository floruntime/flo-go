package flo

import (
	"encoding/binary"
	"fmt"
)

// kv.get and kv.set are table ops: the request is [namespace][key][body],
// the body coded by the op's schema, field by field in order. An optional
// field is a presence byte then its value; a field with a default is sent
// present. A byte string has a length prefix as wide as its bound needs.

// isTableOp reports whether op's request carries a schema-coded body
// rather than a value and options.
func isTableOp(op OpCode) bool {
	return op == OpKVGet || op == OpKVPut
}

// MaxKVValueSize is the longest value kv.set takes.
const MaxKVValueSize = 253817

// maxRoutingKey is the longest routing key; its length prefix is a u16.
const maxRoutingKey = 4096

type bodyWriter struct{ buf []byte }

func (w *bodyWriter) u8(v uint8) { w.buf = append(w.buf, v) }
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
func (w *bodyWriter) routingKey(k string) {
	if k == "" {
		w.u8(0)
		return
	}
	w.u8(1)
	w.buf = binary.LittleEndian.AppendUint16(w.buf, uint16(len(k)))
	w.buf = append(w.buf, k...)
}

func checkRoutingKey(k string) error {
	if len(k) > maxRoutingKey {
		return fmt.Errorf("flo: routing key is %d bytes, at most %d", len(k), maxRoutingKey)
	}
	return nil
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
	w := bodyWriter{buf: make([]byte, 0, 48+len(value)+len(opts.RoutingKey))}
	w.buf = binary.LittleEndian.AppendUint32(w.buf, uint32(len(value)))
	w.buf = append(w.buf, value...)
	ttl := opts.TTLMs
	if ttl != nil && *ttl == 0 {
		ttl = nil
	}
	w.optU64(ttl)
	w.u8(1)
	w.u8(uint8(opts.Condition))
	w.optU64(opts.CASVersion)
	w.routingKey(opts.RoutingKey)
	w.optU64(txn)
	return w.buf, nil
}

// decodeGetAnswer reads kv.get's answer: [version:u64][value_len:u32][value].
func decodeGetAnswer(data []byte) (*GetResult, error) {
	if len(data) < 12 {
		return nil, ErrIncompleteResponse
	}
	n := binary.LittleEndian.Uint32(data[8:12])
	if uint64(len(data)) != 12+uint64(n) {
		return nil, ErrIncompleteResponse
	}
	value := make([]byte, n)
	copy(value, data[12:])
	return &GetResult{Version: binary.LittleEndian.Uint64(data[0:8]), Value: value}, nil
}

// decodeSetAnswer reads kv.set's answer: its version, absent inside a
// transaction, where the write has none until it commits.
func decodeSetAnswer(data []byte) (PutResult, error) {
	switch {
	case len(data) == 1 && data[0] == 0:
		return PutResult{}, nil
	case len(data) == 9 && data[0] == 1:
		return PutResult{Version: binary.LittleEndian.Uint64(data[1:9])}, nil
	}
	return PutResult{}, ErrIncompleteResponse
}
