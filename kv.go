package flo

import "encoding/binary"

// KVClient provides KV operations for a Flo client.
type KVClient struct {
	client *Client
}

// Get retrieves the value for a key.
// Returns nil if the key is not found.
// Use BlockMS option for long polling (wait for key to appear).
func (kv *KVClient) Get(key string, opts *GetOptions) ([]byte, error) {
	if opts == nil {
		opts = &GetOptions{}
	}
	namespace := kv.client.getNamespace(opts.Namespace)

	// Build TLV options
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

	if len(resp.Data) == 0 {
		return nil, nil
	}

	return resp.Data, nil
}

// Put sets a key-value pair.
func (kv *KVClient) Put(key string, value []byte, opts *PutOptions) error {
	if opts == nil {
		opts = &PutOptions{}
	}
	namespace := kv.client.getNamespace(opts.Namespace)

	// Build TLV options
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

	_, err := kv.client.sendAndCheck(OpKVPut, namespace, []byte(key), value, builder.Build(), false)
	return err
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

// Helper functions for creating option pointers

// Uint32Ptr returns a pointer to the given uint32 value.
func Uint32Ptr(v uint32) *uint32 {
	return &v
}

// Uint64Ptr returns a pointer to the given uint64 value.
func Uint64Ptr(v uint64) *uint64 {
	return &v
}
