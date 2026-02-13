package flo

import (
	"encoding/binary"
	"fmt"
)

// StreamClient provides stream operations.
type StreamClient struct {
	client *Client
}

// NewStreamClient creates a new stream client.
func (c *Client) Stream() *StreamClient {
	return &StreamClient{client: c}
}

// Append appends a record to a stream.
func (s *StreamClient) Append(stream string, payload []byte, opts *StreamAppendOptions) (*StreamAppendResult, error) {
	if opts == nil {
		opts = &StreamAppendOptions{}
	}

	namespace := s.client.getNamespace(opts.Namespace)

	resp, err := s.client.sendAndCheck(OpStreamAppend, namespace, []byte(stream), payload, nil, true)
	if err != nil {
		return nil, err
	}

	// Parse response: [sequence:u64][timestamp_ms:i64]
	if len(resp.Data) < 16 {
		return nil, fmt.Errorf("incomplete stream append response")
	}

	return &StreamAppendResult{
		Sequence:    binary.LittleEndian.Uint64(resp.Data[0:8]),
		TimestampMs: int64(binary.LittleEndian.Uint64(resp.Data[8:16])),
	}, nil
}

// Read reads records from a stream.
func (s *StreamClient) Read(stream string, opts *StreamReadOptions) (*StreamReadResult, error) {
	if opts == nil {
		opts = &StreamReadOptions{}
	}

	namespace := s.client.getNamespace(opts.Namespace)

	// Build TLV options
	builder := NewOptionsBuilder()

	// Tail mode flag (mutually exclusive with Start)
	if opts.Tail {
		builder.AddFlag(OptStreamTail)
	}

	// Start StreamID (16 bytes)
	if opts.Start != nil {
		builder.AddBytes(OptStreamStart, opts.Start.ToBytes())
	}

	// End StreamID (16 bytes)
	if opts.End != nil {
		builder.AddBytes(OptStreamEnd, opts.End.ToBytes())
	}

	// Explicit partition
	if opts.Partition != nil {
		builder.AddU32(OptPartition, *opts.Partition)
	}

	if opts.Count != nil {
		builder.AddU32(OptCount, *opts.Count)
	}

	if opts.BlockMS != nil {
		builder.AddU32(OptBlockMS, *opts.BlockMS)
	}

	resp, err := s.client.sendAndCheck(OpStreamRead, namespace, []byte(stream), nil, builder.Build(), true)
	if err != nil {
		return nil, err
	}

	return parseStreamReadResponse(resp.Data)
}

// Info gets stream metadata.
func (s *StreamClient) Info(stream string, opts *StreamInfoOptions) (*StreamInfo, error) {
	if opts == nil {
		opts = &StreamInfoOptions{}
	}

	namespace := s.client.getNamespace(opts.Namespace)

	resp, err := s.client.sendAndCheck(OpStreamInfo, namespace, []byte(stream), nil, nil, true)
	if err != nil {
		return nil, err
	}

	// Parse response: [first_seq:u64][last_seq:u64][count:u64][bytes:u64]
	if len(resp.Data) < 32 {
		return nil, fmt.Errorf("incomplete stream info response")
	}

	return &StreamInfo{
		FirstSeq: binary.LittleEndian.Uint64(resp.Data[0:8]),
		LastSeq:  binary.LittleEndian.Uint64(resp.Data[8:16]),
		Count:    binary.LittleEndian.Uint64(resp.Data[16:24]),
		Bytes:    binary.LittleEndian.Uint64(resp.Data[24:32]),
	}, nil
}

// Trim trims a stream.
func (s *StreamClient) Trim(stream string, opts *StreamTrimOptions) error {
	if opts == nil {
		opts = &StreamTrimOptions{}
	}

	namespace := s.client.getNamespace(opts.Namespace)

	builder := NewOptionsBuilder()

	if opts.MaxLen != nil {
		builder.AddU64(OptRetentionCount, *opts.MaxLen)
	}

	if opts.MaxAgeSeconds != nil {
		builder.AddU64(OptRetentionAge, *opts.MaxAgeSeconds)
	}

	if opts.MaxBytes != nil {
		builder.AddU64(OptRetentionBytes, *opts.MaxBytes)
	}

	if opts.DryRun {
		builder.AddFlag(OptDryRun)
	}

	_, err := s.client.sendAndCheck(OpStreamTrim, namespace, []byte(stream), nil, builder.Build(), true)
	return err
}

// GroupJoin joins a consumer group.
func (s *StreamClient) GroupJoin(stream, group, consumer string, opts *StreamGroupJoinOptions) error {
	if opts == nil {
		opts = &StreamGroupJoinOptions{}
	}

	namespace := s.client.getNamespace(opts.Namespace)

	// Encode group and consumer in value: [group_len:u16][group][consumer_len:u16][consumer]
	value := make([]byte, 2+len(group)+2+len(consumer))
	offset := 0

	binary.LittleEndian.PutUint16(value[offset:], uint16(len(group)))
	offset += 2
	copy(value[offset:], group)
	offset += len(group)

	binary.LittleEndian.PutUint16(value[offset:], uint16(len(consumer)))
	offset += 2
	copy(value[offset:], consumer)

	_, err := s.client.sendAndCheck(OpStreamGroupJoin, namespace, []byte(stream), value, nil, true)
	return err
}

// GroupLeave leaves a consumer group.
func (s *StreamClient) GroupLeave(stream, group, consumer string, opts *StreamGroupJoinOptions) error {
	if opts == nil {
		opts = &StreamGroupJoinOptions{}
	}

	namespace := s.client.getNamespace(opts.Namespace)

	// Same encoding as GroupJoin
	value := make([]byte, 2+len(group)+2+len(consumer))
	offset := 0

	binary.LittleEndian.PutUint16(value[offset:], uint16(len(group)))
	offset += 2
	copy(value[offset:], group)
	offset += len(group)

	binary.LittleEndian.PutUint16(value[offset:], uint16(len(consumer)))
	offset += 2
	copy(value[offset:], consumer)

	_, err := s.client.sendAndCheck(OpStreamGroupLeave, namespace, []byte(stream), value, nil, true)
	return err
}

// GroupRead reads from a consumer group.
func (s *StreamClient) GroupRead(stream, group, consumer string, opts *StreamGroupReadOptions) (*StreamReadResult, error) {
	if opts == nil {
		opts = &StreamGroupReadOptions{}
	}

	namespace := s.client.getNamespace(opts.Namespace)

	// Encode group and consumer in value
	value := make([]byte, 2+len(group)+2+len(consumer))
	offset := 0

	binary.LittleEndian.PutUint16(value[offset:], uint16(len(group)))
	offset += 2
	copy(value[offset:], group)
	offset += len(group)

	binary.LittleEndian.PutUint16(value[offset:], uint16(len(consumer)))
	offset += 2
	copy(value[offset:], consumer)

	builder := NewOptionsBuilder()

	if opts.Count != nil {
		builder.AddU32(OptCount, *opts.Count)
	}

	if opts.BlockMS != nil {
		builder.AddU32(OptBlockMS, *opts.BlockMS)
	}

	resp, err := s.client.sendAndCheck(OpStreamGroupRead, namespace, []byte(stream), value, builder.Build(), true)
	if err != nil {
		return nil, err
	}

	return parseStreamReadResponse(resp.Data)
}

// GroupAck acknowledges records in a consumer group.
func (s *StreamClient) GroupAck(stream, group string, seqs []uint64, opts *StreamGroupAckOptions) error {
	if opts == nil {
		opts = &StreamGroupAckOptions{}
	}

	namespace := s.client.getNamespace(opts.Namespace)

	// Encode group and seqs in value: [group_len:u16][group][count:u32][seq:u64]*
	value := make([]byte, 2+len(group)+4+len(seqs)*8)
	offset := 0

	binary.LittleEndian.PutUint16(value[offset:], uint16(len(group)))
	offset += 2
	copy(value[offset:], group)
	offset += len(group)

	binary.LittleEndian.PutUint32(value[offset:], uint32(len(seqs)))
	offset += 4

	for _, seq := range seqs {
		binary.LittleEndian.PutUint64(value[offset:], seq)
		offset += 8
	}

	_, err := s.client.sendAndCheck(OpStreamGroupAck, namespace, []byte(stream), value, nil, true)
	return err
}

// parseStreamReadResponse parses a stream read response.
// Wire format: [count:u32]([sequence:u64][timestamp_ms:i64][tier:u8][partition:u32][key_present:u8][payload_len:u32][payload][header_count:u32])*
func parseStreamReadResponse(data []byte) (*StreamReadResult, error) {
	if len(data) < 4 {
		return &StreamReadResult{
			Records: []StreamRecord{},
		}, nil
	}

	pos := 0
	count := binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	records := make([]StreamRecord, 0, count)

	for i := uint32(0); i < count && pos < len(data); i++ {
		// Read sequence
		if pos+8 > len(data) {
			return nil, fmt.Errorf("incomplete stream record: missing sequence")
		}
		sequence := binary.LittleEndian.Uint64(data[pos:])
		pos += 8

		// Read timestamp_ms
		if pos+8 > len(data) {
			return nil, fmt.Errorf("incomplete stream record: missing timestamp_ms")
		}
		timestampMs := int64(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8

		// Read tier
		if pos+1 > len(data) {
			return nil, fmt.Errorf("incomplete stream record: missing tier")
		}
		tier := StorageTier(data[pos])
		pos += 1

		// Skip partition
		if pos+4 > len(data) {
			return nil, fmt.Errorf("incomplete stream record: missing partition")
		}
		pos += 4

		// Read key_present
		if pos+1 > len(data) {
			return nil, fmt.Errorf("incomplete stream record: missing key_present")
		}
		keyPresent := data[pos]
		pos += 1

		// Skip key if present
		if keyPresent != 0 {
			if pos+4 > len(data) {
				return nil, fmt.Errorf("incomplete stream record: missing key length")
			}
			keyLen := binary.LittleEndian.Uint32(data[pos:])
			pos += 4
			if pos+int(keyLen) > len(data) {
				return nil, fmt.Errorf("incomplete stream record: missing key data")
			}
			pos += int(keyLen)
		}

		// Read payload
		if pos+4 > len(data) {
			return nil, fmt.Errorf("incomplete stream record: missing payload length")
		}
		payloadLen := binary.LittleEndian.Uint32(data[pos:])
		pos += 4

		if pos+int(payloadLen) > len(data) {
			return nil, fmt.Errorf("incomplete stream record: missing payload data")
		}
		payload := make([]byte, payloadLen)
		copy(payload, data[pos:pos+int(payloadLen)])
		pos += int(payloadLen)

		// Skip header_count (TODO: parse headers)
		if pos+4 > len(data) {
			return nil, fmt.Errorf("incomplete stream record: missing header count")
		}
		pos += 4

		records = append(records, StreamRecord{
			Sequence:    sequence,
			TimestampMs: timestampMs,
			Tier:        tier,
			Payload:     payload,
			Headers:     nil,
		})
	}

	return &StreamReadResult{
		Records: records,
	}, nil
}
