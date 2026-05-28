package flo

import (
	"encoding/binary"
	"fmt"
	"math"
)

// StreamClient provides stream operations.
type StreamClient struct {
	client *Client
}

// buildStreamBatchValue frames a single record into the batch wire format the
// server's stream_append handler expects. A single append is a batch of one:
//
//	[record_count:u32=1][payload_len:u32][payload]
//	[header_count:u16]([key_len:u16][key][val_len:u16][val])*
func buildStreamBatchValue(payload []byte, headers map[string]string) []byte {
	size := 4 + 4 + len(payload) + 2
	for k, v := range headers {
		size += 2 + len(k) + 2 + len(v)
	}
	buf := make([]byte, 0, size)
	buf = binary.LittleEndian.AppendUint32(buf, 1) // record_count
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(payload)))
	buf = append(buf, payload...)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(headers)))
	for k, v := range headers {
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(k)))
		buf = append(buf, k...)
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(v)))
		buf = append(buf, v...)
	}
	return buf
}

// Append appends a record to a stream.
func (s *StreamClient) Append(stream string, payload []byte, opts *StreamAppendOptions) (*StreamAppendResult, error) {
	if opts == nil {
		opts = &StreamAppendOptions{}
	}

	namespace := s.client.getNamespace(opts.Namespace)

	// The server's stream_append expects the value batch-framed (a single
	// append is a batch of one record); raw payloads are stored but read back
	// as zero records.
	value := buildStreamBatchValue(payload, opts.Headers)

	resp, err := s.client.sendAndCheck(OpStreamAppend, namespace, []byte(stream), value, nil, true)
	if err != nil {
		return nil, err
	}

	// Parse response: [sequence:u64][timestamp_ms:i64]
	if len(resp.Data) < 16 {
		return nil, fmt.Errorf("incomplete stream append response")
	}

	return &StreamAppendResult{
		ID: StreamID{
			Sequence:    binary.LittleEndian.Uint64(resp.Data[0:8]),
			TimestampMS: binary.LittleEndian.Uint64(resp.Data[8:16]),
		},
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

	// Parse response: [first_ts:u64][first_seq:u64][last_ts:u64][last_seq:u64][count:u64][bytes:u64][partition_count:u32]
	if len(resp.Data) < 52 {
		return nil, fmt.Errorf("incomplete stream info response")
	}

	return &StreamInfo{
		FirstID: StreamID{
			TimestampMS: binary.LittleEndian.Uint64(resp.Data[0:8]),
			Sequence:    binary.LittleEndian.Uint64(resp.Data[8:16]),
		},
		LastID: StreamID{
			TimestampMS: binary.LittleEndian.Uint64(resp.Data[16:24]),
			Sequence:    binary.LittleEndian.Uint64(resp.Data[24:32]),
		},
		Count:          binary.LittleEndian.Uint64(resp.Data[32:40]),
		Bytes:          binary.LittleEndian.Uint64(resp.Data[40:48]),
		PartitionCount: binary.LittleEndian.Uint32(resp.Data[48:52]),
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

// GroupRead reads new records from a consumer group, advancing the group's
// last_delivered_id and adding the delivered records to the consumer's Pending
// Entry List (PEL) until acked.
//
// Crash recovery: GroupRead alone is NOT sufficient. It only returns records
// past last_delivered_id, so a record delivered-but-unacked at crash time is
// never re-surfaced by a subsequent GroupRead. To re-process in-flight work
// after a reconnect, drain the PEL with GroupClaim (StreamWorker does this
// automatically — see RedeliverPendingOnReconnect).
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

// GroupPending lists a consumer group's pending (delivered-but-unacked)
// entries. If consumer is non-empty, only that consumer's entries are returned;
// otherwise the whole group's PEL is returned. (FLO-102)
func (s *StreamClient) GroupPending(stream, group, consumer string, opts *StreamGroupReadOptions) ([]PendingEntry, error) {
	if opts == nil {
		opts = &StreamGroupReadOptions{}
	}
	namespace := s.client.getNamespace(opts.Namespace)

	// Wire: [group_len:u16][group]([consumer_len:u16][consumer])?
	size := 2 + len(group)
	if consumer != "" {
		size += 2 + len(consumer)
	}
	value := make([]byte, size)
	offset := 0
	binary.LittleEndian.PutUint16(value[offset:], uint16(len(group)))
	offset += 2
	copy(value[offset:], group)
	offset += len(group)
	if consumer != "" {
		binary.LittleEndian.PutUint16(value[offset:], uint16(len(consumer)))
		offset += 2
		copy(value[offset:], consumer)
	}

	resp, err := s.client.sendAndCheck(OpStreamGroupPending, namespace, []byte(stream), value, nil, true)
	if err != nil {
		return nil, err
	}
	return parsePendingEntries(resp.Data)
}

// GroupClaim claims a page of a consumer group's pending entries for `consumer`,
// scanning the PEL in StreamID order from `startID` and taking up to `count`
// entries idle for at least `minIdleMS`. Returns the claimed records (payload +
// headers) plus a cursor for the next page. (FLO-102)
//
//   - Drain own pending (reconnect): minIdleMS = 0, startID = StreamID{}.
//   - Steal from idle consumers (rebalance): minIdleMS > 0.
//
// Loop until result.Done to fully drain:
//
//	cursor := StreamID{}
//	for {
//	    r, err := c.Stream.GroupClaim(stream, group, consumer, 0, cursor, 100, nil)
//	    if err != nil { return err }
//	    for _, rec := range r.Records { process(rec) }
//	    if r.Done || len(r.Records) == 0 { break }
//	    cursor = r.NextCursor
//	}
func (s *StreamClient) GroupClaim(stream, group, consumer string, minIdleMS uint32, startID StreamID, count uint32, opts *StreamGroupReadOptions) (*StreamClaimResult, error) {
	if opts == nil {
		opts = &StreamGroupReadOptions{}
	}
	namespace := s.client.getNamespace(opts.Namespace)

	// Wire: [group_len:u16][group][consumer_len:u16][consumer]
	//       [min_idle_ms:u32][start_ts:u64][start_seq:u64][count:u32]
	value := make([]byte, 2+len(group)+2+len(consumer)+4+8+8+4)
	offset := 0
	binary.LittleEndian.PutUint16(value[offset:], uint16(len(group)))
	offset += 2
	copy(value[offset:], group)
	offset += len(group)
	binary.LittleEndian.PutUint16(value[offset:], uint16(len(consumer)))
	offset += 2
	copy(value[offset:], consumer)
	offset += len(consumer)
	binary.LittleEndian.PutUint32(value[offset:], minIdleMS)
	offset += 4
	binary.LittleEndian.PutUint64(value[offset:], startID.TimestampMS)
	offset += 8
	binary.LittleEndian.PutUint64(value[offset:], startID.Sequence)
	offset += 8
	binary.LittleEndian.PutUint32(value[offset:], count)

	resp, err := s.client.sendAndCheck(OpStreamGroupClaim, namespace, []byte(stream), value, nil, true)
	if err != nil {
		return nil, err
	}
	return parseClaimResponse(resp.Data)
}

// parseClaimResponse decodes a stream_group_claim response:
// <records blob> + [next_ts:u64][next_seq:u64] 16-byte cursor trailer.
// A StreamID.MAX (max,max) cursor sets Done (PEL fully scanned).
func parseClaimResponse(data []byte) (*StreamClaimResult, error) {
	if len(data) < 16 {
		return &StreamClaimResult{Records: []StreamRecord{}, Done: true}, nil
	}
	cursorOff := len(data) - 16
	nextTS := binary.LittleEndian.Uint64(data[cursorOff:])
	nextSeq := binary.LittleEndian.Uint64(data[cursorOff+8:])

	read, err := parseStreamReadResponse(data[:cursorOff])
	if err != nil {
		return nil, err
	}

	done := nextTS == math.MaxUint64 && nextSeq == math.MaxUint64
	return &StreamClaimResult{
		Records:    read.Records,
		NextCursor: StreamID{TimestampMS: nextTS, Sequence: nextSeq},
		Done:       done,
	}, nil
}

// parsePendingEntries decodes the PEL wire format:
// [count:u32]([ts:u64][seq:u64][delivery_count:u32][consumer_len:u16][consumer])*
func parsePendingEntries(data []byte) ([]PendingEntry, error) {
	if len(data) < 4 {
		return []PendingEntry{}, nil
	}
	pos := 0
	count := binary.LittleEndian.Uint32(data[pos:])
	pos += 4
	entries := make([]PendingEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		if pos+8+8+4+2 > len(data) {
			return nil, fmt.Errorf("incomplete pending entry")
		}
		ts := binary.LittleEndian.Uint64(data[pos:])
		pos += 8
		seq := binary.LittleEndian.Uint64(data[pos:])
		pos += 8
		dc := binary.LittleEndian.Uint32(data[pos:])
		pos += 4
		clen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if pos+clen > len(data) {
			return nil, fmt.Errorf("incomplete pending entry consumer")
		}
		consumer := string(data[pos : pos+clen])
		pos += clen
		entries = append(entries, PendingEntry{
			ID:            StreamID{TimestampMS: ts, Sequence: seq},
			Consumer:      consumer,
			DeliveryCount: dc,
		})
	}
	return entries, nil
}

// GroupAck acknowledges records in a consumer group.
func (s *StreamClient) GroupAck(stream, group string, ids []StreamID, opts *StreamGroupAckOptions) error {
	if opts == nil {
		opts = &StreamGroupAckOptions{}
	}

	namespace := s.client.getNamespace(opts.Namespace)

	// Encode group, consumer and ids in value:
	// [group_len:u16][group][consumer_len:u16][consumer][count:u32][timestamp_ms:u64][sequence:u64]*
	consumer := opts.Consumer
	value := make([]byte, 2+len(group)+2+len(consumer)+4+len(ids)*16)
	offset := 0

	binary.LittleEndian.PutUint16(value[offset:], uint16(len(group)))
	offset += 2
	copy(value[offset:], group)
	offset += len(group)

	binary.LittleEndian.PutUint16(value[offset:], uint16(len(consumer)))
	offset += 2
	copy(value[offset:], consumer)
	offset += len(consumer)

	binary.LittleEndian.PutUint32(value[offset:], uint32(len(ids)))
	offset += 4

	for _, id := range ids {
		binary.LittleEndian.PutUint64(value[offset:], id.TimestampMS)
		offset += 8
		binary.LittleEndian.PutUint64(value[offset:], id.Sequence)
		offset += 8
	}

	_, err := s.client.sendAndCheck(OpStreamGroupAck, namespace, []byte(stream), value, nil, true)
	return err
}

// GroupNack negatively acknowledges records in a consumer group.
// Records will be redelivered after the redelivery delay.
func (s *StreamClient) GroupNack(stream, group string, ids []StreamID, opts *StreamGroupNackOptions) error {
	if opts == nil {
		opts = &StreamGroupNackOptions{}
	}

	namespace := s.client.getNamespace(opts.Namespace)

	// Encode group, consumer and ids in value:
	// [group_len:u16][group][consumer_len:u16][consumer][count:u32][timestamp_ms:u64][sequence:u64]*
	consumer := opts.Consumer
	value := make([]byte, 2+len(group)+2+len(consumer)+4+len(ids)*16)
	offset := 0

	binary.LittleEndian.PutUint16(value[offset:], uint16(len(group)))
	offset += 2
	copy(value[offset:], group)
	offset += len(group)

	binary.LittleEndian.PutUint16(value[offset:], uint16(len(consumer)))
	offset += 2
	copy(value[offset:], consumer)
	offset += len(consumer)

	binary.LittleEndian.PutUint32(value[offset:], uint32(len(ids)))
	offset += 4

	for _, id := range ids {
		binary.LittleEndian.PutUint64(value[offset:], id.TimestampMS)
		offset += 8
		binary.LittleEndian.PutUint64(value[offset:], id.Sequence)
		offset += 8
	}

	// Build options for redelivery delay
	var options []byte
	if opts.RedeliveryDelayMS != nil {
		builder := NewOptionsBuilder()
		builder.AddU32(OptRedeliveryDelayMS, *opts.RedeliveryDelayMS)
		options = builder.Build()
	}

	_, err := s.client.sendAndCheck(OpStreamGroupNack, namespace, []byte(stream), value, options, true)
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

		// Read key (stream name) if present
		var streamName string
		if keyPresent != 0 {
			if pos+4 > len(data) {
				return nil, fmt.Errorf("incomplete stream record: missing key length")
			}
			keyLen := binary.LittleEndian.Uint32(data[pos:])
			pos += 4
			if pos+int(keyLen) > len(data) {
				return nil, fmt.Errorf("incomplete stream record: missing key data")
			}
			streamName = string(data[pos : pos+int(keyLen)])
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

		// Read headers: [header_count:u32]([key_len:u32][key][val_len:u32][val])*
		if pos+4 > len(data) {
			return nil, fmt.Errorf("incomplete stream record: missing header count")
		}
		headerCount := binary.LittleEndian.Uint32(data[pos:])
		pos += 4

		var headers map[string]string
		if headerCount > 0 {
			headers = make(map[string]string, headerCount)
			for h := uint32(0); h < headerCount; h++ {
				if pos+4 > len(data) {
					return nil, fmt.Errorf("incomplete stream record: missing header key length")
				}
				hKeyLen := binary.LittleEndian.Uint32(data[pos:])
				pos += 4
				if pos+int(hKeyLen) > len(data) {
					return nil, fmt.Errorf("incomplete stream record: missing header key")
				}
				hKey := string(data[pos : pos+int(hKeyLen)])
				pos += int(hKeyLen)
				if pos+4 > len(data) {
					return nil, fmt.Errorf("incomplete stream record: missing header value length")
				}
				hValLen := binary.LittleEndian.Uint32(data[pos:])
				pos += 4
				if pos+int(hValLen) > len(data) {
					return nil, fmt.Errorf("incomplete stream record: missing header value")
				}
				hVal := string(data[pos : pos+int(hValLen)])
				pos += int(hValLen)
				headers[hKey] = hVal
			}
		}

		records = append(records, StreamRecord{
			ID: StreamID{
				TimestampMS: uint64(timestampMs),
				Sequence:    sequence,
			},
			Tier:    tier,
			Stream:  streamName,
			Payload: payload,
			Headers: headers,
		})
	}

	return &StreamReadResult{
		Records: records,
	}, nil
}
