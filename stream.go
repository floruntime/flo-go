package flo

import (
	"encoding/binary"
	"errors"
	"fmt"
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

// Read reads records from a stream. Without BlockMS, a read of a stream
// that doesn't exist (never created or appended to, or deleted) is an
// error that IsNotFound reports, never an empty read; one that exists with
// nothing past Start is ok and empty. A blocking read of a stream not
// created yet waits for its first append. A consumer that may start before
// its stream exists should catch not-found and start again. A stale node's
// not-found is ErrStaleNotFound (IsStale, and still IsNotFound): the stream
// may exist but that node hasn't seen it yet, so read again rather than
// treat it as gone.
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

	if err := checkBlockMS(opts.BlockMS); err != nil {
		return nil, err
	}
	if opts.BlockMS != nil {
		builder.AddU32(OptBlockMS, *opts.BlockMS)
	}

	resp, err := s.client.sendRequest(OpStreamRead, namespace, []byte(stream), nil, builder.Build())
	if err != nil {
		return nil, err
	}
	if err := checkRead(resp); err != nil {
		return nil, err
	}

	return parseStreamReadResponse(resp.Data)
}

// Info gets stream metadata. A stream that doesn't exist is ErrNotFound,
// not an empty stream's zeros.
func (s *StreamClient) Info(stream string, opts *StreamInfoOptions) (*StreamInfo, error) {
	if opts == nil {
		opts = &StreamInfoOptions{}
	}

	namespace := s.client.getNamespace(opts.Namespace)

	resp, err := s.client.sendAndCheck(OpStreamInfo, namespace, []byte(stream), nil, nil, false)
	if err != nil {
		return nil, err
	}

	// Parse response: [first_ts:u64][first_seq:u64][last_ts:u64][last_seq:u64][count:u64][bytes:u64][partition_count:u32][retention_age_s:u64][retention_count:u64][retention_bytes:u64]
	if len(resp.Data) < 76 {
		return nil, fmt.Errorf("%w: stream info answer of %d bytes", ErrIncompleteResponse, len(resp.Data))
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
		RetentionAgeS:  binary.LittleEndian.Uint64(resp.Data[52:60]),
		RetentionCount: binary.LittleEndian.Uint64(resp.Data[60:68]),
	}, nil
}

// Create creates a stream with its settings: partitions and retention. An
// append creates the stream it names, with one partition and no retention,
// so create one first only to set others. Creating a stream that exists
// with the same settings changes nothing, as a retry does. Creating one
// with other settings is ErrConflict ("the stream exists with other
// settings"): change them with Alter. Once a stream has been altered, a
// create must name the settings it has now, partitions and retention as
// the alter left them, or it is that conflict. The one exception is a
// stream an append made and nothing has altered: a create gives it the
// settings asked. A name whose 64-bit hash matches another stream's in the
// namespace is ErrConflict too; choose another name.
func (s *StreamClient) Create(stream string, opts *StreamCreateOptions) error {
	if opts == nil {
		opts = &StreamCreateOptions{}
	}
	var value []byte
	if opts.Partitions > 0 {
		value = binary.LittleEndian.AppendUint32(nil, opts.Partitions)
	}
	_, err := s.client.sendAndCheck(OpStreamCreate, s.client.getNamespace(opts.Namespace), []byte(stream), value, retentionOptions(opts.RetentionAgeS, opts.RetentionCount), false)
	return err
}

// Alter changes a stream's retention: each of RetentionAgeS and
// RetentionCount that is non-zero replaces the stream's. A stream an append
// made can be altered. A stream that doesn't exist is ErrNotFound. After an
// alter, a create must name the stream's settings as they now are (see
// Create).
func (s *StreamClient) Alter(stream string, opts *StreamAlterOptions) error {
	if opts == nil {
		opts = &StreamAlterOptions{}
	}
	_, err := s.client.sendAndCheck(OpStreamAlter, s.client.getNamespace(opts.Namespace), []byte(stream), nil, retentionOptions(opts.RetentionAgeS, opts.RetentionCount), false)
	return err
}

func retentionOptions(ageS, count uint64) []byte {
	b := NewOptionsBuilder()
	if ageS > 0 {
		b.AddU64(OptRetentionAge, ageS)
	}
	if count > 0 {
		b.AddU64(OptRetentionCount, count)
	}
	return b.Build()
}

// Trim removes a stream's oldest records by exactly one bound: Before,
// MaxLen or MaxAgeSeconds. With DryRun it removes nothing and reports what
// it would remove. Trim cuts whole append batches, so MaxLen may keep a few
// more records than asked.
func (s *StreamClient) Trim(stream string, opts *StreamTrimOptions) (*StreamTrimResult, error) {
	if opts == nil {
		opts = &StreamTrimOptions{}
	}

	namespace := s.client.getNamespace(opts.Namespace)

	builder := NewOptionsBuilder()

	if opts.Before != nil {
		builder.AddBytes(OptStreamStart, opts.Before.ToBytes())
	}

	if opts.MaxLen != nil {
		builder.AddU64(OptLimit, *opts.MaxLen)
	}

	if opts.MaxAgeSeconds != nil {
		builder.AddU64(OptMaxAgeSeconds, *opts.MaxAgeSeconds)
	}

	if opts.DryRun {
		builder.AddFlag(OptDryRun)
	}

	resp, err := s.client.sendAndCheck(OpStreamTrim, namespace, []byte(stream), nil, builder.Build(), false)
	if err != nil {
		return nil, err
	}
	if len(resp.Data) != 16 {
		return nil, fmt.Errorf("stream trim: response is %d bytes, want 16", len(resp.Data))
	}
	return &StreamTrimResult{
		Removed:  binary.LittleEndian.Uint64(resp.Data[0:8]),
		FirstSeq: binary.LittleEndian.Uint64(resp.Data[8:16]),
	}, nil
}

// Delete removes a stream's records and metadata. The server refuses a
// non-empty stream with ErrConflict unless Force is set. Deleting a stream
// that doesn't exist succeeds.
func (s *StreamClient) Delete(stream string, opts *StreamDeleteOptions) error {
	if opts == nil {
		opts = &StreamDeleteOptions{}
	}

	namespace := s.client.getNamespace(opts.Namespace)

	builder := NewOptionsBuilder()
	if opts.Force {
		builder.AddFlag(OptForce)
	}

	_, err := s.client.sendAndCheck(OpStreamDelete, namespace, []byte(stream), nil, builder.Build(), false)
	return err
}

// Consumer groups lease a stream's records, one at a time. A read takes the
// group's records ready again (expired or nacked) first, then the stream's
// past the group's cursor, and leases them to the consumer: each comes back
// with its Deliveries count, and the read with its delivery's lease (term,
// index). Ack, Nack and Touch name that lease and each record's
// deliveries. A record whose lease runs out without an ack is ready for the
// next read; one nacked or expired MaxDeliver times is dead. Delivery is at
// least once. A group's leases are logged, so a leader change keeps them.

const (
	// MaxGroupNameSize is the longest group name.
	MaxGroupNameSize = 256
	// MaxGroupBatch is the most records one group read leases and one
	// report names.
	MaxGroupBatch = 100
	// MinGroupAckTimeoutMS and MaxGroupAckTimeoutMS bound a group's lease.
	MinGroupAckTimeoutMS = 5000
	MaxGroupAckTimeoutMS = 12 * 60 * 60 * 1000
	// MaxGroupNackError is the longest error a nack carries, in bytes.
	MaxGroupNackError = 1024
)

// groupValue codes a group request's value: [group_len:u16][group] and,
// with a consumer, [consumer_len:u16][consumer].
func groupValue(group string, consumer *string, extra int) (*bodyWriter, error) {
	if group == "" {
		return nil, errors.New("flo: a group name is required")
	}
	if len(group) > MaxGroupNameSize {
		return nil, fmt.Errorf("flo: group name is %d bytes, at most %d", len(group), MaxGroupNameSize)
	}
	size := 2 + len(group) + extra
	if consumer != nil {
		if len(*consumer) > maxConsumerID {
			return nil, fmt.Errorf("flo: consumer id is %d bytes, at most %d", len(*consumer), maxConsumerID)
		}
		size += 2 + len(*consumer)
	}
	w := &bodyWriter{buf: make([]byte, 0, size)}
	w.bytes16([]byte(group))
	if consumer != nil {
		w.bytes16([]byte(*consumer))
	}
	return w, nil
}

// GroupCreate creates a consumer group with its lease (AckTimeoutMS) and
// attempts (MaxDeliver), which only a create sets. A read creates the group
// it names at the defaults (a 30 s lease, 5 attempts), so create one first
// only to set others. It reports whether it made the group: creating one
// that exists with the same settings changes nothing, and with others is
// ErrConflict, the message naming the group's own.
func (s *StreamClient) GroupCreate(stream, group string, opts *StreamGroupCreateOptions) (bool, error) {
	if opts == nil {
		opts = &StreamGroupCreateOptions{}
	}
	w, err := groupValue(group, nil, 0)
	if err != nil {
		return false, err
	}
	builder := NewOptionsBuilder()
	if opts.AckTimeoutMS != nil {
		if t := *opts.AckTimeoutMS; t < MinGroupAckTimeoutMS || t > MaxGroupAckTimeoutMS {
			return false, fmt.Errorf("flo: group ack timeout %d ms, want %d..%d", t, MinGroupAckTimeoutMS, MaxGroupAckTimeoutMS)
		}
		builder.AddU32(OptAckTimeoutMS, *opts.AckTimeoutMS)
	}
	if opts.MaxDeliver != nil {
		if *opts.MaxDeliver == 0 {
			return false, errors.New("flo: group max deliver is 1..255")
		}
		builder.AddU8(OptMaxDeliver, *opts.MaxDeliver)
	}
	resp, err := s.client.sendAndCheck(OpStreamGroupCreate, s.client.getNamespace(opts.Namespace), []byte(stream), w.buf, builder.Build(), false)
	if err != nil {
		return false, err
	}
	r := bodyReader{buf: resp.Data}
	created := r.bool()
	return created, r.done()
}

// GroupDelete deletes a consumer group and every record it holds, and
// returns how many it held. An unknown group is ErrNotFound.
func (s *StreamClient) GroupDelete(stream, group string, opts *StreamInfoOptions) (uint64, error) {
	if opts == nil {
		opts = &StreamInfoOptions{}
	}
	w, err := groupValue(group, nil, 0)
	if err != nil {
		return 0, err
	}
	resp, err := s.client.sendAndCheck(OpStreamGroupDelete, s.client.getNamespace(opts.Namespace), []byte(stream), w.buf, nil, false)
	if err != nil {
		return 0, err
	}
	r := bodyReader{buf: resp.Data}
	dropped := r.u64()
	return dropped, r.done()
}

// GroupRead leases up to Count (default and at most MaxGroupBatch) of the
// group's records to consumer: those ready again first, then the stream's
// past the group's cursor. With BlockMS it waits that long for one. Ack each
// within the lease (or Touch to keep it), or Nack it; a record not acked in
// time is delivered again. An empty consumer is the server's "default". The
// first read of a group creates it at the defaults. An empty result leased
// nothing.
func (s *StreamClient) GroupRead(stream, group, consumer string, opts *StreamGroupReadOptions) (*StreamGroupReadResult, error) {
	if opts == nil {
		opts = &StreamGroupReadOptions{}
	}
	w, err := groupValue(group, &consumer, 0)
	if err != nil {
		return nil, err
	}
	builder := NewOptionsBuilder()
	if opts.Count != nil {
		builder.AddU32(OptCount, *opts.Count)
	}
	if err := checkBlockMS(opts.BlockMS); err != nil {
		return nil, err
	}
	if opts.BlockMS != nil {
		builder.AddU32(OptBlockMS, *opts.BlockMS)
	}
	resp, err := s.client.sendAndCheck(OpStreamGroupRead, s.client.getNamespace(opts.Namespace), []byte(stream), w.buf, builder.Build(), false)
	if err != nil {
		return nil, err
	}
	got, err := parseGroupReadResponse(resp.Data)
	if err != nil {
		return nil, err
	}
	got.Lease.Consumer = consumer
	return got, nil
}

// parseGroupReadResponse reads a group read's answer:
// [term:u64][index:u64][expires_at_ms:i64][count:u32], then each record as
// a stream read gives it followed by [deliveries:u32].
func parseGroupReadResponse(data []byte) (*StreamGroupReadResult, error) {
	const fixed = 8 + 8 + 8 + 4
	if len(data) < fixed {
		return nil, fmt.Errorf("%w: group read answer of %d bytes", ErrIncompleteResponse, len(data))
	}
	got := &StreamGroupReadResult{
		Lease:       StreamGroupLease{Term: binary.LittleEndian.Uint64(data[0:8]), Index: binary.LittleEndian.Uint64(data[8:16])},
		ExpiresAtMS: int64(binary.LittleEndian.Uint64(data[16:24])),
	}
	count := binary.LittleEndian.Uint32(data[24:28])
	if count > MaxGroupBatch {
		return nil, fmt.Errorf("%w: group read of %d records", ErrIncompleteResponse, count)
	}
	got.Records = make([]StreamGroupRecord, 0, count)
	pos := fixed
	for i := uint32(0); i < count; i++ {
		rec, next, err := parseStreamRecord(data, pos)
		if err != nil {
			return nil, err
		}
		if next+4 > len(data) {
			return nil, fmt.Errorf("incomplete group record: missing deliveries")
		}
		got.Records = append(got.Records, StreamGroupRecord{StreamRecord: rec, Deliveries: binary.LittleEndian.Uint32(data[next:])})
		pos = next + 4
	}
	if pos != len(data) {
		return nil, fmt.Errorf("%w: %d bytes past a group read's records", ErrIncompleteResponse, len(data)-pos)
	}
	return got, nil
}

// groupReportBody codes a report's value: group, consumer, term, index,
// then each record's id and deliveries.
func groupReportBody(group string, lease StreamGroupLease, items []StreamGroupItem, extra int) (*bodyWriter, error) {
	if len(items) == 0 || len(items) > MaxGroupBatch {
		return nil, fmt.Errorf("flo: a group report names 1..%d records, got %d", MaxGroupBatch, len(items))
	}
	w, err := groupValue(group, &lease.Consumer, 8+8+4+20*len(items)+extra)
	if err != nil {
		return nil, err
	}
	w.u64(lease.Term)
	w.u64(lease.Index)
	w.u32(uint32(len(items)))
	for _, it := range items {
		w.u64(it.ID.TimestampMS)
		w.u64(it.ID.Sequence)
		w.u32(it.Deliveries)
	}
	return w, nil
}

// GroupAck settles records a read leased: they leave the group. The answer
// is ok with, per record, applied or refused and why. A refusal means the
// record's delivery moved on (its lease ran out, it was delivered again, or
// another consumer holds it): nothing was applied to it, and resending
// won't change that. A resent ack whose first applied is refused gone. A
// report to a group that isn't there (deleted, say) is ErrNotFound, not a
// refusal per record.
func (s *StreamClient) GroupAck(stream, group string, lease StreamGroupLease, items []StreamGroupItem, opts *StreamGroupAckOptions) (*StreamGroupReport, error) {
	if opts == nil {
		opts = &StreamGroupAckOptions{}
	}
	w, err := groupReportBody(group, lease, items, 0)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.sendAndCheck(OpStreamGroupAck, s.client.getNamespace(opts.Namespace), []byte(stream), w.buf, nil, false)
	if err != nil {
		return nil, err
	}
	return decodeGroupReport(resp.Data, false)
}

// GroupNack hands leased records back: ready for the next read after
// DelayMS, or dead once nacked or expired MaxDeliver times (at once with
// Dead). With Abandon they are handed back unstarted instead: ready at
// once, no failure counted, never dead-lettered; abandon only records the
// consumer never started, never on an error. Refusals come per record, as
// for GroupAck; a group that isn't there is ErrNotFound.
func (s *StreamClient) GroupNack(stream, group string, lease StreamGroupLease, items []StreamGroupItem, opts *StreamGroupNackOptions) (*StreamGroupReport, error) {
	if opts == nil {
		opts = &StreamGroupNackOptions{}
	}
	if opts.DelayMS > MaxQueueDelayMS {
		return nil, fmt.Errorf("flo: nack delay %d ms, at most %d", opts.DelayMS, MaxQueueDelayMS)
	}
	if len(opts.Error) > MaxGroupNackError {
		return nil, fmt.Errorf("flo: nack error is %d bytes, at most %d", len(opts.Error), MaxGroupNackError)
	}
	if opts.Abandon && (opts.Dead || opts.DelayMS > 0 || opts.Error != "") {
		return nil, errors.New("flo: an abandoning nack hands records back unstarted, so it takes no Dead, DelayMS or Error")
	}
	w, err := groupReportBody(group, lease, items, 2+len(opts.Error))
	if err != nil {
		return nil, err
	}
	w.bytes16([]byte(opts.Error))
	builder := NewOptionsBuilder()
	if opts.DelayMS > 0 {
		builder.AddU64(OptDelayMS, opts.DelayMS)
	}
	if opts.Dead {
		builder.AddFlag(OptDead)
	}
	if opts.Abandon {
		builder.AddFlag(OptAbandon)
	}
	resp, err := s.client.sendAndCheck(OpStreamGroupNack, s.client.getNamespace(opts.Namespace), []byte(stream), w.buf, builder.Build(), false)
	if err != nil {
		return nil, err
	}
	return decodeGroupReport(resp.Data, false)
}

// GroupTouch keeps leased records' lease: with half of it or less left, it
// runs a full lease from now, up to MaxHoldMS from their read; before that
// it changes nothing. ExpiresAtMS is when it runs out. Refusals come per
// record, as for GroupAck.
func (s *StreamClient) GroupTouch(stream, group string, lease StreamGroupLease, items []StreamGroupItem, opts *StreamGroupAckOptions) (*StreamGroupReport, error) {
	if opts == nil {
		opts = &StreamGroupAckOptions{}
	}
	w, err := groupReportBody(group, lease, items, 0)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.sendAndCheck(OpStreamGroupTouch, s.client.getNamespace(opts.Namespace), []byte(stream), w.buf, nil, false)
	if err != nil {
		return nil, err
	}
	return decodeGroupReport(resp.Data, true)
}

// decodeGroupReport reads a report's answer: [expires_at_ms:i64] for a
// touch, then [applied:u32][refused:u32] and each refused
// [ms:u64][seq:u64][why:u8].
func decodeGroupReport(data []byte, touch bool) (*StreamGroupReport, error) {
	r := bodyReader{buf: data}
	got := &StreamGroupReport{}
	if touch {
		got.ExpiresAtMS = int64(r.u64())
	}
	got.Applied = r.u32()
	n := int(r.u32())
	if n > MaxGroupBatch {
		return nil, ErrIncompleteResponse
	}
	for i := 0; i < n && !r.bad; i++ {
		f := StreamGroupRefusal{ID: StreamID{TimestampMS: r.u64(), Sequence: r.u64()}, Why: GroupRefusalWhy(r.u8())}
		if f.Why > maxQueueRefusalWhy {
			r.bad = true
		}
		got.Refused = append(got.Refused, f)
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	return got, nil
}

// GroupInfo returns a consumer group's counts. An unknown group is
// ErrNotFound.
func (s *StreamClient) GroupInfo(stream, group string, opts *StreamInfoOptions) (*StreamGroupInfo, error) {
	if opts == nil {
		opts = &StreamInfoOptions{}
	}
	w, err := groupValue(group, nil, 0)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.sendAndCheck(OpStreamGroupInfo, s.client.getNamespace(opts.Namespace), []byte(stream), w.buf, nil, false)
	if err != nil {
		return nil, err
	}
	r := bodyReader{buf: resp.Data}
	got := &StreamGroupInfo{Pending: r.u64(), Holders: r.u32(), CreatedAtNS: r.u64(), Ready: r.u64(), Leased: r.u64(), Dead: r.u64(), Trimmed: r.u64()}
	if err := r.done(); err != nil {
		return nil, err
	}
	return got, nil
}

// GroupPending lists up to MaxGroupBatch of a consumer group's leased
// records: consumer's alone when it is non-empty, else every consumer's.
// An unknown group is ErrNotFound.
func (s *StreamClient) GroupPending(stream, group, consumer string, opts *StreamInfoOptions) ([]PendingEntry, error) {
	if opts == nil {
		opts = &StreamInfoOptions{}
	}
	var c *string
	if consumer != "" {
		c = &consumer
	}
	w, err := groupValue(group, c, 0)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.sendAndCheck(OpStreamGroupPending, s.client.getNamespace(opts.Namespace), []byte(stream), w.buf, nil, false)
	if err != nil {
		return nil, err
	}
	return parsePendingEntries(resp.Data)
}

// GroupList returns a stream's consumer groups, as many as one answer
// holds; More says there are others. It reads the leader's state.
func (s *StreamClient) GroupList(stream string, opts *StreamInfoOptions) (*StreamGroupList, error) {
	if opts == nil {
		opts = &StreamInfoOptions{}
	}
	resp, err := s.client.sendAndCheck(OpStreamGroupList, s.client.getNamespace(opts.Namespace), []byte(stream), nil, nil, false)
	if err != nil {
		return nil, err
	}
	return decodeGroupListAnswer(resp.Data)
}

// GroupMembers returns a consumer group's members, as many as one answer
// holds; More says there are others. An unknown group is an error that
// IsNotFound reports. It reads the leader's state.
func (s *StreamClient) GroupMembers(stream, group string, opts *StreamInfoOptions) (*StreamGroupMembers, error) {
	if opts == nil {
		opts = &StreamInfoOptions{}
	}
	w, err := groupValue(group, nil, 0)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.sendAndCheck(OpStreamGroupMembers, s.client.getNamespace(opts.Namespace), []byte(stream), w.buf, nil, false)
	if err != nil {
		return nil, err
	}
	return decodeGroupMembersAnswer(resp.Data)
}

// GroupSteal takes records another consumer holds and has left idle for
// at least minIdleMS (1 ms to 12 h): idle is the time since the holder last
// touched one (its read, a steal, or any touch). Each record taken gets a
// new delivery, leased to consumer, its failures kept; report on it with
// the result's Lease and Stolen. Name each record by its id and the
// deliveries its holder has (GroupPending lists them). It is an operator's
// command: a read takes expired records by itself. An unknown group is an
// error that IsNotFound reports.
func (s *StreamClient) GroupSteal(stream, group, consumer string, minIdleMS uint64, items []StreamGroupItem, opts *StreamGroupStealOptions) (*StreamGroupStealResult, error) {
	if opts == nil {
		opts = &StreamGroupStealOptions{}
	}
	if consumer == "" {
		return nil, errors.New("flo: a steal names the consumer that takes the records")
	}
	if minIdleMS < 1 || minIdleMS > MaxGroupAckTimeoutMS {
		return nil, fmt.Errorf("flo: steal min idle %d ms, want 1..%d", minIdleMS, MaxGroupAckTimeoutMS)
	}
	if l := opts.LeaseMS; l != nil && (*l < MinGroupAckTimeoutMS || *l > MaxGroupAckTimeoutMS) {
		return nil, fmt.Errorf("flo: steal lease %d ms, want %d..%d", *l, MinGroupAckTimeoutMS, MaxGroupAckTimeoutMS)
	}
	if len(items) == 0 || len(items) > MaxGroupBatch {
		return nil, fmt.Errorf("flo: a steal names 1..%d records, got %d", MaxGroupBatch, len(items))
	}
	w, err := groupValue(group, &consumer, 8+9+1+20*len(items))
	if err != nil {
		return nil, err
	}
	w.u64(minIdleMS)
	w.optU64(opts.LeaseMS)
	w.u8(uint8(len(items)))
	for _, it := range items {
		w.u64(it.ID.TimestampMS)
		w.u64(it.ID.Sequence)
		w.u32(it.Deliveries)
	}
	resp, err := s.client.sendAndCheck(OpStreamGroupSteal, s.client.getNamespace(opts.Namespace), []byte(stream), w.buf, nil, false)
	if err != nil {
		return nil, err
	}
	got, err := decodeStealAnswer(resp.Data)
	if err != nil {
		return nil, err
	}
	got.Lease.Consumer = consumer
	return got, nil
}

// decodeStealAnswer reads stream.group.steal's answer: term, index,
// expires_at_ms, stolen, refused.
func decodeStealAnswer(data []byte) (*StreamGroupStealResult, error) {
	r := bodyReader{buf: data}
	got := &StreamGroupStealResult{Lease: StreamGroupLease{Term: r.u64(), Index: r.u64()}, ExpiresAtMS: int64(r.u64())}
	n := int(r.u8())
	if n > MaxGroupBatch {
		return nil, ErrIncompleteResponse
	}
	for i := 0; i < n && !r.bad; i++ {
		got.Stolen = append(got.Stolen, StreamGroupItem{ID: StreamID{TimestampMS: r.u64(), Sequence: r.u64()}, Deliveries: r.u32()})
	}
	got.Refused = decodeGroupRefusals(&r)
	if err := r.done(); err != nil {
		return nil, err
	}
	return got, nil
}

// decodeGroupRefusals reads an operator command's refused records: id,
// why, reason.
func decodeGroupRefusals(r *bodyReader) []StreamGroupRefusal {
	n := int(r.u8())
	if n > MaxGroupBatch {
		r.bad = true
		return nil
	}
	var refused []StreamGroupRefusal
	for i := 0; i < n && !r.bad; i++ {
		f := StreamGroupRefusal{ID: StreamID{TimestampMS: r.u64(), Sequence: r.u64()}, Why: GroupRefusalWhy(r.u8()), Reason: string(r.bytes8())}
		if f.Why > maxQueueRefusalWhy {
			r.bad = true
		}
		refused = append(refused, f)
	}
	return refused
}

// GroupDLQList returns one page of a consumer group's dead letters, oldest
// dead first. Pass the page's Cursor as the next call's After to read the
// next one. It reads the leader's state. An unknown group is an error that
// IsNotFound reports.
func (s *StreamClient) GroupDLQList(stream, group string, opts *StreamGroupDLQListOptions) (*StreamGroupDLQPage, error) {
	if opts == nil {
		opts = &StreamGroupDLQListOptions{}
	}
	if opts.Limit > MaxDLQPage {
		return nil, fmt.Errorf("flo: DLQ list limit %d, at most %d", opts.Limit, MaxDLQPage)
	}
	w, err := groupValue(group, nil, 5+25)
	if err != nil {
		return nil, err
	}
	optLimit(w, opts.Limit)
	if c := opts.After; c != nil {
		w.u8(1)
		w.u64(c.deadNS)
		w.u64(c.id.TimestampMS)
		w.u64(c.id.Sequence)
	} else {
		w.u8(0)
	}
	resp, err := s.client.sendAndCheck(OpStreamGroupDLQList, s.client.getNamespace(opts.Namespace), []byte(stream), w.buf, nil, false)
	if err != nil {
		return nil, err
	}
	return decodeGroupDLQPage(resp.Data)
}

// decodeGroupDLQPage reads stream.group.dlq_list's answer: dead, cursor.
func decodeGroupDLQPage(data []byte) (*StreamGroupDLQPage, error) {
	r := bodyReader{buf: data}
	n := int(r.u8())
	if n > MaxDLQPage {
		return nil, ErrIncompleteResponse
	}
	page := &StreamGroupDLQPage{Dead: make([]StreamGroupDeadLetter, 0, n)}
	for i := 0; i < n && !r.bad; i++ {
		d := StreamGroupDeadLetter{ID: StreamID{TimestampMS: r.u64(), Sequence: r.u64()}, Deliveries: r.u32(), Failures: r.u32(), DeadAtMS: int64(r.u64())}
		d.Error = string(r.bytes16())
		if len(d.Error) > maxDLQError {
			r.bad = true
		}
		page.Dead = append(page.Dead, d)
	}
	if r.present() {
		page.Cursor = &StreamGroupDLQCursor{deadNS: r.u64(), id: StreamID{TimestampMS: r.u64(), Sequence: r.u64()}}
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	return page, nil
}

// GroupDLQRequeue makes a group's dead letters ready again at once, with
// their failures reset to 0 (their deliveries and last error stay): those
// named by id, or with opts.All the oldest, up to opts.Limit. One trimmed
// from the stream since is gone. The answer says how many it applied to,
// why each other was refused, and, with All, whether more remain.
func (s *StreamClient) GroupDLQRequeue(stream, group string, ids []StreamID, opts *StreamGroupDLQOptions) (*StreamGroupDLQReport, error) {
	if opts == nil {
		opts = &StreamGroupDLQOptions{}
	}
	w, err := groupDLQBody(group, ids, opts.All, opts.Limit)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.sendAndCheck(OpStreamGroupDLQRequeue, s.client.getNamespace(opts.Namespace), []byte(stream), w.buf, nil, false)
	if err != nil {
		return nil, err
	}
	return decodeGroupDLQReport(resp.Data)
}

// GroupDLQDelete deletes a group's dead letters for good, named or with
// opts.All the oldest, as GroupDLQRequeue takes them. With opts.DryRun it
// deletes nothing, and Applied is how many it would.
func (s *StreamClient) GroupDLQDelete(stream, group string, ids []StreamID, opts *StreamGroupDLQDeleteOptions) (*StreamGroupDLQReport, error) {
	if opts == nil {
		opts = &StreamGroupDLQDeleteOptions{}
	}
	w, err := groupDLQBody(group, ids, opts.All, opts.Limit)
	if err != nil {
		return nil, err
	}
	w.u8(1)
	w.u8(boolByte(opts.DryRun))
	resp, err := s.client.sendAndCheck(OpStreamGroupDLQDelete, s.client.getNamespace(opts.Namespace), []byte(stream), w.buf, nil, false)
	if err != nil {
		return nil, err
	}
	return decodeGroupDLQReport(resp.Data)
}

// groupDLQBody codes a group requeue's or delete's leading fields: group,
// records, all, limit.
func groupDLQBody(group string, ids []StreamID, all bool, limit uint32) (*bodyWriter, error) {
	if err := checkDeadPick(len(ids), all, limit); err != nil {
		return nil, err
	}
	w, err := groupValue(group, nil, 2+16*len(ids)+9)
	if err != nil {
		return nil, err
	}
	if all {
		w.u8(0)
	} else {
		w.u8(1)
		w.u8(uint8(len(ids)))
		for _, id := range ids {
			w.u64(id.TimestampMS)
			w.u64(id.Sequence)
		}
	}
	w.u8(1)
	w.u8(boolByte(all))
	optLimit(w, limit)
	return w, nil
}

// decodeGroupDLQReport reads a group requeue's or delete's answer:
// applied, refused, more.
func decodeGroupDLQReport(data []byte) (*StreamGroupDLQReport, error) {
	r := bodyReader{buf: data}
	got := &StreamGroupDLQReport{Applied: r.u32()}
	got.Refused = decodeGroupRefusals(&r)
	got.More = r.bool()
	if err := r.done(); err != nil {
		return nil, err
	}
	return got, nil
}

const (
	maxListedGroups    = 256
	maxListedMembers   = 1024
	memberStateLeaving = 1
)

// decodeGroupListAnswer reads stream.group.list's answer: groups, more.
func decodeGroupListAnswer(data []byte) (*StreamGroupList, error) {
	r := bodyReader{buf: data}
	n := int(r.u16())
	if n > maxListedGroups {
		return nil, ErrIncompleteResponse
	}
	got := &StreamGroupList{Groups: make([]StreamGroup, 0, n)}
	for i := 0; i < n && !r.bad; i++ {
		got.Groups = append(got.Groups, StreamGroup{
			Name:             string(r.bytes16()),
			Members:          r.u32(),
			Pending:          r.u64(),
			CreatedAtMS:      int64(r.u64()),
			AckTimeoutMS:     r.u32(),
			MaxDeliver:       r.u8(),
			LastDeliveredMS:  r.u64(),
			LastDeliveredSeq: r.u64(),
		})
	}
	got.More = r.bool()
	if err := r.done(); err != nil {
		return nil, err
	}
	return got, nil
}

// decodeGroupMembersAnswer reads stream.group.members' answer: members, more.
func decodeGroupMembersAnswer(data []byte) (*StreamGroupMembers, error) {
	r := bodyReader{buf: data}
	n := int(r.u16())
	if n > maxListedMembers {
		return nil, ErrIncompleteResponse
	}
	got := &StreamGroupMembers{Members: make([]StreamGroupMember, 0, n)}
	for i := 0; i < n && !r.bad; i++ {
		m := StreamGroupMember{ID: string(r.bytes16())}
		state := r.u8()
		if state > memberStateLeaving {
			r.bad = true
		}
		m.Leaving = state == memberStateLeaving
		m.Pending = r.u32()
		m.JoinedAtMS = int64(r.u64())
		if r.present() {
			at := int64(r.u64())
			m.LastActiveMS = &at
		}
		got.Members = append(got.Members, m)
	}
	got.More = r.bool()
	if err := r.done(); err != nil {
		return nil, err
	}
	return got, nil
}

// parsePendingEntries decodes a group pending answer:
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
			ID:         StreamID{TimestampMS: ts, Sequence: seq},
			Consumer:   consumer,
			Deliveries: dc,
		})
	}
	return entries, nil
}

// parseStreamReadResponse parses a stream read response.
// Wire format: [count:u32] then each record as parseStreamRecord reads it.
func parseStreamReadResponse(data []byte) (*StreamReadResult, error) {
	if len(data) < 4 {
		return &StreamReadResult{
			Records: []StreamRecord{},
		}, nil
	}

	pos := 0
	count := binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	// A count the data can't hold is refused before it sizes an
	// allocation, and an answer that ends before its count of records is
	// refused, not returned short.
	if uint64(count) > uint64(len(data)-pos)/minStreamRecord {
		return nil, fmt.Errorf("incomplete stream read: %d records can't fit in %d bytes", count, len(data)-pos)
	}

	records := make([]StreamRecord, 0, count)
	for i := uint32(0); i < count; i++ {
		rec, next, err := parseStreamRecord(data, pos)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
		pos = next
	}

	return &StreamReadResult{
		Records: records,
	}, nil
}

// minStreamRecord is a record's least size: [seq:u64][ts:i64][tier:u8]
// [partition:u32][key_present:u8][payload_len:u32][header_count:u32].
const minStreamRecord = 8 + 8 + 1 + 4 + 1 + 4 + 4

// parseStreamRecord reads the record at data[pos:] and returns it and the
// position after it:
// [sequence:u64][timestamp_ms:i64][tier:u8][partition:u32][key_present:u8]
// ([key_len:u32][key])?[payload_len:u32][payload][header_count:u32]
// ([key_len:u32][key][val_len:u32][val])*
func parseStreamRecord(data []byte, pos int) (StreamRecord, int, error) {
	// Read sequence
	if pos+8 > len(data) {
		return StreamRecord{}, 0, fmt.Errorf("incomplete stream record: missing sequence")
	}
	sequence := binary.LittleEndian.Uint64(data[pos:])
	pos += 8

	// Read timestamp_ms
	if pos+8 > len(data) {
		return StreamRecord{}, 0, fmt.Errorf("incomplete stream record: missing timestamp_ms")
	}
	timestampMs := int64(binary.LittleEndian.Uint64(data[pos:]))
	pos += 8

	// Read tier
	if pos+1 > len(data) {
		return StreamRecord{}, 0, fmt.Errorf("incomplete stream record: missing tier")
	}
	tier := StorageTier(data[pos])
	pos += 1

	// Skip partition
	if pos+4 > len(data) {
		return StreamRecord{}, 0, fmt.Errorf("incomplete stream record: missing partition")
	}
	pos += 4

	// Read key_present
	if pos+1 > len(data) {
		return StreamRecord{}, 0, fmt.Errorf("incomplete stream record: missing key_present")
	}
	keyPresent := data[pos]
	pos += 1

	// Read key (stream name) if present
	var streamName string
	if keyPresent != 0 {
		if pos+4 > len(data) {
			return StreamRecord{}, 0, fmt.Errorf("incomplete stream record: missing key length")
		}
		keyLen := binary.LittleEndian.Uint32(data[pos:])
		pos += 4
		if pos+int(keyLen) > len(data) {
			return StreamRecord{}, 0, fmt.Errorf("incomplete stream record: missing key data")
		}
		streamName = string(data[pos : pos+int(keyLen)])
		pos += int(keyLen)
	}

	// Read payload
	if pos+4 > len(data) {
		return StreamRecord{}, 0, fmt.Errorf("incomplete stream record: missing payload length")
	}
	payloadLen := binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	if pos+int(payloadLen) > len(data) {
		return StreamRecord{}, 0, fmt.Errorf("incomplete stream record: missing payload data")
	}
	payload := make([]byte, payloadLen)
	copy(payload, data[pos:pos+int(payloadLen)])
	pos += int(payloadLen)

	// Read headers: [header_count:u32]([key_len:u32][key][val_len:u32][val])*
	if pos+4 > len(data) {
		return StreamRecord{}, 0, fmt.Errorf("incomplete stream record: missing header count")
	}
	headerCount := binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	var headers map[string]string
	if headerCount > 0 {
		headers = make(map[string]string, headerCount)
		for h := uint32(0); h < headerCount; h++ {
			if pos+4 > len(data) {
				return StreamRecord{}, 0, fmt.Errorf("incomplete stream record: missing header key length")
			}
			hKeyLen := binary.LittleEndian.Uint32(data[pos:])
			pos += 4
			if pos+int(hKeyLen) > len(data) {
				return StreamRecord{}, 0, fmt.Errorf("incomplete stream record: missing header key")
			}
			hKey := string(data[pos : pos+int(hKeyLen)])
			pos += int(hKeyLen)
			if pos+4 > len(data) {
				return StreamRecord{}, 0, fmt.Errorf("incomplete stream record: missing header value length")
			}
			hValLen := binary.LittleEndian.Uint32(data[pos:])
			pos += 4
			if pos+int(hValLen) > len(data) {
				return StreamRecord{}, 0, fmt.Errorf("incomplete stream record: missing header value")
			}
			hVal := string(data[pos : pos+int(hValLen)])
			pos += int(hValLen)
			headers[hKey] = hVal
		}
	}

	return StreamRecord{
		ID: StreamID{
			TimestampMS: uint64(timestampMs),
			Sequence:    sequence,
		},
		Tier:    tier,
		Stream:  streamName,
		Payload: payload,
		Headers: headers,
	}, pos, nil
}
