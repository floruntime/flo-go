package flo

import (
	"encoding/binary"
	"fmt"
)

// QueueClient provides queue operations for a Flo client.
type QueueClient struct {
	client *Client
}

// Enqueue adds a message to a queue.
// Returns the sequence number of the enqueued message.
func (q *QueueClient) Enqueue(queue string, payload []byte, opts *EnqueueOptions) (uint64, error) {
	if opts == nil {
		opts = &EnqueueOptions{}
	}
	namespace := q.client.getNamespace(opts.Namespace)

	// Build TLV options
	builder := NewOptionsBuilder()

	if opts.Priority != 0 {
		builder.AddU8(OptPriority, opts.Priority)
	}

	resp, err := q.client.sendAndCheck(OpQueueEnqueue, namespace, []byte(queue), payload, builder.Build(), false)
	if err != nil {
		return 0, err
	}

	return parseEnqueueResponse(resp.Data)
}

// Dequeue fetches messages from a queue.
func (q *QueueClient) Dequeue(queue string, count int, opts *DequeueOptions) (*DequeueResult, error) {
	if opts == nil {
		opts = &DequeueOptions{}
	}
	namespace := q.client.getNamespace(opts.Namespace)

	// Build TLV options
	builder := NewOptionsBuilder()
	builder.AddU32(OptCount, uint32(count))

	if err := checkBlockMS(opts.BlockMS); err != nil {
		return nil, err
	}
	if opts.BlockMS != nil {
		builder.AddU32(OptBlockMS, *opts.BlockMS)
	}

	resp, err := q.client.sendAndCheck(OpQueueDequeue, namespace, []byte(queue), nil, builder.Build(), false)
	if err != nil {
		return nil, err
	}

	return parseDequeueResponse(resp.Data)
}

// Ack acknowledges messages as successfully processed.
// A dequeue consumes its messages; queues are currently at-most-once, so Ack and Nack have no effect on a dequeued message.
func (q *QueueClient) Ack(queue string, seqs []uint64, opts *AckOptions) error {
	if len(seqs) == 0 {
		return nil
	}

	if opts == nil {
		opts = &AckOptions{}
	}
	namespace := q.client.getNamespace(opts.Namespace)

	value := serializeSeqs(seqs)

	_, err := q.client.sendAndCheck(OpQueueComplete, namespace, []byte(queue), value, nil, false)
	return err
}

// Nack negative acknowledges messages.
// A dequeue consumes its messages; queues are currently at-most-once, so Ack and Nack have no effect on a dequeued message.
func (q *QueueClient) Nack(queue string, seqs []uint64, opts *NackOptions) error {
	if len(seqs) == 0 {
		return nil
	}

	if opts == nil {
		opts = &NackOptions{}
	}
	namespace := q.client.getNamespace(opts.Namespace)

	value := serializeSeqs(seqs)

	_, err := q.client.sendAndCheck(OpQueueFail, namespace, []byte(queue), value, nil, false)
	return err
}

// DLQList returns one page of a queue's dead letters, oldest first. Pass
// the page's LastSeq as the next call's After to read the next one.
func (q *QueueClient) DLQList(queue string, opts *DLQListOptions) (*DLQPage, error) {
	if opts == nil {
		opts = &DLQListOptions{}
	}
	namespace := q.client.getNamespace(opts.Namespace)

	limit := opts.Limit
	if limit == 0 {
		limit = MaxDLQPage
	}
	if limit > MaxDLQPage {
		return nil, fmt.Errorf("flo: DLQ list limit %d, at most %d", limit, MaxDLQPage)
	}
	// Value: [limit:u32][after_seq:u64]?
	value := binary.LittleEndian.AppendUint32(make([]byte, 0, 12), limit)
	if opts.After != nil {
		value = binary.LittleEndian.AppendUint64(value, *opts.After)
	}

	resp, err := q.client.sendAndCheck(OpQueueDLQList, namespace, []byte(queue), value, nil, false)
	if err != nil {
		return nil, err
	}
	return decodeDLQPage(resp.Data)
}

// maxPeekPayload is the most of a payload Peek and DLQList carry.
const maxPeekPayload = 4096

// decodeDLQPage reads a DLQ list answer:
// [count:u32]([seq:u64][size:u32][len:u32][payload][enqueued_at_ms:i64]
// [attempts:u32][priority:u8][moved_at_ms:i64])*[total:u64][has_more:u8][last_seq:u64].
func decodeDLQPage(data []byte) (*DLQPage, error) {
	r := bodyReader{buf: data}
	n := r.u32()
	if n > MaxDLQPage {
		return nil, ErrIncompleteResponse
	}
	page := &DLQPage{Messages: make([]DeadLetter, 0, n)}
	for i := uint32(0); i < n && !r.bad; i++ {
		d := DeadLetter{Seq: r.u64(), Size: r.u32(), Payload: r.bytes32()}
		d.EnqueuedAtMS = int64(r.u64())
		d.Attempts = r.u32()
		d.Priority = r.u8()
		d.MovedAtMS = int64(r.u64())
		if len(d.Payload) > maxPeekPayload {
			r.bad = true
		}
		page.Messages = append(page.Messages, d)
	}
	page.Total = r.u64()
	page.HasMore = r.bool()
	page.LastSeq = r.u64()
	if err := r.done(); err != nil {
		return nil, err
	}
	return page, nil
}

// DLQRequeue moves messages from DLQ back to the main queue.
func (q *QueueClient) DLQRequeue(queue string, seqs []uint64, opts *DLQRequeueOptions) error {
	if len(seqs) == 0 {
		return nil
	}

	if opts == nil {
		opts = &DLQRequeueOptions{}
	}
	namespace := q.client.getNamespace(opts.Namespace)

	value := serializeSeqs(seqs)

	_, err := q.client.sendAndCheck(OpQueueDLQRequeue, namespace, []byte(queue), value, nil, false)
	return err
}

// Peek returns up to limit of a queue's next ready messages (1..MaxPeekLimit;
// 0 means the server's default, 20), in the order a dequeue takes them,
// without taking them. It reads the leader's state.
func (q *QueueClient) Peek(queue string, limit uint32, opts *PeekOptions) (*PeekResult, error) {
	if opts == nil {
		opts = &PeekOptions{}
	}
	if limit > MaxPeekLimit {
		return nil, fmt.Errorf("flo: peek limit %d, at most %d", limit, MaxPeekLimit)
	}
	w := bodyWriter{buf: make([]byte, 0, 5)}
	if limit == 0 {
		w.u8(0)
	} else {
		w.u8(1)
		w.u32(limit)
	}
	resp, err := q.client.sendAndCheck(OpQueuePeek, q.client.getNamespace(opts.Namespace), []byte(queue), w.buf, nil, false)
	if err != nil {
		return nil, err
	}
	return decodePeekAnswer(resp.Data)
}

// decodePeekAnswer reads queue.peek's answer: messages, more.
func decodePeekAnswer(data []byte) (*PeekResult, error) {
	r := bodyReader{buf: data}
	n := int(r.u8())
	if n > MaxPeekLimit {
		return nil, ErrIncompleteResponse
	}
	got := &PeekResult{Messages: make([]PeekedMessage, 0, n)}
	for i := 0; i < n && !r.bad; i++ {
		m := PeekedMessage{Seq: r.u64(), Priority: r.u32(), Attempts: r.u32(), EnqueuedAtMS: int64(r.u64()), Size: r.u32(), Payload: r.bytes16()}
		if len(m.Payload) > maxPeekPayload {
			r.bad = true
		}
		got.Messages = append(got.Messages, m)
	}
	got.More = r.bool()
	if err := r.done(); err != nil {
		return nil, err
	}
	return got, nil
}
