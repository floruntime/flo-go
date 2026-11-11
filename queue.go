package flo

import (
	"encoding/binary"
	"fmt"
)

// QueueClient provides queue operations for a Flo client.
type QueueClient struct {
	client *Client
}

// Enqueue adds a message to a queue and returns its seq. A queue past its
// caps of messages or held bytes, or a shard whose queues are, refuses it
// overloaded (ran no): the client retries that briefly as any overloaded,
// then returns ErrOverloaded with the server's message. Retrying later
// helps only once the queue is consumed or purged.
func (q *QueueClient) Enqueue(queue string, payload []byte, opts *EnqueueOptions) (uint64, error) {
	if opts == nil {
		opts = &EnqueueOptions{}
	}
	if opts.DelayMS > MaxQueueDelayMS {
		return 0, fmt.Errorf("flo: enqueue delay %d ms, at most %d", opts.DelayMS, MaxQueueDelayMS)
	}
	namespace := q.client.getNamespace(opts.Namespace)

	builder := NewOptionsBuilder()
	if opts.Priority != 0 {
		builder.AddU8(OptPriority, opts.Priority)
	}
	if opts.DelayMS != 0 {
		builder.AddU64(OptDelayMS, opts.DelayMS)
	}

	resp, err := q.client.sendAndCheck(OpQueueEnqueue, namespace, []byte(queue), payload, builder.Build(), false)
	if err != nil {
		return 0, err
	}

	return parseEnqueueResponse(resp.Data)
}

// The bounds a dequeue and a report carry.
const (
	// MaxDequeueCount is the most messages one dequeue takes, and one
	// report names.
	MaxDequeueCount = 100
	maxConsumerID   = 4096 - 14
	maxQueueError   = 1024
	maxQueuePayload = 64 * 1024
)

// Dequeue takes up to count (1..MaxDequeueCount) of a queue's next ready
// messages, leased to opts.Consumer. They stay in the queue until
// completed: Complete each within the lease (or Touch to keep it), or Fail
// it to make it ready again; one whose lease runs out is delivered again.
// Delivery is at least once. With opts.Ack they're completed as they're
// taken. An empty result took nothing.
func (q *QueueClient) Dequeue(queue string, count int, opts *DequeueOptions) (*DequeueResult, error) {
	if opts == nil {
		opts = &DequeueOptions{}
	}
	if count < 1 || count > MaxDequeueCount {
		return nil, fmt.Errorf("flo: dequeue count %d, want 1..%d", count, MaxDequeueCount)
	}
	if err := checkBlockMS(opts.BlockMS); err != nil {
		return nil, err
	}
	if len(opts.Consumer) > maxConsumerID {
		return nil, fmt.Errorf("flo: consumer id is %d bytes, at most %d", len(opts.Consumer), maxConsumerID)
	}
	if l := opts.LeaseMS; l != nil && (*l < MinLeaseMS || *l > MaxLeaseMS) {
		return nil, fmt.Errorf("flo: lease %d ms, want %d..%d", *l, MinLeaseMS, MaxLeaseMS)
	}
	var blockMS uint64
	if opts.BlockMS != nil {
		blockMS = uint64(*opts.BlockMS)
	}
	w := bodyWriter{buf: make([]byte, 0, 32+len(opts.Consumer))}
	w.u8(1)
	w.bytes16([]byte(opts.Consumer))
	w.u8(1)
	w.u32(uint32(count))
	w.optU64(opts.LeaseMS)
	w.u8(1)
	w.u64(blockMS)
	w.u8(1)
	w.u8(boolByte(opts.Ack))

	resp, err := q.client.sendAndCheck(OpQueueDequeue, q.client.getNamespace(opts.Namespace), []byte(queue), w.buf, nil, false)
	if err != nil {
		return nil, err
	}
	got, err := decodeDequeueAnswer(resp.Data)
	if err != nil {
		return nil, err
	}
	got.Lease.Consumer = opts.Consumer
	return got, nil
}

// decodeDequeueAnswer reads queue.dequeue's answer: messages, term, index,
// expires_at_ms.
func decodeDequeueAnswer(data []byte) (*DequeueResult, error) {
	r := bodyReader{buf: data}
	n := int(r.u8())
	if n > MaxDequeueCount {
		return nil, ErrIncompleteResponse
	}
	got := &DequeueResult{Messages: make([]Message, 0, n)}
	for i := 0; i < n && !r.bad; i++ {
		m := Message{Seq: r.u64(), Priority: r.u32(), Deliveries: r.u32(), EnqueuedAtMS: int64(r.u64()), Payload: r.bytes32()}
		if len(m.Payload) > maxQueuePayload {
			r.bad = true
		}
		got.Messages = append(got.Messages, m)
	}
	got.Lease.Term = r.u64()
	got.Lease.Index = r.u64()
	got.ExpiresAtMS = int64(r.u64())
	if err := r.done(); err != nil {
		return nil, err
	}
	return got, nil
}

// reportBody codes a report's leading fields: consumer, term, index,
// messages.
func reportBody(lease QueueLease, items []QueueItem, extra int) (*bodyWriter, error) {
	if len(items) == 0 || len(items) > MaxDequeueCount {
		return nil, fmt.Errorf("flo: a report names 1..%d messages, got %d", MaxDequeueCount, len(items))
	}
	if len(lease.Consumer) > maxConsumerID {
		return nil, fmt.Errorf("flo: consumer id is %d bytes, at most %d", len(lease.Consumer), maxConsumerID)
	}
	w := &bodyWriter{buf: make([]byte, 0, 20+len(lease.Consumer)+12*len(items)+extra)}
	w.u8(1)
	w.bytes16([]byte(lease.Consumer))
	w.u64(lease.Term)
	w.u64(lease.Index)
	w.u8(uint8(len(items)))
	for _, it := range items {
		w.u64(it.Seq)
		w.u32(it.Deliveries)
	}
	return w, nil
}

// Complete removes delivered messages from the queue. The answer is ok
// with, per message, applied or refused and why: QueueGone means done
// already (a resent complete), any other why means the delivery moved on
// and the message will be or was delivered again.
func (q *QueueClient) Complete(queue string, lease QueueLease, items []QueueItem, opts *QueueReportOptions) (*QueueReport, error) {
	if opts == nil {
		opts = &QueueReportOptions{}
	}
	w, err := reportBody(lease, items, 0)
	if err != nil {
		return nil, err
	}
	resp, err := q.client.sendAndCheck(OpQueueComplete, q.client.getNamespace(opts.Namespace), []byte(queue), w.buf, nil, false)
	if err != nil {
		return nil, err
	}
	return decodeQueueReport(resp.Data, false)
}

// Fail hands delivered messages back: ready again after opts.DelayMS, or
// dead once their attempts are spent (or at once with opts.Dead). Refusals
// come per message, as for Complete.
func (q *QueueClient) Fail(queue string, lease QueueLease, items []QueueItem, opts *QueueFailOptions) (*QueueReport, error) {
	if opts == nil {
		opts = &QueueFailOptions{}
	}
	if opts.DelayMS > MaxQueueDelayMS {
		return nil, fmt.Errorf("flo: fail delay %d ms, at most %d", opts.DelayMS, MaxQueueDelayMS)
	}
	if len(opts.Error) > maxQueueError {
		return nil, fmt.Errorf("flo: fail error is %d bytes, at most %d", len(opts.Error), maxQueueError)
	}
	w, err := reportBody(lease, items, 16+len(opts.Error))
	if err != nil {
		return nil, err
	}
	w.u8(1)
	w.u64(opts.DelayMS)
	w.u8(1)
	w.u8(boolByte(opts.Dead))
	w.u8(1)
	w.bytes16([]byte(opts.Error))
	resp, err := q.client.sendAndCheck(OpQueueFail, q.client.getNamespace(opts.Namespace), []byte(queue), w.buf, nil, false)
	if err != nil {
		return nil, err
	}
	return decodeQueueReport(resp.Data, false)
}

// Touch keeps delivered messages' lease: with half of it or less left, it
// runs a full lease from now, up to 12 h from their dequeue; before that it
// changes nothing. ExpiresAtMS is when it runs out.
func (q *QueueClient) Touch(queue string, lease QueueLease, items []QueueItem, opts *QueueReportOptions) (*QueueReport, error) {
	if opts == nil {
		opts = &QueueReportOptions{}
	}
	w, err := reportBody(lease, items, 0)
	if err != nil {
		return nil, err
	}
	resp, err := q.client.sendAndCheck(OpQueueTouch, q.client.getNamespace(opts.Namespace), []byte(queue), w.buf, nil, false)
	if err != nil {
		return nil, err
	}
	return decodeQueueReport(resp.Data, true)
}

// decodeQueueReport reads a report's answer: [expires_at_ms] for a touch,
// then applied and the refused messages.
func decodeQueueReport(data []byte, touch bool) (*QueueReport, error) {
	r := bodyReader{buf: data}
	got := &QueueReport{}
	if touch {
		got.ExpiresAtMS = int64(r.u64())
	}
	got.Applied = r.u32()
	n := int(r.u8())
	if n > MaxDequeueCount {
		return nil, ErrIncompleteResponse
	}
	for i := 0; i < n && !r.bad; i++ {
		f := QueueRefusal{Seq: r.u64(), Why: QueueRefusalWhy(r.u8()), Reason: string(r.bytes8())}
		if f.Why > QueueNotHeld {
			r.bad = true
		}
		got.Refused = append(got.Refused, f)
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	return got, nil
}

// Purge removes every message from a queue, dead letters included, and
// returns how many it removed.
func (q *QueueClient) Purge(queue string, opts *PurgeOptions) (uint32, error) {
	if opts == nil {
		opts = &PurgeOptions{}
	}
	resp, err := q.client.sendAndCheck(OpQueuePurge, q.client.getNamespace(opts.Namespace), []byte(queue), nil, nil, false)
	if err != nil {
		return 0, err
	}
	if len(resp.Data) != 4 {
		return 0, fmt.Errorf("%w: purge answer of %d bytes", ErrIncompleteResponse, len(resp.Data))
	}
	return binary.LittleEndian.Uint32(resp.Data), nil
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
