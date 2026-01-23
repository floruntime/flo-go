package flo

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

	if opts.DelayMS != nil {
		builder.AddU64(OptDelayMS, *opts.DelayMS)
	}

	if opts.DedupKey != "" {
		builder.AddBytes(OptDedupKey, []byte(opts.DedupKey))
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

	if opts.VisibilityTimeoutMS != nil {
		builder.AddU32(OptVisibilityTimeoutMS, *opts.VisibilityTimeoutMS)
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

// Nack negative acknowledges messages (retry or send to DLQ).
func (q *QueueClient) Nack(queue string, seqs []uint64, opts *NackOptions) error {
	if len(seqs) == 0 {
		return nil
	}

	if opts == nil {
		opts = &NackOptions{}
	}
	namespace := q.client.getNamespace(opts.Namespace)

	// Build TLV options
	builder := NewOptionsBuilder()

	if opts.ToDLQ {
		builder.AddU8(OptSendToDLQ, 1)
	}

	value := serializeSeqs(seqs)

	_, err := q.client.sendAndCheck(OpQueueFail, namespace, []byte(queue), value, builder.Build(), false)
	return err
}

// DLQList lists messages in the Dead Letter Queue.
func (q *QueueClient) DLQList(queue string, opts *DLQListOptions) (*DequeueResult, error) {
	if opts == nil {
		opts = &DLQListOptions{Limit: 100}
	}
	namespace := q.client.getNamespace(opts.Namespace)

	// Build TLV options
	builder := NewOptionsBuilder()
	builder.AddU32(OptLimit, opts.Limit)

	resp, err := q.client.sendAndCheck(OpQueueDLQList, namespace, []byte(queue), nil, builder.Build(), false)
	if err != nil {
		return nil, err
	}

	return parseDequeueResponse(resp.Data)
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

// Peek views messages without creating leases (no visibility timeout).
// Messages remain available for dequeue by other consumers.
func (q *QueueClient) Peek(queue string, count int, opts *PeekOptions) (*DequeueResult, error) {
	if opts == nil {
		opts = &PeekOptions{}
	}
	namespace := q.client.getNamespace(opts.Namespace)

	// Build TLV options
	builder := NewOptionsBuilder()
	builder.AddU32(OptCount, uint32(count))

	resp, err := q.client.sendAndCheck(OpQueuePeek, namespace, []byte(queue), nil, builder.Build(), false)
	if err != nil {
		return nil, err
	}

	return parseDequeueResponse(resp.Data)
}

// Touch extends the lease timeout for messages (renews visibility timeout).
// Use this to prevent messages from being returned to the queue while still processing.
func (q *QueueClient) Touch(queue string, seqs []uint64, opts *TouchOptions) error {
	if len(seqs) == 0 {
		return nil
	}

	if opts == nil {
		opts = &TouchOptions{}
	}
	namespace := q.client.getNamespace(opts.Namespace)

	value := serializeSeqs(seqs)

	_, err := q.client.sendAndCheck(OpQueueTouch, namespace, []byte(queue), value, nil, false)
	return err
}
