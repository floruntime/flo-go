// Package flo provides a Go client SDK for the Flo distributed systems platform.
//
// Flo is a platform for building reliable and complex software with durable
// primitives for communication and state management. This SDK provides access
// to all Flo primitives: KV store, Queues, Streams, Actions, and Workers.
//
// All primitives are accessed as fields on a connected Client:
//
//	client := flo.NewClient("localhost:9000", flo.WithNamespace("myapp"))
//	client.Connect()
//	client.KV.Get("key", nil)
//	client.Queue.Enqueue("tasks", payload, nil)
//	client.Stream.Append("events", payload, nil)
//	client.Action.Invoke("process", input, nil)
package flo

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

// Protocol constants
const (
	Magic      uint32 = 0x004F4C46 // "FLO\0" in little-endian
	Version    uint8  = 0x01
	HeaderSize        = 32

	// Size limits (for client-side validation)
	MaxNamespaceSize = 255
	MaxKeySize       = 64 * 1024        // 64 KB
	MaxValueSize     = 16 * 1024 * 1024 // 16 MB practical limit
)

// OpCode represents operation codes for Flo protocol requests.
// Three-layer layout: Infra(0x000–0x0FF), Data(0x100–0x2FF), Compute(0x300–0x3FF)
type OpCode uint16

const (
	// ── System (0x000 – 0x00F) ──
	OpPing          OpCode = 0x000
	OpPong          OpCode = 0x001
	OpErrorResponse OpCode = 0x002
	OpAuth          OpCode = 0x003
	OpSetDurability OpCode = 0x004
	OpOK            OpCode = 0x005

	// ── Namespace (0x010 – 0x02F) ──
	OpNamespaceCreate            OpCode = 0x010
	OpNamespaceDelete            OpCode = 0x011
	OpNamespaceList              OpCode = 0x012
	OpNamespaceInfo              OpCode = 0x013
	OpNamespaceConfigSet         OpCode = 0x014
	OpNamespaceConfigGet         OpCode = 0x015
	OpNamespaceCreateResponse    OpCode = 0x020
	OpNamespaceDeleteResponse    OpCode = 0x021
	OpNamespaceListResponse      OpCode = 0x022
	OpNamespaceInfoResponse      OpCode = 0x023
	OpNamespaceConfigSetResponse OpCode = 0x024
	OpNamespaceConfigGetResponse OpCode = 0x025

	// ── Cluster (0x030 – 0x04F) ──
	OpClusterStatus          OpCode = 0x030
	OpClusterMembers         OpCode = 0x031
	OpClusterJoin            OpCode = 0x032
	OpClusterLeave           OpCode = 0x033
	OpClusterTransferLeader  OpCode = 0x034
	OpClusterAddNode         OpCode = 0x035
	OpClusterRemoveNode      OpCode = 0x036
	OpClusterStatusResponse  OpCode = 0x040
	OpClusterMembersResponse OpCode = 0x041
	OpClusterJoinResponse    OpCode = 0x042

	// ── KV + Transactions + Snapshots (0x100 – 0x12F) ──
	OpKVPut                    OpCode = 0x100
	OpKVGet                    OpCode = 0x101
	OpKVMGet                   OpCode = 0x102
	OpKVDelete                 OpCode = 0x103
	OpKVScan                   OpCode = 0x104
	OpKVHistory                OpCode = 0x105
	OpKVGetResponse            OpCode = 0x106
	OpKVMGetResponse           OpCode = 0x107
	OpKVPutResponse            OpCode = 0x108
	OpKVScanResponse           OpCode = 0x109
	OpKVHistoryResponse        OpCode = 0x10A
	OpKVBeginTxn               OpCode = 0x110
	OpKVCommitTxn              OpCode = 0x111
	OpKVRollbackTxn            OpCode = 0x112
	OpKVSnapshotCreate         OpCode = 0x120
	OpKVSnapshotGet            OpCode = 0x121
	OpKVSnapshotRelease        OpCode = 0x122
	OpKVSnapshotCreateResponse OpCode = 0x123

	// ── Streams (0x130 – 0x14F) ──
	OpStreamAppend         OpCode = 0x130
	OpStreamRead           OpCode = 0x131
	OpStreamTrim           OpCode = 0x132
	OpStreamInfo           OpCode = 0x133
	OpStreamAppendResponse OpCode = 0x134
	OpStreamReadResponse   OpCode = 0x135
	OpStreamEvent          OpCode = 0x136
	OpStreamSubscribe      OpCode = 0x137
	OpStreamUnsubscribe    OpCode = 0x138
	OpStreamSubscribed     OpCode = 0x139
	OpStreamUnsubscribed   OpCode = 0x13A
	OpStreamList           OpCode = 0x13B
	OpStreamListResponse   OpCode = 0x13C
	OpStreamCreate         OpCode = 0x13D
	OpStreamCreateResponse OpCode = 0x13E
	OpStreamAlter          OpCode = 0x13F

	// ── Stream Consumer Groups (0x150 – 0x16F) ──
	OpStreamGroupCreate           OpCode = 0x150
	OpStreamGroupJoin             OpCode = 0x151
	OpStreamGroupLeave            OpCode = 0x152
	OpStreamGroupRead             OpCode = 0x153
	OpStreamGroupAck              OpCode = 0x154
	OpStreamGroupClaim            OpCode = 0x155
	OpStreamGroupPending          OpCode = 0x156
	OpStreamGroupConfigureSweeper OpCode = 0x157
	OpStreamGroupReadResponse     OpCode = 0x158
	OpStreamGroupNack             OpCode = 0x159
	OpStreamGroupTouch            OpCode = 0x15A
	OpStreamGroupInfo             OpCode = 0x15B
	OpStreamGroupDelete           OpCode = 0x15C

	// ── Queues (0x170 – 0x19F) ──
	OpQueueEnqueue              OpCode = 0x170
	OpQueueDequeue              OpCode = 0x171
	OpQueueComplete             OpCode = 0x172
	OpQueueExtendLease          OpCode = 0x173
	OpQueueFail                 OpCode = 0x174
	OpQueueFailAuto             OpCode = 0x175
	OpQueueDLQList              OpCode = 0x176
	OpQueueDLQDelete            OpCode = 0x177
	OpQueueDLQRequeue           OpCode = 0x178
	OpQueueDLQStats             OpCode = 0x179
	OpQueuePromoteDue           OpCode = 0x17A
	OpQueueStats                OpCode = 0x17B
	OpQueuePeek                 OpCode = 0x17C
	OpQueueTouch                OpCode = 0x17D
	OpQueueBatchEnqueue         OpCode = 0x17E
	OpQueuePurge                OpCode = 0x17F
	OpQueueEnqueueResponse      OpCode = 0x190
	OpQueueDequeueResponse      OpCode = 0x191
	OpQueueDLQListResponse      OpCode = 0x192
	OpQueueStatsResponse        OpCode = 0x193
	OpQueuePeekResponse         OpCode = 0x194
	OpQueueTouchResponse        OpCode = 0x195
	OpQueueBatchEnqueueResponse OpCode = 0x196
	OpQueuePurgeResponse        OpCode = 0x197
	OpQueueList                 OpCode = 0x198
	OpQueueListResponse         OpCode = 0x199

	// ── Time-Series (0x1A0 – 0x1BF) ──
	OpTSWrite             OpCode = 0x1A0
	OpTSRead              OpCode = 0x1A1
	OpTSQuery             OpCode = 0x1A2
	OpTSFloQL             OpCode = 0x1A3
	OpTSList              OpCode = 0x1A4
	OpTSDelete            OpCode = 0x1A5
	OpTSRetention         OpCode = 0x1A6
	OpTSWriteResponse     OpCode = 0x1A7
	OpTSReadResponse      OpCode = 0x1A8
	OpTSQueryResponse     OpCode = 0x1A9
	OpTSFloQLResponse     OpCode = 0x1AA
	OpTSListResponse      OpCode = 0x1AB
	OpTSDeleteResponse    OpCode = 0x1AC
	OpTSRetentionResponse OpCode = 0x1AD

	// ── Actions (0x300 – 0x31F) ──
	OpActionRegister         OpCode = 0x300
	OpActionInvoke           OpCode = 0x301
	OpActionStatus           OpCode = 0x302
	OpActionList             OpCode = 0x303
	OpActionListRuns         OpCode = 0x304
	OpActionDelete           OpCode = 0x305
	OpActionAwait            OpCode = 0x306
	OpActionComplete         OpCode = 0x307
	OpActionFail             OpCode = 0x308
	OpActionTouch            OpCode = 0x309
	OpActionRegisterResponse OpCode = 0x310
	OpActionInvokeResponse   OpCode = 0x311
	OpActionStatusResponse   OpCode = 0x312
	OpActionListResponse     OpCode = 0x313
	OpActionListRunsResponse OpCode = 0x314
	OpActionTaskAssignment   OpCode = 0x315

	// ── Workers (0x320 – 0x33F) ──
	OpWorkerRegister         OpCode = 0x320
	OpWorkerHeartbeat        OpCode = 0x321
	OpWorkerDeregister       OpCode = 0x322
	OpWorkerList             OpCode = 0x323
	OpWorkerInfo             OpCode = 0x324
	OpWorkerDrain            OpCode = 0x325
	OpWorkerRegisterResponse OpCode = 0x330
	OpWorkerListResponse     OpCode = 0x331
	OpWorkerInfoResponse     OpCode = 0x332
	OpWorkerDrainResponse    OpCode = 0x333

	// ── Workflows (0x340 – 0x35F) ──
	OpWorkflowCreate                  OpCode = 0x340
	OpWorkflowStart                   OpCode = 0x341
	OpWorkflowSignal                  OpCode = 0x342
	OpWorkflowCancel                  OpCode = 0x343
	OpWorkflowStatus                  OpCode = 0x344
	OpWorkflowHistory                 OpCode = 0x345
	OpWorkflowListRuns                OpCode = 0x346
	OpWorkflowGetDefinition           OpCode = 0x347
	OpWorkflowDisable                 OpCode = 0x348
	OpWorkflowEnable                  OpCode = 0x349
	OpWorkflowListDefinitions         OpCode = 0x34A
	OpWorkflowCreateResponse          OpCode = 0x350
	OpWorkflowStartResponse           OpCode = 0x351
	OpWorkflowStatusResponse          OpCode = 0x352
	OpWorkflowHistoryResponse         OpCode = 0x353
	OpWorkflowListRunsResponse        OpCode = 0x354
	OpWorkflowGetDefinitionResponse   OpCode = 0x355
	OpWorkflowDisableResponse         OpCode = 0x356
	OpWorkflowEnableResponse          OpCode = 0x357
	OpWorkflowListDefinitionsResponse OpCode = 0x358

	// ── Processing (0x360 – 0x37F) ──
	OpProcessingSubmit            OpCode = 0x360
	OpProcessingStop              OpCode = 0x361
	OpProcessingCancel            OpCode = 0x362
	OpProcessingStatus            OpCode = 0x363
	OpProcessingList              OpCode = 0x364
	OpProcessingSavepoint         OpCode = 0x365
	OpProcessingRestore           OpCode = 0x366
	OpProcessingRescale           OpCode = 0x367
	OpProcessingSubmitResponse    OpCode = 0x370
	OpProcessingStopResponse      OpCode = 0x371
	OpProcessingCancelResponse    OpCode = 0x372
	OpProcessingStatusResponse    OpCode = 0x373
	OpProcessingListResponse      OpCode = 0x374
	OpProcessingSavepointResponse OpCode = 0x375
	OpProcessingRestoreResponse   OpCode = 0x376
	OpProcessingRescaleResponse   OpCode = 0x377
)

// StatusCode represents status codes for Flo protocol responses.
type StatusCode uint8

const (
	StatusOK                   StatusCode = 0
	StatusErrorGeneric         StatusCode = 1
	StatusNotFound             StatusCode = 2
	StatusBadRequest           StatusCode = 3
	StatusCrossCoreTransaction StatusCode = 4
	StatusNoActiveTransaction  StatusCode = 5
	StatusGroupLocked          StatusCode = 6
	StatusUnauthorized         StatusCode = 7
	StatusConflict             StatusCode = 8
	StatusInternalError        StatusCode = 9
	StatusOverloaded           StatusCode = 10
	StatusRateLimited          StatusCode = 11 // Request rate limit exceeded (WebSocket)
)

// String returns a human-readable message for the status code.
func (s StatusCode) String() string {
	switch s {
	case StatusOK:
		return "OK"
	case StatusErrorGeneric:
		return "Generic error"
	case StatusNotFound:
		return "Not found"
	case StatusBadRequest:
		return "Bad request"
	case StatusCrossCoreTransaction:
		return "Cross-core transaction not supported"
	case StatusNoActiveTransaction:
		return "No active transaction"
	case StatusGroupLocked:
		return "Consumer group is locked"
	case StatusUnauthorized:
		return "Unauthorized"
	case StatusConflict:
		return "Conflict"
	case StatusInternalError:
		return "Internal server error"
	case StatusOverloaded:
		return "Server overloaded"
	case StatusRateLimited:
		return "Request rate limit exceeded"
	default:
		return "Unknown error"
	}
}

// OptionTag represents option tags for TLV-encoded operation parameters.
type OptionTag uint8

const (
	// KV Options (0x01 - 0x0F)
	OptTTLSeconds  OptionTag = 0x01 // u64: Time-to-live in seconds (0 = no expiration)
	OptCASVersion  OptionTag = 0x02 // u64: Expected version for compare-and-swap
	OptIfNotExists OptionTag = 0x03 // void: Only set if key doesn't exist (NX)
	OptIfExists    OptionTag = 0x04 // void: Only set if key exists (XX)
	OptLimit       OptionTag = 0x05 // u32: Maximum number of results for scan/list operations
	OptKeysOnly    OptionTag = 0x06 // u8: Skip values in scan response (0/1)
	OptCursor      OptionTag = 0x07 // bytes: Pagination cursor (ShardWalker format)
	OptRoutingKey  OptionTag = 0x08 // string: Explicit routing key for shard co-location

	// Queue Options (0x10 - 0x1F)
	OptPriority            OptionTag = 0x10 // u8: Message priority (0-255, higher = more urgent)
	OptDelayMS             OptionTag = 0x11 // u64: Delay before message becomes visible
	OptVisibilityTimeoutMS OptionTag = 0x12 // u32: How long message is invisible after dequeue
	OptDedupKey            OptionTag = 0x13 // string: Deduplication key
	OptMaxRetries          OptionTag = 0x14 // u8: Maximum retry attempts before DLQ
	OptCount               OptionTag = 0x15 // u32: Number of messages to dequeue
	OptSendToDLQ           OptionTag = 0x16 // u8: Whether to send failed messages to DLQ (0/1)
	OptBlockMS             OptionTag = 0x17 // u32: Blocking timeout for dequeue (0 = infinite)
	OptWaitMS              OptionTag = 0x18 // u32: Watch timeout - wait for NEXT version change (0=forever)

	// Stream Options (0x20 - 0x2F) - StreamID-native ONLY
	// All stream positioning uses StreamID (timestamp_ms + sequence) - no legacy offset/timestamp modes
	// 0x20 reserved
	OptStreamStart    OptionTag = 0x21 // [16]u8: Start StreamID for reads (inclusive)
	OptStreamEnd      OptionTag = 0x22 // [16]u8: End StreamID for reads (inclusive)
	OptStreamTail     OptionTag = 0x23 // void: Flag indicating tail read (start from end)
	OptPartition      OptionTag = 0x24 // u32: Explicit partition index
	OptPartitionKey   OptionTag = 0x25 // string: Key for partition routing
	OptMaxAgeSeconds  OptionTag = 0x26 // u64: Maximum age in seconds for retention
	OptMaxBytes       OptionTag = 0x27 // u64: Maximum size in bytes for retention
	OptDryRun         OptionTag = 0x28 // void: Flag to preview what would be deleted
	OptRetentionCount OptionTag = 0x29 // u64: Retention policy - max event count
	OptRetentionAge   OptionTag = 0x2A // u64: Retention policy - max age in seconds
	OptRetentionBytes OptionTag = 0x2B // u64: Retention policy - max bytes

	// Consumer Group Options (0x30 - 0x3F)
	OptAckTimeoutMS      OptionTag = 0x30 // u32: Time before unacked message auto-redelivers
	OptMaxDeliver        OptionTag = 0x31 // u8: Max delivery attempts before DLQ (default: 10)
	OptSubscriptionMode  OptionTag = 0x32 // u8: 0=shared, 1=exclusive, 2=key_shared
	OptRedeliveryDelayMS OptionTag = 0x33 // u32: Delay before NACK'd message becomes visible
	OptConsumerTimeoutMS OptionTag = 0x34 // u32: Remove consumer from group if no activity
	OptNoAck             OptionTag = 0x35 // void: Auto-ack on delivery (at-most-once)
	OptIdleTimeoutMS     OptionTag = 0x36 // u64: Min idle time for claiming stuck messages
	OptMaxAckPending     OptionTag = 0x37 // u32: Max unacked messages per consumer
	OptExtendAckMS       OptionTag = 0x38 // u32: Amount of time to extend ack deadline
	OptMaxStandbys       OptionTag = 0x39 // u16: Max standby consumers in exclusive mode
	OptNumSlots          OptionTag = 0x3A // u16: Number of hash slots for key_shared mode

	// Worker/Action Options (0x40 - 0x4F)
	OptWorkerID OptionTag = 0x40 // string: Worker identifier
	OptExtendMS OptionTag = 0x41 // u32: Lease extension time in milliseconds
	OptMaxTasks OptionTag = 0x42 // u32: Maximum tasks to return in batch
	OptRetry    OptionTag = 0x43 // u8: Whether to retry on failure (0/1)

	// Workflow Options (0x50 - 0x5F)
	OptTimeoutMS      OptionTag = 0x50 // u64: Workflow/activity timeout
	OptRetryPolicy    OptionTag = 0x51 // bytes: Serialized retry policy
	OptCorrelationID  OptionTag = 0x52 // string: Correlation ID for tracing
	OptSubscriptionID OptionTag = 0x53 // u64: Subscription ID for stream subscriptions

	// Time-Series Options (0x60 - 0x6F)
	OptTSFromMS      OptionTag = 0x60 // i64: Start of time range (inclusive, unix ms)
	OptTSToMS        OptionTag = 0x61 // i64: End of time range (inclusive, 0 = now)
	OptTSWindowMS    OptionTag = 0x62 // i64: Aggregation window size (ms)
	OptTSAggregation OptionTag = 0x63 // string: Aggregation function name (avg, sum, count, min, max)
	OptTSField       OptionTag = 0x64 // string: Field name filter (empty = "value")
	OptTSTags        OptionTag = 0x65 // string: Comma-separated tag filters "key=val,key2=val2"
	OptTSPrecision   OptionTag = 0x66 // u8: Timestamp precision (0=ns, 1=us, 2=ms, 3=s)
	OptTSTimestamp   OptionTag = 0x67 // i64: Explicit timestamp for write (0 = server-assigned)
	OptTSRawTTL      OptionTag = 0x68 // string: Raw data TTL (e.g., "7d")
	OptTSDownsample  OptionTag = 0x69 // string: Downsample rule (e.g., "1m:avg:30d")
	OptTSBatch       OptionTag = 0x6A // void: Flag indicating batch/line-protocol mode
)

// KVEntry represents an entry from scan results.
type KVEntry struct {
	Key   []byte
	Value []byte // nil if keys_only=true
}

// ScanResult represents the result of a KV scan operation.
type ScanResult struct {
	Entries []KVEntry
	Cursor  []byte // nil if no more pages
	HasMore bool
}

// VersionEntry represents a KV version entry from history.
type VersionEntry struct {
	Version   uint64
	Timestamp int64
	Value     []byte
}

// Message represents a queue message.
type Message struct {
	Seq     uint64
	Payload []byte
}

// DequeueResult represents the result of a queue dequeue operation.
type DequeueResult struct {
	Messages []Message
}

// GetOptions contains options for KV get operations.
type GetOptions struct {
	Namespace string
	BlockMS   *uint32 // Blocking timeout for long polling (nil = no blocking)
}

// PutOptions contains options for KV put operations.
type PutOptions struct {
	Namespace   string
	TTLSeconds  *uint64
	CASVersion  *uint64
	IfNotExists bool
	IfExists    bool
}

// DeleteOptions contains options for KV delete operations.
type DeleteOptions struct {
	Namespace string
}

// ScanOptions contains options for KV scan operations.
type ScanOptions struct {
	Namespace string
	Cursor    []byte
	Limit     *uint32
	KeysOnly  bool
}

// HistoryOptions contains options for KV history operations.
type HistoryOptions struct {
	Namespace string
	Limit     *uint32
}

// EnqueueOptions contains options for queue enqueue operations.
type EnqueueOptions struct {
	Namespace string
	Priority  uint8
	DelayMS   *uint64
	DedupKey  string
}

// DequeueOptions contains options for queue dequeue operations.
type DequeueOptions struct {
	Namespace           string
	VisibilityTimeoutMS *uint32
	BlockMS             *uint32
}

// AckOptions contains options for queue ack operations.
type AckOptions struct {
	Namespace string
}

// NackOptions contains options for queue nack operations.
type NackOptions struct {
	Namespace string
	ToDLQ     bool
}

// DLQListOptions contains options for DLQ list operations.
type DLQListOptions struct {
	Namespace string
	Limit     uint32
}

// DLQRequeueOptions contains options for DLQ requeue operations.
type DLQRequeueOptions struct {
	Namespace string
}

// PeekOptions contains options for queue peek operations.
type PeekOptions struct {
	Namespace string
}

// TouchOptions contains options for queue touch (lease renewal) operations.
type TouchOptions struct {
	Namespace string
}

// =============================================================================
// Stream Types
// =============================================================================

// StreamID represents a unique position in a stream (timestamp_ms + sequence).
// The StreamID format is: [timestamp_ms: u64][sequence: u64] = 16 bytes total.
type StreamID struct {
	TimestampMS uint64
	Sequence    uint64
}

// ToBytes serializes the StreamID to 16 bytes (big-endian for lexicographic sorting).
func (id StreamID) ToBytes() []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], id.TimestampMS)
	binary.BigEndian.PutUint64(buf[8:16], id.Sequence)
	return buf
}

// StreamIDFromBytes parses a StreamID from 16 bytes (big-endian).
func StreamIDFromBytes(data []byte) (StreamID, error) {
	if len(data) < 16 {
		return StreamID{}, fmt.Errorf("invalid StreamID: expected 16 bytes, got %d", len(data))
	}
	return StreamID{
		TimestampMS: binary.BigEndian.Uint64(data[0:8]),
		Sequence:    binary.BigEndian.Uint64(data[8:16]),
	}, nil
}

// String returns the StreamID in "timestamp-sequence" format (e.g. "1703350800000-3").
func (id StreamID) String() string {
	return fmt.Sprintf("%d-%d", id.TimestampMS, id.Sequence)
}

// Next returns the next StreamID in sequence (same timestamp, sequence + 1).
func (id StreamID) Next() StreamID {
	return StreamID{
		TimestampMS: id.TimestampMS,
		Sequence:    id.Sequence + 1,
	}
}

// StorageTier indicates the storage tier of a stream record.
type StorageTier uint8

const (
	StorageTierHot     StorageTier = 0
	StorageTierPending StorageTier = 1
	StorageTierWarm    StorageTier = 2
	StorageTierCold    StorageTier = 3
)

// StreamRecord represents a single stream record.
type StreamRecord struct {
	ID      StreamID
	Tier    StorageTier
	Stream  string
	Payload []byte
	Headers map[string]string
}

// Into unmarshals the record payload (JSON) into the provided value.
func (r *StreamRecord) Into(v interface{}) error {
	if len(r.Payload) == 0 {
		return fmt.Errorf("no payload data")
	}
	return json.Unmarshal(r.Payload, v)
}

// StreamReadResult represents the result of a stream read operation.
type StreamReadResult struct {
	Records []StreamRecord
}

// StreamAppendResult represents the result of a stream append operation.
type StreamAppendResult struct {
	ID StreamID
}

// StreamInfo represents stream metadata.
type StreamInfo struct {
	FirstID        StreamID
	LastID         StreamID
	Count          uint64
	Bytes          uint64
	PartitionCount uint32
}

// StreamAppendOptions contains options for stream append operations.
type StreamAppendOptions struct {
	Namespace string
	Headers   map[string]string
}

// StreamReadOptions contains options for stream read operations.
type StreamReadOptions struct {
	Namespace string
	Start     *StreamID // Start StreamID for reads (inclusive)
	End       *StreamID // End StreamID for reads (inclusive)
	Tail      bool      // Start from end of stream (mutually exclusive with Start)
	Partition *uint32   // Explicit partition index
	Count     *uint32   // Maximum number of records to return
	BlockMS   *uint32   // Blocking timeout (0 = infinite)
}

// StreamTrimOptions contains options for stream trim operations.
type StreamTrimOptions struct {
	Namespace     string
	MaxLen        *uint64 // Retention policy - max event count
	MaxAgeSeconds *uint64 // Retention policy - max age in seconds
	MaxBytes      *uint64 // Retention policy - max bytes
	DryRun        bool    // Preview what would be deleted without deleting
}

// StreamInfoOptions contains options for stream info operations.
type StreamInfoOptions struct {
	Namespace string
}

// StreamGroupJoinOptions contains options for consumer group join.
type StreamGroupJoinOptions struct {
	Namespace string
}

// StreamGroupReadOptions contains options for consumer group read.
type StreamGroupReadOptions struct {
	Namespace string
	Count     *uint32
	BlockMS   *uint32
}

// StreamGroupAckOptions contains options for consumer group ack.
type StreamGroupAckOptions struct {
	Namespace string
	Consumer  string // Consumer ID (required for correct ack matching)
}

// StreamGroupNackOptions contains options for consumer group nack.
type StreamGroupNackOptions struct {
	Namespace         string
	Consumer          string  // Consumer ID (required for correct nack matching)
	RedeliveryDelayMS *uint32 // Delay before message becomes visible again
}

// =============================================================================
// Action/Worker Types
// =============================================================================

// ActionType represents the type of action.
type ActionType uint8

const (
	ActionTypeUser ActionType = 0 // User-defined action (external worker processes tasks)
)

// RunStatus represents the status of an action run.
type RunStatus uint8

const (
	RunStatusPending   RunStatus = 0
	RunStatusRunning   RunStatus = 1
	RunStatusCompleted RunStatus = 2
	RunStatusFailed    RunStatus = 3
	RunStatusCancelled RunStatus = 4
	RunStatusTimedOut  RunStatus = 5
)

// TaskAssignment represents a task assigned to a worker.
type TaskAssignment struct {
	TaskID             string
	TaskType           string
	Payload            []byte
	CreatedAt          int64
	Attempt            uint32
	CallerRunID        string
	CallerWorkflowName string
}

// ActionRunStatus represents the status of an action invocation.
type ActionRunStatus struct {
	RunID        string
	Status       RunStatus
	CreatedAt    int64
	StartedAt    *int64
	CompletedAt  *int64
	Output       []byte
	ErrorMessage string
	RetryCount   uint32
}

// ActionRegisterOptions contains options for action registration.
type ActionRegisterOptions struct {
	Namespace   string
	Description string
	TimeoutMS   *uint64
	MaxRetries  *uint8
}

// ActionInvokeOptions contains options for action invocation.
type ActionInvokeOptions struct {
	Namespace      string
	Priority       *uint8
	DelayMS        *uint64
	IdempotencyKey string
}

// ActionInvokeResult represents the result of an action invocation.
type ActionInvokeResult struct {
	RunID string // Unique run ID for tracking
}

// ActionStatusOptions contains options for action status query.
type ActionStatusOptions struct {
	Namespace string
}

// ActionListOptions contains options for listing actions.
type ActionListOptions struct {
	Namespace string
	Limit     *uint32
	Prefix    string
}

// WorkerRegisterOptions contains options for registering a worker in the worker registry.
type WorkerRegisterOptions struct {
	Namespace      string
	WorkerType     WorkerType     // action or stream
	MaxConcurrency uint32         // Maximum concurrent tasks (default 10)
	Processes      []ProcessEntry // Actions/streams this worker handles
	Metadata       string         // Optional JSON metadata (labels, etc.)
	MachineID      string         // Optional machine/host identifier for grouping workers
}

// ProcessKind identifies what a registered process does.
type ProcessKind uint8

const (
	ProcessKindAction         ProcessKind = 0 // Handles an action
	ProcessKindStreamConsumer ProcessKind = 1 // Consumes a stream
)

// ProcessEntry describes a single process to register on a worker.
type ProcessEntry struct {
	Name string      // e.g. "process-image" or "events/processors"
	Kind ProcessKind // action or stream_consumer
}

// ProcessStats holds per-process tracking data returned by the server.
type ProcessStats struct {
	Name        string
	Kind        ProcessKind
	RunCount    uint64
	FailCount   uint64
	LastRunAtMS int64
}

// WorkerAwaitOptions contains options for action_await (blocking wait for task).
type WorkerAwaitOptions struct {
	Namespace string
	TimeoutMS *uint64
	BlockMS   *uint32
}

// WorkerCompleteOptions contains options for action_complete.
type WorkerCompleteOptions struct {
	Namespace string
	Outcome   string // Named outcome for workflow routing (default: "success")
}

// WorkerFailOptions contains options for action_fail.
type WorkerFailOptions struct {
	Namespace string
	Retry     bool
}

// WorkerTouchOptions contains options for action_touch (extend lease).
type WorkerTouchOptions struct {
	Namespace string
	ExtendMS  *uint32
}

// WorkerHeartbeatOptions contains options for worker heartbeat.
type WorkerHeartbeatOptions struct {
	Namespace string
}

// WorkerDeregisterOptions contains options for worker deregistration.
type WorkerDeregisterOptions struct {
	Namespace string
}

// WorkerDrainOptions contains options for draining a worker.
type WorkerDrainOptions struct {
	Namespace string
}

// WorkerListOptions contains options for listing workers.
type WorkerListOptions struct {
	Namespace string
	Limit     *uint32
}

// WorkerInfoOptions contains options for getting worker info.
type WorkerInfoOptions struct {
	Namespace string
}

// WorkerType identifies the kind of worker.
type WorkerType uint8

const (
	WorkerTypeAction WorkerType = 0 // Processes action tasks
	WorkerTypeStream WorkerType = 1 // Processes stream records
)

// WorkerStatus represents the health state of a worker.
type WorkerStatus uint8

const (
	WorkerStatusActive    WorkerStatus = 0 // Actively processing
	WorkerStatusIdle      WorkerStatus = 1 // Connected, no current tasks
	WorkerStatusDraining  WorkerStatus = 2 // Finishing current tasks, accepting no new ones
	WorkerStatusUnhealthy WorkerStatus = 3 // Missed heartbeats
)

// WorkerInfo holds information about a registered worker.
type WorkerInfo struct {
	ID             string
	Type           WorkerType
	Status         WorkerStatus
	Metadata       string         // JSON metadata
	MachineID      string         // Machine/host identifier
	Processes      []ProcessStats // Per-process tracking
	TasksCompleted uint64
	TasksFailed    uint64
	CurrentLoad    uint32
	MaxConcurrency uint32
	RegisteredAtMS int64
	LastHeartbeat  int64
}

// =============================================================================
// Workflow Types
// =============================================================================

// WorkflowCreateOptions contains options for creating a workflow.
type WorkflowCreateOptions struct {
	Namespace string
}

// WorkflowGetDefinitionOptions contains options for getting a workflow definition.
type WorkflowGetDefinitionOptions struct {
	Namespace string
	Version   string // If set, only returns if version matches
}

// WorkflowStartOptions contains options for starting a workflow run.
type WorkflowStartOptions struct {
	Namespace      string
	Version        string // Workflow version (default: "latest")
	IdempotencyKey string // Prevents duplicate runs with the same key
	RunID          string // Explicit run ID (auto-generated if empty)
}

// WorkflowStatusOptions contains options for workflow status queries.
type WorkflowStatusOptions struct {
	Namespace string
}

// WorkflowStatusResult contains parsed status of a workflow run.
type WorkflowStatusResult struct {
	RunID       string
	Workflow    string
	Version     string
	Status      string
	CurrentStep string
	Input       []byte
	CreatedAt   int64
	StartedAt   *int64
	CompletedAt *int64
	WaitSignal  *string
	Output      []byte
}

// WorkflowSignalOptions contains options for sending a signal to a workflow.
type WorkflowSignalOptions struct {
	Namespace string
}

// WorkflowCancelOptions contains options for cancelling a workflow run.
type WorkflowCancelOptions struct {
	Namespace string
}

// WorkflowDisableOptions contains options for disabling a workflow.
type WorkflowDisableOptions struct {
	Namespace string
}

// WorkflowEnableOptions contains options for enabling a workflow.
type WorkflowEnableOptions struct {
	Namespace string
}

// WorkflowSyncOptions contains options for declarative workflow sync.
type WorkflowSyncOptions struct {
	Namespace string
}

// WorkflowHistoryOptions contains options for workflow history queries.
type WorkflowHistoryOptions struct {
	Namespace string
}

// WorkflowListRunsOptions contains options for listing workflow runs.
type WorkflowListRunsOptions struct {
	Namespace string
	Limit     uint32
	Status    string // Filter by run status (e.g. "running", "completed", "failed")
	Cursor    string // Cursor for pagination (last run_id from previous page)
	Search    string // Case-insensitive search across run fields
}

// WorkflowListDefinitionsOptions contains options for listing workflow definitions.
type WorkflowListDefinitionsOptions struct {
	Namespace string
	Limit     uint32
	Cursor    []byte
}

// =============================================================================
// Processing Types
// =============================================================================

// ProcessingSubmitOptions contains options for submitting a processing job.
type ProcessingSubmitOptions struct {
	Namespace string
}

// ProcessingStatusOptions contains options for getting processing job status.
type ProcessingStatusOptions struct {
	Namespace string
}

// ProcessingListOptions contains options for listing processing jobs.
type ProcessingListOptions struct {
	Namespace string
	Limit     uint32
	Cursor    []byte
}

// ProcessingStopOptions contains options for stopping a processing job.
type ProcessingStopOptions struct {
	Namespace string
}

// ProcessingCancelOptions contains options for cancelling a processing job.
type ProcessingCancelOptions struct {
	Namespace string
}

// ProcessingSavepointOptions contains options for triggering a savepoint.
type ProcessingSavepointOptions struct {
	Namespace string
}

// ProcessingRestoreOptions contains options for restoring from a savepoint.
type ProcessingRestoreOptions struct {
	Namespace string
}

// ProcessingRescaleOptions contains options for rescaling a processing job.
type ProcessingRescaleOptions struct {
	Namespace string
}

// ProcessingSyncOptions contains options for declarative processing sync.
type ProcessingSyncOptions struct {
	Namespace string
}

// ProcessingStatusResult contains the status of a processing job.
type ProcessingStatusResult struct {
	JobID            string
	Name             string
	Status           string
	Parallelism      uint32
	BatchSize        uint32
	RecordsProcessed uint64
	CreatedAt        int64
}

// ProcessingListEntry represents a single entry in a processing job list.
type ProcessingListEntry struct {
	Name        string
	JobID       string
	Status      string
	Parallelism uint32
	CreatedAt   int64
}
