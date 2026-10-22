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
	Version    uint8  = 0x02
	HeaderSize        = 32

	// TableHash is the hash of the op table this SDK was built for, written
	// into every request header. A server built from another table refuses
	// the request, and its answers carry its own hash, which this client
	// refuses. CI's live tests run against the flo commit in
	// .github/flo-ref.env: change both in one commit.
	TableHash uint64 = 0x2fba7dd9ae941830

	// Size limits (for client-side validation)
	MaxNamespaceSize = 255
	MaxKeySize       = 64 * 1024        // 64 KB
	MaxValueSize     = 16 * 1024 * 1024 // 16 MB practical limit

	// MaxBlockMS is the longest blocking wait (BlockMS) the server accepts.
	MaxBlockMS uint32 = 300000 // 5 minutes

	defaultWorkerBlockMS uint32 = 30000
)

// OpCode represents operation codes for Flo protocol requests.
// Three-layer layout: Infra(0x000–0x0FF), Data(0x100–0x2FF), Compute(0x300–0x3FF)
type OpCode uint16

const (
	// ── System (0x000 – 0x00F) ──
	OpPing OpCode = 0x000

	// ── Namespace (0x010 – 0x02F) ──
	OpNamespaceCreate    OpCode = 0x010
	OpNamespaceDelete    OpCode = 0x011
	OpNamespaceList      OpCode = 0x012
	OpNamespaceInfo      OpCode = 0x013
	OpNamespaceConfigSet OpCode = 0x014
	OpNamespaceConfigGet OpCode = 0x015

	// ── Cluster (0x030 – 0x04F) ──
	OpClusterStatus         OpCode = 0x030
	OpClusterMembers        OpCode = 0x031
	OpClusterJoin           OpCode = 0x032
	OpClusterLeave          OpCode = 0x033
	OpClusterTransferLeader OpCode = 0x034
	OpClusterAddNode        OpCode = 0x035
	OpClusterRemoveNode     OpCode = 0x036

	// ── KV (0x100 – 0x12F) ──
	OpKVPut     OpCode = 0x100
	OpKVGet     OpCode = 0x101
	OpKVMGet    OpCode = 0x102
	OpKVDelete  OpCode = 0x103
	OpKVScan    OpCode = 0x104
	OpKVHistory OpCode = 0x105
	// KV Extended (atomic counters, JSON ops)
	OpKVIncr    OpCode = 0x10B
	OpKVJsonGet OpCode = 0x10C
	OpKVJsonSet OpCode = 0x10D
	OpKVJsonDel OpCode = 0x10E
	// KV Per-Shard Transactions
	OpKVBeginTxn    OpCode = 0x110
	OpKVCommitTxn   OpCode = 0x111
	OpKVRollbackTxn OpCode = 0x112
	// KV Extended (TTL, exists)
	OpKVExpire OpCode = 0x113
	OpKVExists OpCode = 0x115

	// ── Streams (0x130 – 0x14F) ──
	OpStreamAppend OpCode = 0x130
	OpStreamRead   OpCode = 0x131
	OpStreamTrim   OpCode = 0x132
	OpStreamInfo   OpCode = 0x133
	OpStreamList   OpCode = 0x13B
	OpStreamCreate OpCode = 0x13D
	OpStreamAlter  OpCode = 0x13F

	// ── Stream Consumer Groups (0x150 – 0x16F) ──
	OpStreamGroupCreate           OpCode = 0x150
	OpStreamGroupJoin             OpCode = 0x151
	OpStreamGroupLeave            OpCode = 0x152
	OpStreamGroupRead             OpCode = 0x153
	OpStreamGroupAck              OpCode = 0x154
	OpStreamGroupClaim            OpCode = 0x155
	OpStreamGroupPending          OpCode = 0x156
	OpStreamGroupConfigureSweeper OpCode = 0x157
	OpStreamGroupNack             OpCode = 0x159
	OpStreamGroupTouch            OpCode = 0x15A
	OpStreamGroupInfo             OpCode = 0x15B
	OpStreamGroupDelete           OpCode = 0x15C

	// ── Queues (0x170 – 0x19F) ──
	OpQueueEnqueue    OpCode = 0x170
	OpQueueDequeue    OpCode = 0x171
	OpQueueComplete   OpCode = 0x172
	OpQueueFail       OpCode = 0x174
	OpQueueDLQList    OpCode = 0x176
	OpQueueDLQDelete  OpCode = 0x177
	OpQueueDLQRequeue OpCode = 0x178
	OpQueueStats      OpCode = 0x17B
	OpQueuePeek       OpCode = 0x17C
	OpQueuePurge      OpCode = 0x17F
	OpQueueList       OpCode = 0x198

	// ── Time-Series (0x1A0 – 0x1BF) ──
	OpTSWrite     OpCode = 0x1A0
	OpTSRead      OpCode = 0x1A1
	OpTSQuery     OpCode = 0x1A2
	OpTSFloQL     OpCode = 0x1A3
	OpTSList      OpCode = 0x1A4
	OpTSDelete    OpCode = 0x1A5
	OpTSRetention OpCode = 0x1A6

	// ── Actions (0x300 – 0x31F) ──
	OpActionRegister OpCode = 0x300
	OpActionInvoke   OpCode = 0x301
	OpActionStatus   OpCode = 0x302
	OpActionList     OpCode = 0x303
	OpActionListRuns OpCode = 0x304
	OpActionDelete   OpCode = 0x305
	OpActionAwait    OpCode = 0x306
	OpActionComplete OpCode = 0x307
	OpActionFail     OpCode = 0x308
	OpActionTouch    OpCode = 0x309

	// ── Workers (0x320 – 0x33F) ──
	OpWorkerRegister   OpCode = 0x320
	OpWorkerHeartbeat  OpCode = 0x321
	OpWorkerDeregister OpCode = 0x322
	OpWorkerList       OpCode = 0x323
	OpWorkerInfo       OpCode = 0x324
	OpWorkerDrain      OpCode = 0x325

	// ── Workflows (0x340 – 0x35F) ──
	OpWorkflowCreate          OpCode = 0x340
	OpWorkflowStart           OpCode = 0x341
	OpWorkflowSignal          OpCode = 0x342
	OpWorkflowCancel          OpCode = 0x343
	OpWorkflowStatus          OpCode = 0x344
	OpWorkflowHistory         OpCode = 0x345
	OpWorkflowListRuns        OpCode = 0x346
	OpWorkflowGetDefinition   OpCode = 0x347
	OpWorkflowDisable         OpCode = 0x348
	OpWorkflowEnable          OpCode = 0x349
	OpWorkflowListDefinitions OpCode = 0x34A

	// ── Processing (0x360 – 0x37F) ──
	OpProcessingSubmit OpCode = 0x360
	OpProcessingStop   OpCode = 0x361
	OpProcessingCancel OpCode = 0x362
	OpProcessingStatus OpCode = 0x363
	OpProcessingList   OpCode = 0x364
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
	StatusUnavailable          StatusCode = 12 // No leader, or the shard isn't taking writes; retryable
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
	case StatusUnavailable:
		return "Unavailable: no leader or the shard isn't taking writes; retry"
	default:
		return fmt.Sprintf("Unknown status %d", uint8(s))
	}
}

// OptionTag represents option tags for TLV-encoded operation parameters.
type OptionTag uint8

const (
	// General Options (0x01 - 0x0F)
	OptLimit OptionTag = 0x05 // u32: Maximum number of results

	// Queue Options (0x10 - 0x1F)
	OptPriority OptionTag = 0x10 // u8: Message priority (0-255, lower is taken first)
	OptCount    OptionTag = 0x15 // u32: Number of messages to dequeue
	OptBlockMS  OptionTag = 0x17 // u32: Blocking timeout (0 = don't wait, max 300000)

	// Stream Options (0x20 - 0x2F) - StreamID-native ONLY
	// All stream positioning uses StreamID (timestamp_ms + sequence) - no legacy offset/timestamp modes
	// 0x20 reserved
	OptStreamStart   OptionTag = 0x21 // [16]u8: Start StreamID for reads (inclusive)
	OptStreamEnd     OptionTag = 0x22 // [16]u8: End StreamID for reads (inclusive)
	OptStreamTail    OptionTag = 0x23 // void: Flag indicating tail read (start from end)
	OptPartition     OptionTag = 0x24 // u32: Explicit partition index
	OptPartitionKey  OptionTag = 0x25 // string: Key for partition routing
	OptMaxAgeSeconds OptionTag = 0x26 // u64: Maximum age in seconds for retention
	OptDryRun        OptionTag = 0x28 // void: Flag to preview what would be deleted

	// Consumer Group Options (0x30 - 0x3F)
	OptAckTimeoutMS OptionTag = 0x30 // u32: Time before unacked message auto-redelivers
	OptMaxDeliver   OptionTag = 0x31 // u8: Max delivery attempts before DLQ (default: 10)

	// Time-Series Options (0x60 - 0x6F)
	OptTSFromMS      OptionTag = 0x60 // i64: Start of time range (inclusive, unix ms)
	OptTSToMS        OptionTag = 0x61 // i64: End of time range (inclusive, 0 = now)
	OptTSWindowMS    OptionTag = 0x62 // i64: Aggregation window size (ms)
	OptTSAggregation OptionTag = 0x63 // string: Aggregation function name (avg, sum, count, min, max)
	OptTSField       OptionTag = 0x64 // string: Field name filter (empty = "value")
	OptTSTags        OptionTag = 0x65 // string: Comma-separated tag filters "key=val,key2=val2"
	OptTSTimestamp   OptionTag = 0x67 // i64: Explicit timestamp for write (0 = server-assigned)
	OptTSRawTTL      OptionTag = 0x68 // string: Raw data TTL (e.g., "7d")
	OptTSDownsample  OptionTag = 0x69 // string: Downsample rule (e.g., "1m:avg:30d")
)

// ScanResult is one page of a KV scan: key names, and the cursor that
// reads the next page, nil on the last page.
type ScanResult struct {
	Keys   []string
	Cursor []byte
}

// VersionEntry is one version of a key from History.
type VersionEntry struct {
	Version uint64
	// AtMs is when the version was written, in Unix milliseconds.
	AtMs  int64
	Value []byte
}

// IncrResult is a counter's value and version after an Incr.
type IncrResult struct {
	Value   int64
	Version uint64
}

// PutResult is returned by KV writes that succeed. The Version field is the
// new version assigned to the key by the server, suitable for CAS on the next
// write via PutOptions.CASVersion.
type PutResult struct {
	Version uint64
}

// KVCommitResult is returned by a successful KV transaction commit. CommitIndex
// is the Raft log index of the committed batch; OpCount is the number of
// buffered operations that were applied atomically.
type KVCommitResult struct {
	CommitIndex uint64
	OpCount     uint16
}

// GetResult is returned by KV reads that find a key. Get returns nil when the
// key is missing; callers should check for nil before dereferencing.
type GetResult struct {
	Value   []byte
	Version uint64
}

// MGetEntry is one entry in an MGet response. Found is false when the key did
// not exist; in that case Value is nil and Version is 0.
type MGetEntry struct {
	Key     string
	Value   []byte
	Version uint64
	Found   bool
}

// KVMGetOptions contains options for KV MGet operations.
type KVMGetOptions struct {
	Namespace string
	// RoutingKey, when set, is the routing key every key was written with:
	// all are looked up on its shard. Without it each key is looked up on
	// its own.
	RoutingKey string
}

// Message represents a queue message.
type Message struct {
	Seq           uint64
	Payload       []byte
	EnqueuedAtMS  int64
	DeliveryCount uint32
	Priority      uint8
}

// DequeueResult represents the result of a queue dequeue operation.
type DequeueResult struct {
	Messages []Message
}

// GetOptions contains options for KV get operations.
type GetOptions struct {
	Namespace string
	// WaitMs waits up to this long for what WaitFor names (0 = don't wait,
	// at most MaxWaitMS).
	WaitMs uint64
	// WaitFor is what a wait waits for: the key to exist (the default), or
	// its next version.
	WaitFor WaitFor
	// RoutingKey routes the request in place of the key.
	RoutingKey string
}

// WaitFor is what a Get with WaitMs waits for.
type WaitFor uint8

const (
	// WaitForExists answers at once if the key exists, else once it's written.
	WaitForExists WaitFor = 0
	// WaitForChange answers the key's next version.
	WaitForChange WaitFor = 1
)

// MaxWaitMS is the longest a Get may wait.
const MaxWaitMS = 300000

// PutOptions contains options for KV put operations.
type PutOptions struct {
	Namespace string
	TTLMs     *uint64 // Time-to-live in milliseconds (nil or 0 = no expiration)
	// Condition says when the write may happen.
	Condition Condition
	// CASVersion, when set, must be the key's version; 0 means it doesn't exist.
	CASVersion *uint64
	// RoutingKey routes the request in place of the key.
	RoutingKey string
}

// Condition says when a Put may write.
type Condition uint8

const (
	ConditionAlways    Condition = 0
	ConditionIfAbsent  Condition = 1
	ConditionIfPresent Condition = 2
)

// DeleteOptions contains options for KV delete operations.
type DeleteOptions struct {
	Namespace string
	// CASVersion, when set, must be the key's version: only the owner
	// deletes a lock or lease.
	CASVersion *uint64
	// RoutingKey routes the request in place of the key.
	RoutingKey string
}

// ScanOptions contains options for KV scan operations.
type ScanOptions struct {
	Namespace string
	// Cursor is the previous page's Cursor; nil reads the first page.
	Cursor []byte
	// Limit is the most names in the page: 1..MaxScanLimit, default
	// MaxScanLimit.
	Limit *uint32
}

// HistoryOptions contains options for KV history operations.
type HistoryOptions struct {
	Namespace string
	// Limit is the most versions returned: 1..MaxHistoryLimit, default 10.
	Limit *uint32
	// RoutingKey routes the request in place of the key.
	RoutingKey string
}

// EnqueueOptions contains options for queue enqueue operations.
type EnqueueOptions struct {
	Namespace string
	Priority  uint8 // Lower is taken first
}

// DequeueOptions contains options for queue dequeue operations.
type DequeueOptions struct {
	Namespace string
	BlockMS   *uint32 // Long-poll wait (nil or 0 = don't wait, max 300000)
}

// AckOptions contains options for queue ack operations.
type AckOptions struct {
	Namespace string
}

// NackOptions contains options for queue nack operations.
type NackOptions struct {
	Namespace string
}

// DLQListOptions contains options for DLQ list operations.
type DLQListOptions struct {
	Namespace string
}

// DLQRequeueOptions contains options for DLQ requeue operations.
type DLQRequeueOptions struct {
	Namespace string
}

// PeekOptions contains options for queue peek operations.
type PeekOptions struct {
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

// StorageTier is the tier a stream record was served from. These are the
// only two the server sends.
type StorageTier uint8

const (
	StorageTierHot  StorageTier = 0 // the in-memory log
	StorageTierWarm StorageTier = 1 // the warm store, after the record left the log
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

// PendingEntry describes one entry in a consumer group's Pending Entry List
// (delivered-but-unacked message).
type PendingEntry struct {
	ID            StreamID
	Consumer      string
	DeliveryCount uint32
}

// StreamClaimResult is one page of a GroupClaim cursor scan.
//
// Records carry payload + headers (same shape as GroupRead). NextCursor is the
// start_id to pass on the next GroupClaim call; when Done is true the PEL has
// been fully scanned and the loop should stop.
type StreamClaimResult struct {
	Records    []StreamRecord
	NextCursor StreamID
	Done       bool
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
	BlockMS   *uint32   // Long-poll wait (nil or 0 = don't wait, max 300000)
}

// StreamTrimOptions contains options for stream trim operations. Set exactly
// one of Before, MaxLen or MaxAgeSeconds; the server refuses none or several.
type StreamTrimOptions struct {
	Namespace     string
	Before        *StreamID // Remove records up to and including this id
	MaxLen        *uint64   // Keep only the newest N records (> 0)
	MaxAgeSeconds *uint64   // Remove records older than this (> 0)
	DryRun        bool      // Report what would be removed, removing nothing
}

// StreamTrimResult is what a trim removed, or with DryRun would remove.
type StreamTrimResult struct {
	Removed  uint64 // Records removed
	FirstSeq uint64 // Sequence of the first record left; meaningful only when records remain
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
	BlockMS   *uint32 // Long-poll wait (nil or 0 = don't wait, max 300000)
}

// StreamGroupAckOptions contains options for consumer group ack.
type StreamGroupAckOptions struct {
	Namespace string
	Consumer  string // Consumer ID; sent, but the server doesn't check it: any consumer can ack an entry
}

// StreamGroupNackOptions contains options for consumer group nack.
type StreamGroupNackOptions struct {
	Namespace string
	Consumer  string // Consumer ID; sent, but the server doesn't check it: any consumer can nack an entry
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
	RunID       string
	Status      RunStatus
	CreatedAt   int64  // ms since the epoch
	StartedAt   *int64 // ms; nil until a worker first claims it
	CompletedAt *int64 // ms; nil until it finishes
	// Attempt is how many attempts have begun, the current one included.
	Attempt uint32
	// Output is what the worker completed it with; nil until it completes.
	Output []byte
	// ErrorMessage is why its last attempt failed, if one did.
	ErrorMessage string
	// WorkerID is the worker holding the run, or that last held it; "" until
	// one takes it. Only the leader knows a take at once: another node
	// learns it at the run's next update.
	WorkerID string
}

// ActionRegisterOptions contains options for action registration.
// Registering replaces the action's settings: the last registration wins.
type ActionRegisterOptions struct {
	Namespace string
	// TimeoutMS is how long a run may go without its worker completing,
	// failing or touching it before it's taken back; 0 means never. nil
	// sends 30000, the server's default today.
	TimeoutMS *uint32
	// MaxRetries counts attempts in all, the first included. nil sends 3,
	// the server's default today.
	MaxRetries *uint32
	Owner      string
}

func (o *ActionRegisterOptions) timeoutMS() uint32 {
	if o.TimeoutMS == nil {
		return 30000
	}
	return *o.TimeoutMS
}

// ActionInvokeOptions contains options for action invocation.
type ActionInvokeOptions struct {
	Namespace string
	Labels    string // Required worker labels as a JSON object, e.g. {"gpu":true}; empty means none
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

// WorkerAwaitOptions contains options for action_await (blocking wait for task).
type WorkerAwaitOptions struct {
	Namespace string
	BlockMS   *uint32 // Long-poll wait (nil = 30000, 0 = don't wait, max 300000)
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
	// Limit is the most workers on the page, at most MaxWorkerListLimit;
	// 0 means MaxWorkerListLimit.
	Limit uint32
	// Cursor is the previous page's Cursor; nil starts from the first worker.
	Cursor []byte
}

// WorkerListResult is one page of a namespace's workers, in id order.
type WorkerListResult struct {
	Workers []WorkerInfo
	// Cursor is where the next page starts; nil on the last page.
	Cursor []byte
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

// WorkerInfo is a registered worker, as worker.list shows it.
type WorkerInfo struct {
	ID     string
	Type   WorkerType
	Status WorkerStatus
	// CurrentLoad is the running handlers the worker last reported; 0 once
	// it is unhealthy.
	CurrentLoad     uint32
	MaxConcurrency  uint32
	TasksCompleted  uint64
	TasksFailed     uint64
	RegisteredAtMS  int64
	LastHeartbeatMS int64
	MachineID       string // "" when the worker gave none
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

// ProcessingSyncOptions contains options for declarative processing sync.
type ProcessingSyncOptions struct {
	Namespace string
}

// ProcessingStatusResult contains the status of a processing job.
type ProcessingStatusResult struct {
	JobID            string
	Name             string
	Status           string
	BatchSize        uint32
	RecordsProcessed uint64
	CreatedAt        int64
}

// ProcessingListEntry represents a single entry in a processing job list.
type ProcessingListEntry struct {
	Name      string
	JobID     string
	Status    string
	CreatedAt int64
}

// Reason says why the server refused a request. It mirrors the server's
// list; the table hash covers it, so a server can't send one this SDK
// doesn't know.
type Reason uint16

const (
	ReasonUnknownOp           Reason = 1
	ReasonMalformed           Reason = 2
	ReasonFieldTooLong        Reason = 3
	ReasonOutOfRange          Reason = 4
	ReasonLeaderUnknown       Reason = 5
	ReasonShardBusy           Reason = 6
	ReasonTableMismatch       Reason = 7
	ReasonUnknownFlag         Reason = 8
	ReasonInternal            Reason = 9
	ReasonCommittedNotApplied Reason = 10
	ReasonAnswerLost          Reason = 11
	// ReasonUnclassified is a refusal from an op family the server hasn't
	// classified yet; its status and message say what happened.
	ReasonUnclassified Reason = 12
	// ReasonNotFound: what the request names doesn't exist.
	ReasonNotFound Reason = 13
	// ReasonReservedKey: the key is in a prefix the server keeps for itself.
	ReasonReservedKey Reason = 14
	// ReasonCASMismatch: the write's CASVersion isn't the key's version.
	ReasonCASMismatch Reason = 15
	// ReasonConditionFailed: the write's Condition didn't hold.
	ReasonConditionFailed Reason = 16
	// ReasonNoSuchTransaction: no open transaction has this id here.
	ReasonNoSuchTransaction Reason = 17
	// ReasonWrongRoutingKey: the request's routing key (its key, when it gives
	// none) isn't the one its transaction was begun with.
	ReasonWrongRoutingKey Reason = 18
	// ReasonTxnTooLarge: the transaction holds as many ops or bytes as it may.
	ReasonTxnTooLarge Reason = 19
	// ReasonTooManyWaiters: the shard holds as many waiting reads as it may.
	ReasonTooManyWaiters Reason = 20
	// ReasonConflictingFields: two fields given together that don't go together.
	ReasonConflictingFields Reason = 21
	// ReasonWritesStopped: the shard stopped taking writes until the node restarts.
	ReasonWritesStopped Reason = 22
	// ReasonTooManyProposals: too many writes wait for commit.
	ReasonTooManyProposals Reason = 23
	// ReasonWrongType: the key's value isn't what the op works on (a counter,
	// a JSON document).
	ReasonWrongType Reason = 24
	// ReasonTooManyTransactions: the shard holds as many open transactions
	// as it may.
	ReasonTooManyTransactions Reason = 25
	// ReasonStaleReport: an action complete or fail for a run that has moved
	// on (another worker holds it, or it finished). It was dropped; resending
	// it is refused the same way.
	ReasonStaleReport Reason = 26
	// ReasonWorkerUnhealthy: an await from a worker that missed its
	// heartbeats; a heartbeat, or registering again, lets it take work.
	ReasonWorkerUnhealthy Reason = 27
	// ReasonWorkerDraining: an await from a worker that is draining.
	ReasonWorkerDraining Reason = 28
)

var reasonNames = map[Reason]string{
	ReasonUnknownOp: "unknown_op", ReasonMalformed: "malformed", ReasonFieldTooLong: "field_too_long",
	ReasonOutOfRange: "out_of_range", ReasonLeaderUnknown: "leader_unknown", ReasonShardBusy: "shard_busy",
	ReasonTableMismatch: "table_mismatch", ReasonUnknownFlag: "unknown_flag", ReasonInternal: "internal",
	ReasonCommittedNotApplied: "committed_not_applied", ReasonAnswerLost: "answer_lost", ReasonUnclassified: "unclassified",
	ReasonNotFound: "not_found", ReasonReservedKey: "reserved_key", ReasonCASMismatch: "cas_mismatch",
	ReasonConditionFailed: "condition_failed", ReasonNoSuchTransaction: "no_such_transaction",
	ReasonWrongRoutingKey: "wrong_routing_key", ReasonTxnTooLarge: "txn_too_large", ReasonTooManyWaiters: "too_many_waiters",
	ReasonConflictingFields: "conflicting_fields", ReasonWritesStopped: "writes_stopped", ReasonTooManyProposals: "too_many_proposals",
	ReasonWrongType: "wrong_type", ReasonTooManyTransactions: "too_many_transactions",
	ReasonStaleReport: "stale_report", ReasonWorkerUnhealthy: "worker_unhealthy", ReasonWorkerDraining: "worker_draining",
}

func (r Reason) String() string {
	if n, ok := reasonNames[r]; ok {
		return n
	}
	return fmt.Sprintf("reason(%d)", uint16(r))
}

// Ran says whether a refused request took effect: retry logic reads this,
// not the message.
type Ran uint8

const (
	RanNo Ran = 0
	// RanYes: it took effect; don't resend it.
	RanYes Ran = 1
	// RanUnknown: it may still take effect; check before resending.
	RanUnknown Ran = 2
)
