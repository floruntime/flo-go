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
	"strconv"
	"strings"
)

// Protocol constants
const (
	Magic      uint32 = 0x004F4C46 // "FLO\0" in little-endian
	Version    uint8  = 0x03
	HeaderSize        = 32

	// TableHash is the hash of the op table this SDK was built for, written
	// into every request header. A server built from another table refuses
	// the request, and its answers carry its own hash, which this client
	// refuses. CI's live tests run against the flo commit in
	// .github/flo-ref.env: change both in one commit.
	TableHash uint64 = 0xc688167b51fcc846

	// Size limits (for client-side validation)
	MaxNamespaceSize = 255
	MaxKeySize       = 64 * 1024        // 64 KB
	MaxValueSize     = 16 * 1024 * 1024 // 16 MB practical limit

	// maxAnswerData is the most data one answer carries: the server's
	// largest answer frame, 256 KiB, less its header.
	maxAnswerData = 256*1024 - HeaderSize

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
	OpNamespaceCreate OpCode = 0x010
	OpNamespaceDelete OpCode = 0x011
	OpNamespaceList   OpCode = 0x012
	OpNamespaceInfo   OpCode = 0x013
	// OpNamespaceEnsure is sent shard to shard, never by a client.
	OpNamespaceEnsure OpCode = 0x016

	// ── Cluster (0x030 – 0x04F) ──
	OpClusterStatus  OpCode = 0x030
	OpClusterMembers OpCode = 0x031
	OpClusterPromote OpCode = 0x032
	OpClusterRemove  OpCode = 0x033
	// OpClusterRepairSkip is served only on a node's admin socket.
	OpClusterRepairSkip OpCode = 0x034

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
	OpStreamDelete OpCode = 0x140

	// ── Stream Consumer Groups (0x150 – 0x16F) ──
	OpStreamGroupCreate  OpCode = 0x150
	OpStreamGroupRead    OpCode = 0x153
	OpStreamGroupAck     OpCode = 0x154
	OpStreamGroupPending OpCode = 0x156
	OpStreamGroupNack    OpCode = 0x159
	OpStreamGroupTouch   OpCode = 0x15A
	OpStreamGroupInfo    OpCode = 0x15B
	OpStreamGroupDelete  OpCode = 0x15C
	OpStreamGroupList    OpCode = 0x15D
	OpStreamGroupMembers OpCode = 0x15E

	// ── Queues (0x170 – 0x19F) ──
	OpQueueEnqueue    OpCode = 0x170
	OpQueueDLQList    OpCode = 0x176
	OpQueueDLQDelete  OpCode = 0x177
	OpQueueDLQRequeue OpCode = 0x178
	OpQueueStats      OpCode = 0x17B
	OpQueuePurge      OpCode = 0x17F
	OpQueuePeek       OpCode = 0x180
	OpQueueDequeue    OpCode = 0x181
	OpQueueComplete   OpCode = 0x182
	OpQueueFail       OpCode = 0x183
	OpQueueTouch      OpCode = 0x184
	OpQueueList       OpCode = 0x198

	// ── Time-Series (0x1A0 – 0x1BF) ──
	OpTSWrite     OpCode = 0x1A0
	OpTSRead      OpCode = 0x1A1
	OpTSQuery     OpCode = 0x1A2
	OpTSFloQL     OpCode = 0x1A3
	OpTSList      OpCode = 0x1A4
	OpTSDelete    OpCode = 0x1A5
	OpTSRetention OpCode = 0x1A6
	OpTSInfo      OpCode = 0x1A7

	// ── Actions (0x300 – 0x31F) ──
	OpActionRegister OpCode = 0x300
	OpActionInvoke   OpCode = 0x301
	OpActionStatus   OpCode = 0x302
	OpActionList     OpCode = 0x303
	OpActionListRuns OpCode = 0x304
	OpActionDelete   OpCode = 0x305
	OpActionAwait    OpCode = 0x30A
	OpActionComplete OpCode = 0x30B
	OpActionFail     OpCode = 0x30C
	OpActionTouch    OpCode = 0x30D
	OpActionInfo     OpCode = 0x30F

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
	OpProcessingRemove OpCode = 0x365
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
	// StatusOutcomePending: the write committed but hasn't applied, because
	// its shard halted at or before it. Never resend it unchanged.
	StatusOutcomePending StatusCode = 13
	// StatusNoNamespace: the namespace the request names doesn't exist.
	// Not StatusNotFound, which is a missing key.
	StatusNoNamespace StatusCode = 14
	// StatusLimit: the node is at a limit it holds to (ErrLimit). The
	// reason names which; it never ran.
	StatusLimit StatusCode = 15
)

// ResponseStale is the answer flag on a read served by a shard that can't
// apply what its group committed: halted, diverged, or blocked reading the
// log.
const ResponseStale uint8 = 0x01

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
	case StatusOutcomePending:
		return "Committed, not applied: its shard halted; don't resend it unchanged"
	case StatusNoNamespace:
		return "No such namespace"
	case StatusLimit:
		return "At a limit: delete something, give the node more room, or raise the setting"
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
	OptDelayMS  OptionTag = 0x11 // u64: How long before an enqueued message is ready (at most a day)
	OptCount    OptionTag = 0x15 // u32: Most records a stream read returns
	OptBlockMS  OptionTag = 0x17 // u32: Blocking timeout (0 = don't wait, max 300000)

	// Stream Options (0x20 - 0x2F) - StreamID-native ONLY
	// All stream positioning uses StreamID (timestamp_ms + sequence) - no legacy offset/timestamp modes
	// 0x20 reserved
	OptStreamStart   OptionTag = 0x21 // [16]u8: Read the records after this StreamID (exclusive)
	OptStreamEnd     OptionTag = 0x22 // [16]u8: End StreamID for reads (inclusive)
	OptStreamTail    OptionTag = 0x23 // void: Flag indicating tail read (start from end)
	OptPartition     OptionTag = 0x24 // u32: Explicit partition index
	OptPartitionKey  OptionTag = 0x25 // string: Key for partition routing
	OptMaxAgeSeconds OptionTag = 0x26 // u64: Maximum age in seconds for retention
	OptDryRun        OptionTag = 0x28 // void: Flag to preview what would be deleted
	OptForce         OptionTag = 0x2C // void: Allow a destructive op, e.g. deleting a non-empty stream

	// Consumer Group Options (0x30 - 0x3F)
	OptAckTimeoutMS OptionTag = 0x30 // u32: A group's lease: a record not acked this long is delivered again (5 s to 12 h)
	OptMaxDeliver   OptionTag = 0x31 // u8: A group's attempts: a record nacked or expired this many times is dead (1 to 255)
	OptDead         OptionTag = 0x32 // void: A nack dead-letters its records now
	OptErrorText    OptionTag = 0x33 // string: Why a nack's records failed

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
	// Stale: the read was served by a shard behind what its group
	// committed (halted, or blocked reading the log), from its state then.
	Stale bool
}

// HistoryResult is one page of a key's versions, newest first.
type HistoryResult struct {
	Versions []VersionEntry
	// More says older versions follow: ask again with BeforeVersion set to
	// the last version here.
	More bool
	// Stale: the read was served by a shard behind what its group
	// committed (halted, or blocked reading the log), from its state then.
	Stale bool
}

// ExistsResult says whether a key is present.
type ExistsResult struct {
	Exists bool
	// Stale: the read was served by a shard behind what its group
	// committed (halted, or blocked reading the log), from its state then.
	Stale bool
}

// VersionEntry is one version of a key from History.
type VersionEntry struct {
	Version uint64
	// AtMs is when the version was written, in Unix milliseconds.
	AtMs int64
	// Deleted is set on the version a delete wrote.
	Deleted bool
	Value   []byte
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
	// ExpiresAtMs is when the key's TTL runs out, in Unix milliseconds; nil
	// when it has none, and always nil from JsonGet.
	ExpiresAtMs *int64
	// Stale: the read was served by a shard behind what its group committed:
	// halted, from its state just before the entry it halted at, or blocked
	// reading the log, or diverged. The read succeeded; later writes aren't
	// in it.
	Stale bool
}

// MGetEntry is one entry in an MGet response. Found is false when the key did
// not exist; in that case Value is nil and Version is 0.
type MGetEntry struct {
	Key     string
	Value   []byte
	Version uint64
	Found   bool
	// Stale: the read was served by a shard behind what its group
	// committed (halted, or blocked reading the log), from its state then.
	Stale bool
}

// KVMGetOptions contains options for KV MGet operations.
type KVMGetOptions struct {
	Namespace string
}

// Message is a dequeued queue message. With its dequeue's QueueLease, Seq
// and Deliveries name its delivery: Complete, Fail and Touch name it back.
type Message struct {
	Seq      uint64
	Priority uint32
	// Deliveries counts the message's deliveries, this one included.
	Deliveries   uint32
	EnqueuedAtMS int64
	Payload      []byte
}

// Item is how a report names the message: its seq and the deliveries it
// was dequeued with.
func (m Message) Item() QueueItem { return QueueItem{Seq: m.Seq, Deliveries: m.Deliveries} }

// QueueItem names one delivered message in a report.
type QueueItem struct {
	Seq        uint64
	Deliveries uint32
}

// QueueLease names a dequeue's delivery: the consumer that took the
// messages and the log position the delivery was made at. A report must
// name the same.
type QueueLease struct {
	Consumer string
	Term     uint64
	Index    uint64
}

// DequeueResult is what a dequeue took: its messages, leased to the
// consumer until completed, failed or expired.
type DequeueResult struct {
	Messages []Message
	Lease    QueueLease
	// ExpiresAtMS is when the lease runs out without a touch, in Unix
	// milliseconds; 0 when nothing was taken, or with Ack.
	ExpiresAtMS int64
}

// Items names every message taken, for a report on all of them.
func (r *DequeueResult) Items() []QueueItem {
	items := make([]QueueItem, len(r.Messages))
	for i, m := range r.Messages {
		items[i] = m.Item()
	}
	return items
}

// QueueRefusalWhy says why a report applied nothing to a message.
type QueueRefusalWhy uint8

const (
	// QueueGone: no such message in the queue. For a complete it means
	// done: a resent complete whose first applied is answered this.
	QueueGone QueueRefusalWhy = 0
	// QueueDeliveredAgain: its lease ran out or it was failed, and it was
	// dequeued again; this delivery is over.
	QueueDeliveredAgain QueueRefusalWhy = 1
	// QueueOtherHolder: another consumer holds this delivery.
	QueueOtherHolder QueueRefusalWhy = 2
	// QueueExpired: its lease ran out, and it is ready again or dead.
	QueueExpired QueueRefusalWhy = 3
	// QueueReleased: it was failed, and is ready again or dead.
	QueueReleased QueueRefusalWhy = 4
	// QueueNotHeld: it isn't leased.
	QueueNotHeld QueueRefusalWhy = 5
)

var queueRefusalNames = [...]string{"gone", "delivered_again", "other_holder", "expired", "released", "not_held"}

func (w QueueRefusalWhy) String() string {
	if int(w) < len(queueRefusalNames) {
		return queueRefusalNames[w]
	}
	return fmt.Sprintf("why(%d)", uint8(w))
}

// QueueRefusal is a message a report applied nothing to.
type QueueRefusal struct {
	Seq uint64
	Why QueueRefusalWhy
	// Reason says the same for a person; read Why, not this.
	Reason string
}

// QueueReport is a complete's, fail's or touch's answer: per message, the
// server applies it or says why not, in an ok answer.
type QueueReport struct {
	Applied uint32
	Refused []QueueRefusal
	// ExpiresAtMS, for a touch, is when the lease runs out without another.
	ExpiresAtMS int64
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
	// BeforeVersion, when set, returns only versions older than it: the
	// previous page's last version reads the next page.
	BeforeVersion *uint64
}

// EnqueueOptions contains options for queue enqueue operations.
type EnqueueOptions struct {
	Namespace string
	Priority  uint8 // Lower is taken first
	// DelayMS is how long before the message is ready, at most
	// MaxQueueDelayMS.
	DelayMS uint64
}

// MaxQueueDelayMS is the longest an enqueue or a fail may delay a message.
const MaxQueueDelayMS = 24 * 60 * 60 * 1000

// DequeueOptions contains options for queue dequeue operations.
type DequeueOptions struct {
	Namespace string
	// Consumer is who takes the messages; a report on them must name the
	// same, and the result's Lease carries it.
	Consumer string
	// LeaseMS is how long the messages are held without a touch,
	// MinLeaseMS..MaxLeaseMS; nil is the server's 30 s.
	LeaseMS *uint64
	BlockMS *uint32 // Long-poll wait (nil or 0 = don't wait, max 300000)
	// Ack completes the messages as they're taken, a one-shot consume. If
	// the leader fails in between, they come back once their lease runs out.
	Ack bool
}

// QueueReportOptions contains options for a queue complete or touch.
type QueueReportOptions struct {
	Namespace string
}

// QueueFailOptions contains options for a queue fail.
type QueueFailOptions struct {
	Namespace string
	// DelayMS is how long before the messages are ready again, at most
	// MaxQueueDelayMS.
	DelayMS uint64
	// Dead dead-letters them now, whatever attempts are left.
	Dead bool
	// Error says why they failed, at most 1024 bytes.
	Error string
}

// PurgeOptions contains options for a queue purge.
type PurgeOptions struct {
	Namespace string
}

// DLQListOptions contains options for DLQ list operations.
type DLQListOptions struct {
	Namespace string
	// Limit is the most dead letters on the page, 1..MaxDLQPage; 0 means
	// MaxDLQPage.
	Limit uint32
	// After, when set, starts the page after the dead letter with this seq:
	// the previous page's LastSeq.
	After *uint64
}

// MaxDLQPage is the most dead letters one DLQList page holds.
const MaxDLQPage = 100

// DeadLetter is one message in a queue's dead letter queue.
type DeadLetter struct {
	Seq uint64
	// Size is the payload's whole length; Payload holds at most its first
	// 4 KiB.
	Size         uint32
	Payload      []byte
	EnqueuedAtMS int64
	Attempts     uint32
	Priority     uint8
	MovedAtMS    int64
}

// DLQPage is one page of a queue's dead letters, oldest first.
type DLQPage struct {
	Messages []DeadLetter
	// Total is how many dead letters the queue holds.
	Total uint64
	// HasMore says another page follows: ask again with After set to LastSeq.
	HasMore bool
	// LastSeq is the seq of the page's last message; 0 on an empty page.
	LastSeq uint64
}

// PeekOptions contains options for queue peek operations.
type PeekOptions struct {
	Namespace string
}

// MaxPeekLimit is the most messages one Peek may ask for.
const MaxPeekLimit = 100

// PeekedMessage is a ready message, as Peek shows it.
type PeekedMessage struct {
	Seq          uint64
	Priority     uint32
	Attempts     uint32
	EnqueuedAtMS int64
	// Size is the payload's whole length; Payload holds at most its first
	// 4 KiB.
	Size    uint32
	Payload []byte
}

// PeekResult is the next ready messages of a queue, in the order a dequeue
// takes them.
type PeekResult struct {
	Messages []PeekedMessage
	// More says more may follow: the limit, the answer's room or a run of
	// leased messages stopped it.
	More bool
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

// StorageTier is the tier a stream record was served from. A code this
// SDK doesn't know is kept as it came; String names it unknown.
type StorageTier uint8

const (
	StorageTierHot  StorageTier = 0 // the in-memory log
	StorageTierWarm StorageTier = 1 // the warm store, after the record left the log
	StorageTierDisk StorageTier = 2 // read back from the log's segments on disk, past the warm store
)

func (t StorageTier) String() string {
	switch t {
	case StorageTierHot:
		return "hot"
	case StorageTierWarm:
		return "warm"
	case StorageTierDisk:
		return "disk"
	}
	return fmt.Sprintf("unknown(%d)", uint8(t))
}

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

// PendingEntry is one of a consumer group's leased records, as
// GroupPending lists it.
type PendingEntry struct {
	ID       StreamID
	Consumer string
	// Deliveries counts the record's deliveries, this one included.
	Deliveries uint32
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
	Start     *StreamID // Read the records after this id (exclusive): pass the last id read
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

// StreamDeleteOptions contains options for stream delete operations.
type StreamDeleteOptions struct {
	Namespace string
	Force     bool // Delete the stream even if it still holds records
}

// StreamInfoOptions contains options for stream info operations.
type StreamInfoOptions struct {
	Namespace string
}

// StreamGroup is one consumer group, as GroupList shows it.
type StreamGroup struct {
	Name string
	// Members counts the consumers holding its records now.
	Members uint32
	// Pending counts its records ready or leased, not yet acked.
	Pending     uint64
	CreatedAtMS int64
	// AckTimeoutMS is its lease.
	AckTimeoutMS uint32
	// MaxDeliver is how many times a record is nacked or expires before
	// it is dead.
	MaxDeliver uint8
	// LastDeliveredMS and LastDeliveredSeq are its cursor: the last of the
	// stream's records it has delivered.
	LastDeliveredMS  uint64
	LastDeliveredSeq uint64
}

// StreamGroupList is a stream's consumer groups.
type StreamGroupList struct {
	Groups []StreamGroup
	// More says the stream has more groups than the answer holds.
	More bool
}

// StreamGroupMember is one member of a consumer group: a consumer holding
// its records now. One with nothing leased isn't a member.
type StreamGroupMember struct {
	ID string
	// Leaving is a state the answer can carry; the server sends members
	// active.
	Leaving bool
	// Pending counts the records leased to it.
	Pending uint32
	// JoinedAtMS is when the earliest of them was delivered.
	JoinedAtMS int64
	// LastActiveMS is when the latest of them was delivered.
	LastActiveMS *int64
}

// StreamGroupMembers is a consumer group's members.
type StreamGroupMembers struct {
	Members []StreamGroupMember
	// More says the group has more members than the answer holds.
	More bool
}

// StreamGroupCreateOptions contains options for GroupCreate.
type StreamGroupCreateOptions struct {
	Namespace string
	// AckTimeoutMS is the group's lease, MinGroupAckTimeoutMS to
	// MaxGroupAckTimeoutMS; nil takes the server's 30 s.
	AckTimeoutMS *uint32
	// MaxDeliver is how many times a record is nacked or expires before it
	// is dead, 1 to 255; nil takes the server's 5.
	MaxDeliver *uint8
}

// StreamGroupReadOptions contains options for consumer group read.
type StreamGroupReadOptions struct {
	Namespace string
	// Count is the most records to lease; nil, 0 or past MaxGroupBatch
	// leases up to MaxGroupBatch.
	Count   *uint32
	BlockMS *uint32 // Long-poll wait (nil or 0 = don't wait, max 300000)
}

// StreamGroupLease names a group read's delivery: the consumer that took
// the records and the log position the delivery was made at. A report
// names the same.
type StreamGroupLease struct {
	Consumer string
	Term     uint64
	Index    uint64
}

// StreamGroupRecord is a record a group read leased.
type StreamGroupRecord struct {
	StreamRecord
	// Deliveries counts the record's deliveries, this one included.
	Deliveries uint32
}

// Item is how a report names the record: its id and the deliveries it was
// read with.
func (r StreamGroupRecord) Item() StreamGroupItem {
	return StreamGroupItem{ID: r.ID, Deliveries: r.Deliveries}
}

// StreamGroupItem names one leased record in a report.
type StreamGroupItem struct {
	ID         StreamID
	Deliveries uint32
}

// StreamGroupReadResult is what a group read leased: its records, until
// acked, nacked or expired.
type StreamGroupReadResult struct {
	Records []StreamGroupRecord
	Lease   StreamGroupLease
	// ExpiresAtMS is when the lease runs out without a touch, in Unix
	// milliseconds; 0 when nothing was leased.
	ExpiresAtMS int64
}

// Items names every record leased, for a report on all of them.
func (r *StreamGroupReadResult) Items() []StreamGroupItem {
	items := make([]StreamGroupItem, len(r.Records))
	for i, rec := range r.Records {
		items[i] = rec.Item()
	}
	return items
}

// GroupRefusalWhy says why a group report applied nothing to a record. The
// values are a queue report's: QueueGone (no such record in the group:
// acked already, or never leased), QueueDeliveredAgain, QueueOtherHolder,
// QueueExpired, QueueReleased (nacked) and QueueNotHeld.
type GroupRefusalWhy = QueueRefusalWhy

// StreamGroupRefusal is a record a group report applied nothing to.
type StreamGroupRefusal struct {
	ID  StreamID
	Why GroupRefusalWhy
}

// StreamGroupReport is an ack's, nack's or touch's answer: per record, the
// server applies it or says why not, in an ok answer. A stale report (one
// naming a delivery that moved on) applies nothing and is answered so.
type StreamGroupReport struct {
	Applied uint32
	Refused []StreamGroupRefusal
	// ExpiresAtMS, for a touch, is when the lease runs out without another.
	ExpiresAtMS int64
}

// StreamGroupAckOptions contains options for consumer group ack and touch.
type StreamGroupAckOptions struct {
	Namespace string
}

// StreamGroupNackOptions contains options for consumer group nack.
type StreamGroupNackOptions struct {
	Namespace string
	// DelayMS is how long before the records are ready again, at most
	// MaxQueueDelayMS.
	DelayMS uint64
	// Dead makes them dead now, whatever attempts are left.
	Dead bool
	// Error says why they failed, at most 255 bytes.
	Error string
}

// StreamGroupInfo is a consumer group's counts. Ready, Leased, Dead and
// Trimmed add up to what the group holds; Trimmed counts records at or
// below a stream trim that the server hasn't removed yet.
type StreamGroupInfo struct {
	// Pending is Ready plus Leased.
	Pending uint64
	// Holders counts the consumers holding its records now.
	Holders     uint32
	CreatedAtNS uint64
	Ready       uint64
	Leased      uint64
	Dead        uint64
	Trimmed     uint64
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

// Delivery names one delivery of a run to a worker. A complete, fail or
// touch names it back; one naming any other delivery of the run is refused
// as stale (IsStaleReport).
type Delivery struct {
	RunID string
	// Action is the action the run invokes; a report must name it.
	Action string
	// Deliveries counts the run's deliveries, this one included.
	Deliveries uint32
	// Term and Index are where in the log the delivery was made.
	Term  uint64
	Index uint64
}

// TaskID is the delivery as the flo CLI prints and parses it:
// run:deliveries:term:index:action.
func (d Delivery) TaskID() string {
	return fmt.Sprintf("%s:%d:%d:%d:%s", d.RunID, d.Deliveries, d.Term, d.Index, d.Action)
}

// ParseTaskID reads a task id as TaskID writes it. The action comes last,
// so it may hold colons; a run id holds none.
func ParseTaskID(s string) (Delivery, error) {
	parts := strings.SplitN(s, ":", 5)
	bad := fmt.Errorf("flo: %q is not a task id (run:deliveries:term:index:action)", s)
	if len(parts) != 5 || parts[0] == "" || parts[4] == "" {
		return Delivery{}, bad
	}
	deliveries, err1 := strconv.ParseUint(parts[1], 10, 32)
	term, err2 := strconv.ParseUint(parts[2], 10, 64)
	index, err3 := strconv.ParseUint(parts[3], 10, 64)
	if err1 != nil || err2 != nil || err3 != nil {
		return Delivery{}, bad
	}
	return Delivery{RunID: parts[0], Deliveries: uint32(deliveries), Term: term, Index: index, Action: parts[4]}, nil
}

// Task is a delivery of a run to a worker, as an await answers it.
type Task struct {
	Delivery
	// Namespace is the run's namespace.
	Namespace   string
	CreatedAtMS int64
	// Attempt is the run's failures so far, plus one. A fail with Abandoned
	// set counts none, so the next delivery has the same attempt.
	Attempt uint32
	// LeaseMS is how long the worker holds the run without a touch.
	LeaseMS uint64
	// CallerRunID and CallerWorkflowName name the workflow run that invoked
	// it; empty when none did.
	CallerRunID        string
	CallerWorkflowName string
	Input              []byte
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
	// Outcome is the outcome a completed run's worker named, which a
	// workflow step follows; "" for success.
	Outcome string
}

// ActionRegisterOptions contains options for action registration.
// Registering replaces the action's settings: the last registration wins.
type ActionRegisterOptions struct {
	Namespace string
	// TimeoutMS is the lease: how long a worker holds a run without
	// completing, failing or touching it before it's taken back. The server
	// takes MinLeaseMS..MaxLeaseMS; nil or 0 is its default, 300000.
	TimeoutMS *uint32
	// MaxRetries counts attempts in all, the first included, 1..1000. nil
	// sends 3, the server's default.
	MaxRetries *uint32
	Owner      string
}

// The lease an action's TimeoutMS may set, and the longest the server holds
// one delivery however often it is touched.
const (
	MinLeaseMS = 5000
	MaxLeaseMS = 12 * 60 * 60 * 1000
	MaxHoldMS  = 12 * 60 * 60 * 1000
)

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
	// Limit is the most names on the page; 0 lets the server choose.
	Limit uint32
	// Cursor is the previous page's Cursor; nil reads the first page.
	Cursor []byte
	// Prefix, when set, lists only the actions whose names start with it.
	Prefix string
}

// ActionListEntry is one action in a list page.
type ActionListEntry struct {
	Name string
	// Version counts the action's registrations.
	Version     uint32
	Owner       string
	TimeoutMS   uint64
	MaxAttempts uint32
	CreatedAtMS int64
}

// ActionListResult is one page of a namespace's actions, in name order.
type ActionListResult struct {
	Actions []ActionListEntry
	// HasMore says another page follows; Cursor reads it.
	HasMore bool
	// Cursor is where the next page starts; nil on the last page.
	Cursor []byte
}

// ActionInfoOptions contains options for Action.Info.
type ActionInfoOptions struct {
	Namespace string
}

// ActionInfo is one action's settings and counts.
type ActionInfo struct {
	Owner string
	// Version counts the action's registrations.
	Version     uint32
	TimeoutMS   uint64
	MaxAttempts uint32
	CreatedAtMS int64
	// Ready counts the runs waiting for a worker, up to 100,000;
	// ReadyCapped says more wait than were counted.
	Ready       uint64
	ReadyCapped bool
	// TooManyRuns says the action's shard holds too many of the namespace's
	// runs to count them: the counts and latencies below are nil.
	TooManyRuns bool
	Running     *uint64
	Completed   *uint64
	Failed      *uint64
	TimedOut    *uint64
	// The latencies are of the completed runs measured, LatencyCount of them.
	LatencyCount *uint64
	LatencyAvgMS *uint64
	LatencyP50MS *uint64
	LatencyP95MS *uint64
	LatencyMaxMS *uint64
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

// WorkerAwaitOptions contains options for an await.
type WorkerAwaitOptions struct {
	Namespace string
	BlockMS   *uint32 // Long-poll wait (nil = 30000, 0 = don't wait, max 300000)
	// RequestKey, at most MaxRequestKey bytes, makes a retried await safe:
	// one with the key an earlier await used gets the delivery that one
	// got, while its lease holds, not a second run.
	RequestKey string
}

// MaxRequestKey is the longest await request key.
const MaxRequestKey = 64

// WorkerCompleteOptions contains options for a complete.
type WorkerCompleteOptions struct {
	Namespace string
	Outcome   string // Named outcome a workflow step follows; "" is success
}

// WorkerFailOptions contains options for a fail.
type WorkerFailOptions struct {
	Namespace string
	// Retry runs it again while attempts are left; false fails the run now.
	Retry bool
	// Abandoned hands the run back untried: no failure is counted.
	Abandoned bool
	// DelayMS is how long before the run is ready again, at most a day.
	DelayMS uint64
}

// WorkerTouchOptions contains options for a touch.
type WorkerTouchOptions struct {
	Namespace string
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
	// Action, when set, lists only the workers that run this action.
	Action string
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

// WorkerDetails is one worker's record, from Client.WorkerInfo.
type WorkerDetails struct {
	Worker WorkerInfo
	// Metadata is what the worker registered with; "" when it gave none.
	Metadata  string
	Processes []WorkerProcess
}

// WorkerProcess is one process a worker registered, with its counts.
type WorkerProcess struct {
	Name        string
	Kind        ProcessKind
	RunCount    uint64
	FailCount   uint64
	LastRunAtMS int64 // 0 before its first run
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
	// Limit is the most runs on the page; 0 lets the server choose (100),
	// and it holds at most 1024.
	Limit uint32
	// Status keeps only runs in that status (e.g. "running", "completed",
	// "failed"); empty for any.
	Status string
	// Cursor is the previous page's Cursor; nil reads the first page.
	Cursor []byte
	// Search keeps only runs that match it, ignoring case; empty for all.
	Search string
}

// WorkflowRunEntry is one run in a list page.
type WorkflowRunEntry struct {
	RunID     string
	Workflow  string
	Status    string
	CreatedAt int64
}

// WorkflowRunListResult is one page of runs, newest first. A page can hold
// fewer than Limit while HasMore is set: keep reading until HasMore is false.
type WorkflowRunListResult struct {
	Runs []*WorkflowRunEntry
	// HasMore says another page follows; Cursor reads it.
	HasMore bool
	// Cursor is where the next page starts, opaque bytes to pass back; nil
	// on the last page.
	Cursor []byte
}

// WorkflowListDefinitionsOptions contains options for listing workflow definitions.
type WorkflowListDefinitionsOptions struct {
	Namespace string
	// Limit is the most definitions on the page; 0 means 100.
	Limit uint32
	// Cursor is the previous page's Cursor; nil reads the first page.
	Cursor []byte
}

// WorkflowDefinitionEntry is one workflow definition in a list page.
type WorkflowDefinitionEntry struct {
	Name      string
	Version   string
	CreatedAt int64
	// Enabled is false for a disabled definition, which starts no runs.
	Enabled bool
}

// WorkflowDefinitionListResult is one page of a namespace's workflow
// definitions. A page can hold fewer than Limit while HasMore is set: keep
// reading until HasMore is false.
type WorkflowDefinitionListResult struct {
	Definitions []*WorkflowDefinitionEntry
	// HasMore says another page follows; Cursor reads it.
	HasMore bool
	// Cursor is where the next page starts; nil on the last page.
	Cursor []byte
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
	// Limit is the most jobs on the page; 0 means 100.
	Limit uint32
	// Cursor is the previous page's Cursor; nil reads the first page.
	Cursor []byte
}

// ProcessingListResult is one page of a namespace's processing jobs. A page
// can hold fewer than Limit while HasMore is set: keep reading until HasMore
// is false.
type ProcessingListResult struct {
	Jobs []*ProcessingListEntry
	// HasMore says another page follows; Cursor reads it.
	HasMore bool
	// Cursor is where the next page starts; nil on the last page.
	Cursor []byte
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
	// Reason says why a failed job failed, or, for a running job this node
	// holds back, "held here: " and why; empty otherwise.
	Reason string
	// Running and what follows are the job's pipelines on the node that
	// answered, since that node started them; none of it is replicated.
	Running     bool
	RecordsIn   uint64
	RecordsOut  uint64
	WatermarkMS int64
	TickAvgUS   uint64
	TickMaxUS   uint64
	UptimeMS    uint64
	// YAML is the definition the job was submitted with.
	YAML []byte
}

// ProcessingListEntry represents a single entry in a processing job list.
type ProcessingListEntry struct {
	Name  string
	JobID string
	// Status is the job's status as the server words it: "RUNNING",
	// "FAILED: why", "RUNNING (held here: why)".
	Status           string
	CreatedAt        int64
	BatchSize        uint32
	RecordsProcessed uint64
}

// ProcessingRemoveOptions contains options for removing a processing job.
type ProcessingRemoveOptions struct {
	Namespace string
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
	// ReasonKeysNotColocated: the key routes elsewhere than the key the
	// transaction was begun with. Give every key in a transaction the same
	// {tag}.
	ReasonKeysNotColocated Reason = 18
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
	// ReasonStaleReport: an action complete, fail or touch naming a delivery
	// the run has moved on from (its lease expired, it was delivered again,
	// another worker holds it, or it finished otherwise). It was dropped. A
	// resent report that already applied answers ok instead.
	ReasonStaleReport Reason = 26
	// ReasonWorkerUnhealthy: an await from a worker that missed its
	// heartbeats; a heartbeat, or registering again, lets it take work.
	ReasonWorkerUnhealthy Reason = 27
	// ReasonWorkerDraining: an await from a worker that is draining.
	ReasonWorkerDraining Reason = 28
	// ReasonShardHalted: the shard halted at an entry and takes no writes
	// until an operator repairs it. With StatusUnavailable the write didn't
	// run; with StatusOutcomePending it committed and waits on the repair.
	ReasonShardHalted Reason = 29
	// ReasonAdminOnly: the op is served only on the node's admin socket.
	ReasonAdminOnly Reason = 30
	// ReasonJobRunning: a processing job that is still running can't be
	// removed; stop or cancel it first.
	ReasonJobRunning Reason = 31
	// ReasonMemoryBudget (with StatusLimit): the node's memory budget is
	// spent. The message names the node.
	ReasonMemoryBudget Reason = 32
	// ReasonTooManyActions (with StatusLimit): the node holds as many
	// actions as it registers; delete one first.
	ReasonTooManyActions Reason = 33
	// ReasonTooManyWorkflowDefinitions (with StatusLimit): the node holds as
	// many workflow definitions as it keeps; delete one first.
	ReasonTooManyWorkflowDefinitions Reason = 34
	// ReasonTooManyWorkflowRuns (with StatusLimit): the node holds as many
	// workflow runs as it keeps; let some finish or delete them first.
	ReasonTooManyWorkflowRuns Reason = 35
)

var reasonNames = map[Reason]string{
	ReasonUnknownOp: "unknown_op", ReasonMalformed: "malformed", ReasonFieldTooLong: "field_too_long",
	ReasonOutOfRange: "out_of_range", ReasonLeaderUnknown: "leader_unknown", ReasonShardBusy: "shard_busy",
	ReasonTableMismatch: "table_mismatch", ReasonUnknownFlag: "unknown_flag", ReasonInternal: "internal",
	ReasonCommittedNotApplied: "committed_not_applied", ReasonAnswerLost: "answer_lost", ReasonUnclassified: "unclassified",
	ReasonNotFound: "not_found", ReasonReservedKey: "reserved_key", ReasonCASMismatch: "cas_mismatch",
	ReasonConditionFailed: "condition_failed", ReasonNoSuchTransaction: "no_such_transaction",
	ReasonKeysNotColocated: "keys_not_colocated", ReasonTxnTooLarge: "txn_too_large", ReasonTooManyWaiters: "too_many_waiters",
	ReasonConflictingFields: "conflicting_fields", ReasonWritesStopped: "writes_stopped", ReasonTooManyProposals: "too_many_proposals",
	ReasonWrongType: "wrong_type", ReasonTooManyTransactions: "too_many_transactions",
	ReasonStaleReport: "stale_report", ReasonWorkerUnhealthy: "worker_unhealthy", ReasonWorkerDraining: "worker_draining",
	ReasonShardHalted: "shard_halted", ReasonAdminOnly: "admin_only", ReasonJobRunning: "job_running",
	ReasonMemoryBudget: "memory_budget", ReasonTooManyActions: "too_many_actions",
	ReasonTooManyWorkflowDefinitions: "too_many_workflow_definitions", ReasonTooManyWorkflowRuns: "too_many_workflow_runs",
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
