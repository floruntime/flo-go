# Changelog

All notable changes to the Flo Go SDK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

These follow the server's wire. The server's op table changed with them, so
`TableHash` is now `0x19075df8e1ed5f75`, the hash of flo `5213836`, which CI's
live tests run against; this SDK no longer talks to an older server.

### Breaking

- Queues are leased until completed, at least once. `QueueClient.Dequeue`
  (0x181) takes `DequeueOptions.Consumer`, `LeaseMS` and `Ack`, and returns
  messages with `Deliveries` (replacing `DeliveryCount`; `Priority` is a
  `uint32`), the dequeue's `Lease` and `ExpiresAtMS`. `Ack` and `Nack` are
  replaced by `Complete` (0x182), `Fail` (0x183, with `DelayMS`, `Dead` and
  `Error`) and the new `Touch` (0x184); each names the lease and its
  messages (`QueueItem`) and answers a `QueueReport` with per-message
  refusals (`QueueRefusalWhy`). Ops 0x171, 0x172 and 0x174 are gone
- `QueueClient.DLQRequeue` is gone: the server doesn't implement it
- `StorageTier` gains `StorageTierDisk` (2); a tier this SDK doesn't know is
  kept, not refused

- Action await, complete, fail and touch move to the server's typed ops
  0x30A-0x30D; 0x306-0x309 are gone. An await answers a `*Task` (replacing
  `TaskAssignment`): a `Delivery` (`RunID`, `Action`, `Deliveries`, `Term`,
  `Index`) plus `Namespace`, `CreatedAtMS`, `Attempt`, `LeaseMS`, the caller
  and `Input`. `WorkerClient.Complete`, `Fail` and `Touch` take that
  `Delivery`, keyed by the run. `Touch` takes no extend length and returns
  the lease's expiry; `ActionContext.Touch()` likewise. A complete's empty
  outcome is success (it used to send "success")
- A report naming a delivery the run has moved on from is a
  `*StaleReportError` (`IsStaleReport`, still a conflict); a resent report
  that already applied answers ok
- `WorkerFailOptions` gains `Abandoned` (no failure counted) and `DelayMS`;
  an action worker that stops mid-run hands the run back abandoned
- `ActionRegisterOptions.TimeoutMS` is the lease, 5 s to 12 h; nil sends 0,
  the server's default of 300 s (it used to send 30 s). `AutoTouch` touches
  every sixth of the task's lease
- `QueueClient.Peek` uses the server's `queue.peek` (0x180; 0x17C is gone),
  takes a `uint32` limit and returns a `*PeekResult`
- `QueueClient.DLQList` sends its limit and start in the request value, which
  the server reads (it ignored the options it used to send), takes
  `DLQListOptions.Limit` and `After`, and returns a `*DLQPage` (`Messages`,
  `Total`, `HasMore`, `LastSeq`)
- `ActionListResult.Names` is `Actions`, each with its version, owner,
  timeout, attempts and creation time
- `ProcessingStatusResult.FailureReason` is `Reason`, which also says why a
  node holds a running job back; the result gains the answering node's
  runtime counts and the job's YAML. `ProcessingListEntry` gains `BatchSize`
  and `RecordsProcessed`, `WorkflowDefinitionEntry` gains `Enabled`

- The cluster op codes follow the server's: `OpClusterStatus` (0x030),
  `OpClusterMembers` (0x031), `OpClusterPromote` (0x032), `OpClusterRemove`
  (0x033) and `OpClusterRepairSkip` (0x034). `OpClusterJoin`,
  `OpClusterLeave`, `OpClusterTransferLeader`, `OpClusterAddNode` and
  `OpClusterRemoveNode` are gone: the server has no such ops
- `WorkflowClient.ListDefinitions` returns a `*WorkflowDefinitionListResult`
  (parsed `Definitions`, `HasMore`, `Cursor`) instead of raw bytes, and
  `ProcessingClient.List` a `*ProcessingListResult` (`Jobs`, `HasMore`,
  `Cursor`) instead of a slice: both read one page, and a short page can
  still have more behind it
- `ActionListOptions.Limit` is a `uint32` instead of a `*uint32` (0 lets the
  server choose)
- A run id chosen at start (`WorkflowStartOptions.RunID`) is found by status,
  history, signal and cancel on any shard, and a later start of the same id
  on another workflow is refused with `ErrConflict`. Two starts of one id sent
  at once can both run; a lookup by the id then refuses it with `ErrConflict`.
- `WorkflowClient.ListRuns` returns a `*WorkflowRunListResult` (`Runs`, each
  with `RunID`, `Workflow`, `Status` and `CreatedAt`, newest first; `HasMore`;
  `Cursor`) instead of raw bytes. It reads one page, of one workflow's runs
  or, with an empty name, every workflow's; a short page can still have more
  behind it. `WorkflowListRunsOptions.Cursor` is the opaque `[]byte` a page
  returns instead of a string, and a `Limit` of 0 lets the server choose
- `StreamClient.Info` on a stream that doesn't exist returns `ErrNotFound`
  (the server now answers not_found); it used to return an all-zero
  `StreamInfo`
- Reads, lists and scans in a namespace that was never created return
  `ErrNoNamespace` (status 14) instead of answering as if it were empty:
  `KV.Get` no longer reports a plain not found there, and lists no longer
  come back empty. A write still creates the namespace
- `OpNamespaceConfigSet` (0x014) and `OpNamespaceConfigGet` (0x015) are gone:
  the server retired them

- An action handler that panics fails its run with retry, as one that
  returns an error does, so the action's max attempts bounds it (it used to
  fail the run at once)
- So does a handler that runs past the worker's `ActionTimeout` (it too used
  to fail the run at once); `NonRetryableError` is the one way to fail a run
  at once
- So do a task with no handler and a result that doesn't marshal to JSON
- `AutoTouch` touches at the task's arrival plus whole intervals, skipping a
  slot that passed during a slow touch; each touch's latency used to push
  the later ones back

### Added

- `EnqueueOptions.DelayMS`, which the server applies, and
  `QueueClient.Purge`
- A full queue's refusal (`overloaded`, ran no) is retried as any other,
  then returned with the server's message
- The stream worker sends group reads and pending-drain claims refused
  `unavailable` (ran no), as when records are read back from disk, again
  with a backoff; the claim loop no longer gives up until the next reconnect
- `Stale` also marks a read from a node blocked reading its log, or diverged

- `MaxHoldMS`: the longest the server holds one delivery, 12 h, however
  often it is touched

- `ActionClient.Info` (0x30F): an action's settings and run counts
- `ActionRunStatus.Outcome`: the outcome a completed run's worker named
- `StreamClient.GroupList` (0x15D) and `GroupMembers` (0x15E)
- `ProcessingClient.Remove` (0x365) and `ReasonJobRunning`
- `WorkerAwaitOptions.RequestKey`: a retried await gets the delivery the
  first got. The action worker keeps a poll's key until a poll is answered
- `Delivery.TaskID` and `ParseTaskID`, in the flo CLI's
  `run:deliveries:term:index:action` form

- `StatusNoNamespace` (14), `ErrNoNamespace` and `IsNoNamespace`: the
  namespace a request names doesn't exist. It is its own error, which
  `ErrNotFound` doesn't match
- `ReasonAdminOnly`: the op is served only on a node's admin socket
- `ActionClient.List`, paged: `ActionListOptions.Limit`, `Cursor` and
  `Prefix` in, `ActionListResult.Names`, `HasMore` and `Cursor` out
- `StreamClient.Delete` (`OpStreamDelete`, 0x140) removes a stream's records
  and metadata. A non-empty stream is refused with `ErrConflict` unless
  `StreamDeleteOptions.Force` is set (`OptForce`, 0x2C); deleting a missing
  stream succeeds
- `OpNamespaceEnsure` (0x016), so the op table matches the server's; no
  client call sends it, nor `OpClusterRepairSkip`

### Changed

- A `kv.scan` cursor is up to 4098 bytes (a shard id and the last key read),
  up from 64, and its length prefix is a u16

