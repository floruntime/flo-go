# Changelog

All notable changes to the Flo Go SDK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

These follow the server's wire. The server's op table changed with them, so
`TableHash` is now `0xa8de7758afa69427`, the hash of flo `bdb66a0`, which CI's
live tests run against; this SDK no longer talks to an older server.

### Breaking

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
- `OpNamespaceConfigSet` (0x014) and `OpNamespaceConfigGet` (0x015) are gone:
  the server retired them

### Added

- `StatusNoNamespace` (14), `ErrNoNamespace` and `IsNoNamespace`: the
  namespace a request names doesn't exist. It is its own error, which
  `ErrNotFound` doesn't match. Reads in a namespace that was never created
  return it from the server version that adds it; flo `bdb66a0` still answers
  them as absent (`Get` reports not found, lists come back empty)
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

