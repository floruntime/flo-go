# Changelog

All notable changes to the Flo Go SDK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

These follow the server's wire. The server's op table changed with them, so
`TableHash` moves to the server's new hash in the same release; until then
this SDK still pins the previous server.

### Breaking

- The cluster op codes follow the server's: `OpClusterStatus` (0x030),
  `OpClusterMembers` (0x031), `OpClusterPromote` (0x032), `OpClusterRemove`
  (0x033) and `OpClusterRepairSkip` (0x034). `OpClusterJoin`,
  `OpClusterLeave`, `OpClusterTransferLeader`, `OpClusterAddNode` and
  `OpClusterRemoveNode` are gone: the server has no such ops

### Added

- `StatusNoNamespace`, `ErrNoNamespace` and `IsNoNamespace`: the namespace a
  request names doesn't exist. `ErrNotFound` no longer matches it, so a read
  that takes a missing key as an answer (`Get`, `Delete`) fails on a missing
  namespace instead of reporting the key absent
- `ReasonAdminOnly`: the op is served only on a node's admin socket
- `ActionClient.List`, paged: `ActionListOptions.Limit`, `Cursor` and
  `Prefix` in, `ActionListResult.Actions`, `HasMore` and `Cursor` out
- `StreamClient.Delete` (`OpStreamDelete`, 0x140) removes a stream's records
  and metadata. A non-empty stream is refused with `ErrConflict` unless
  `StreamDeleteOptions.Force` is set (`OptForce`, 0x2C); deleting a missing
  stream succeeds
- `OpNamespaceEnsure` (0x016), so the op table matches the server's; no
  client call sends it, nor `OpClusterRepairSkip`

### Changed

- A `kv.scan` cursor is up to 4098 bytes (a shard id and the last key read),
  up from 64, and its length prefix is a u16
- `ActionListOptions.Limit` is a `uint32` (0 lets the server choose)

### Removed

- `OpNamespaceConfigSet` (0x014) and `OpNamespaceConfigGet` (0x015): the
  server retired them
