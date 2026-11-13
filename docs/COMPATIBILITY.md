# Compatibility and upgrades

What a Storine release may change, and how to move a database or a cluster from one release to the next.

## What a release may change

| Release | Example | On-disk formats | Client protocol | Node-to-node protocol | Rust API |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Patch | 0.1.0 → 0.1.1 | unchanged | unchanged | unchanged | compatible |
| Minor, before 1.0 | 0.1.x → 0.2.0 | may change, read forward | additive only | may change | may break |
| Major, from 1.0 | 1.x → 2.0 | may change, read forward | may change | may change | may break |

- **On-disk formats** are the record frames, the WAL, arena segments, index checkpoints, Raft logs and snapshots, columnar segments and sealed files. Each carries a magic number or version. "Read forward" means a newer release opens data an older one wrote, converting it as it goes. An older release never opens data a newer one wrote: **downgrades are not supported** once a newer binary has written to a data directory. Back up before upgrading.
- **Client protocol** (`protocol/PROTOCOL.md`): additive changes only, before and after 1.0, until a major version says otherwise. New opcodes and new trailing fields may be added; existing requests keep their meaning. An older SDK keeps working against a newer server.
- **Node-to-node protocol** (Raft and range RPCs): it carries no version number in 0.1, so every node of a cluster must run the same minor version.
- **Record schemas** are yours: a record type's `SCHEMA_VERSION` and its registered migrations convert older records when they are read or replayed, whatever the Storine version.

Every release's entry in [`CHANGELOG.md`](../CHANGELOG.md) says which of these it changes.

## Upgrading

### An embedded engine or a single server

1. Back up: `storage-cli backup` for an embedded engine, `storage-cli remote-backup` for a server, or copy the stopped data directory.
2. Stop the process. A clean stop checkpoints; a crash is fine too, since the WAL is replayed on open.
3. Start the new binary on the same directory.

### A cluster, between patch releases (rolling)

One node at a time:

1. Stop the node. With three nodes, the other two keep a majority and keep serving.
2. Replace the binary and start it on the same data directory.
3. Wait until the node has caught up before the next one. Ask it with `storage-cli remote-health`. In a single group, `NotLeader` replies naming a leader mean it rejoined. With `--ranges`, every range it holds must report a leader.

Start with followers and do the leader last, so leadership moves once.

### A cluster, between minor versions before 1.0

Rolling upgrades are not supported, since the node-to-node protocol may differ. Either:

- **Stop the whole cluster, upgrade every node, start it again.** On-disk data is read forward. This is the quicker route.
- **Back up and restore into a new cluster** running the new version. Use this when the release notes say a format is not read forward.

## Testing an upgrade

The release checklist ([`RELEASE.md`](RELEASE.md)) runs every test on the release commit only. Before upgrading a production cluster, restore a recent backup into a staging cluster running the new version, and run your application's tests against it.
