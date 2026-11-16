---
title: Clusters
description: Replicate a database across three or five servers with Raft, fail over automatically, and change the cluster's members while it runs.
---

Three or five servers that list each other form a **single-group cluster**. They keep one copy of the data each, replicated with Raft:

- one node is the **leader**. Writes go to it, and it acknowledges a write once a majority of nodes have it in their logs and it has applied it;
- the others are **followers**. They apply the log in the background, and answer clients with the leader's address;
- if the leader stops, the others elect a new one within about a second, and clients follow it on their own.

A cluster of three survives losing one node; a cluster of five survives two. To spread data across more nodes than that, use a [range cluster](range-clusters.md).

## Starting a cluster

Run on each machine, with its own `--node-id` and the same `--peers`:

```bash
export STORAGE_AUTH_TOKENS="admin-token-0123456789:admin,app-token-0123456789:write"
export STORAGE_CLUSTER_TOKEN="node-token-0123456789"

storage-server ./n1 10.0.0.1:7878 --node-id 1 --peers 1=10.0.0.1:7878,2=10.0.0.2:7878,3=10.0.0.3:7878   # on 10.0.0.1
storage-server ./n2 10.0.0.2:7878 --node-id 2 --peers 1=10.0.0.1:7878,2=10.0.0.2:7878,3=10.0.0.3:7878   # on 10.0.0.2
storage-server ./n3 10.0.0.3:7878 --node-id 3 --peers 1=10.0.0.1:7878,2=10.0.0.2:7878,3=10.0.0.3:7878   # on 10.0.0.3
```

- `--peers` lists every founding member, this node included. Nodes talk Raft over the same port as clients.
- `STORAGE_CLUSTER_TOKEN` is the token nodes authenticate to each other with. It is required with authentication, and must be the same on every node.
- To encrypt traffic between nodes, add certificates: see [TLS between nodes](security.md#tls-between-nodes).

You can try a cluster on one machine with three ports:

```bash
for i in 1 2 3; do
  storage-server ./n$i 127.0.0.1:787$i --node-id $i --peers 1=127.0.0.1:7871,2=127.0.0.1:7872,3=127.0.0.1:7873 &
done
```

## Connecting

List any or all nodes in the connection string. The client finds the leader itself:

```text
storine://app-token-0123456789@10.0.0.1:7878,10.0.0.2:7878,10.0.0.3:7878
```

When the leader changes, followers answer `NotLeader` with the new leader's address, and every client retries there with the same request id. If a node is unreachable, the client tries another seed. You do not need a load balancer.

### Follower reads

Reads go to the leader, so they see every acknowledged write. To spread read load, a client may allow **follower reads**, which can be slightly behind:

```text
storine://10.0.0.1:7878,10.0.0.2:7878,10.0.0.3:7878?follower_reads=1
```

## Settings

| Setting | Value |
| :--- | :--- |
| Election timeout | 300–600 ms |
| Heartbeat | 60 ms |
| Commit timeout | 5 s. The client then gets `CommitUnknown` and retries with the same request id |
| Append batch | 512 entries or 4 MiB |
| Peer RPC timeout | 500 ms |
| Checkpoint and log compaction | every 10 s if something was applied |
| TTL sweep | every 60 s, when documents with a TTL exist |
| Members | at most 256 |

## Write speed

A cluster write waits for two disk syncs in a row: the leader's log, then a follower's. Concurrent writes share syncs, so throughput grows with concurrency. On three separate 2-vCPU Azure VMs whose disk syncs take 4 ms, a write took about 8.3 ms at the median: 106 writes a second from one client, and 238 from 16 ([benchmark report](../../docs/BENCHMARK_REPORT.md), section 8.8). A standalone server on the same disk takes about 4 ms.

## Changing membership

Membership changes one node at a time, with `storage-cli cluster`, sent to any member:

| Command | Does |
| :--- | :--- |
| `cluster <addr> add <id> <host:port>` | Add a voter. It joins as a learner, catches up, and is promoted. It has 60 seconds to catch up |
| `cluster <addr> add-learner <id> <host:port>` | Add a learner: it receives the log but does not vote |
| `cluster <addr> promote <id>` | Make a caught-up learner a voter |
| `cluster <addr> remove <id>` | Remove a member, the leader included |
| `cluster <addr> members` | List the members, their roles and the leader |

### Adding a node

Start the new node with its own id and `--join`, and no `--peers`. It waits without campaigning:

```bash
storage-server ./n4 10.0.0.4:7878 --node-id 4 --join
```

Then add it from anywhere:

```bash
STORAGE_TOKEN=admin-token-0123456789 storage-cli cluster 10.0.0.1:7878 add 4 10.0.0.4:7878
```

### Replacing a failed node

1. Remove it: `storage-cli cluster 10.0.0.1:7878 remove 3`.
2. Start a fresh node with a new id and `--join`, and add it as above.

`--peers` only founds a cluster. After a membership change, the configuration in the Raft log wins, so restart nodes with the same flags they started with.

## Monitoring a cluster

`storage-cli cluster <addr> members` and the console's overview show the leader, the term, and each node's commit and applied index. Every node's [health report](monitoring.md#health) covers that node. Watch that every node's applied index keeps up with the leader's commit index.

## Backups and upgrades

- Back up any node with `storage-cli remote-backup` (see [backups](backups.md)). The backup is that node's data, which is all of it. Restore it as a standalone server, or found a new cluster from it.
- Point-in-time recovery works on single-group nodes, with `STORAGE_WAL_ARCHIVE` set on each.
- Patch releases can be rolled through a cluster one node at a time. Before 1.0, a minor release needs every node stopped: see [upgrades](../../docs/COMPATIBILITY.md).
