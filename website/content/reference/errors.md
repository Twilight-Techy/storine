---
title: Errors
description: Every status a server can answer, what each client calls it, and which ones to retry.
---

## Statuses

| Code | Name | Meaning | What to do |
| ---: | :--- | :--- | :--- |
| `0x00` | `Ok` | Success | |
| `0x01` | `NotFound` | The document does not exist (or a patched document did not) | Not an error for `get`, which returns nothing |
| `0x02` | `Error` | A server-side failure, such as an I/O error. The message says what | Look at the server; retry if it was transient |
| `0x03` | `Overloaded` | Too many connections, or too many analytic queries or admin jobs at once | Retry later, with backoff. Clients do this for analytics |
| `0x04` | `Conflict` | A precondition failed. Carries the index of the failed write, whether the document exists, and its current version | Read again, then decide |
| `0x05` | `Unauthorized` | No token, a wrong token, or a role too low for the request. The connection is closed | Fix the token or the role |
| `0x06` | `RateLimited` | This connection exceeded `STORAGE_RATE_LIMIT`. The connection is closed | Retry later |
| `0x07` | `NotLeader` | A cluster follower got a request for the leader. Carries the leader's address, or nothing if it is not known yet | Clients retry on the leader by themselves |
| `0x08` | `Invalid` | A malformed request, a bad value, a type mismatch, a crossed `min` or `max`, a limit exceeded | Do not retry: fix the request |
| `0x09` | `CommitUnknown` | A replicated write could not be confirmed within 5 seconds. It may still commit | Clients retry with the same request id, so it is applied once |

A request rejected before its body was read (unauthorized, too large, rate limited) gets its error, and then the server closes the connection.

## In each client

| Status | Python, Ruby | TypeScript | Go | Java, C#, PHP | Rust |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `NotFound` | `NotFoundError` | `NotFoundError` | `ErrNotFound` | `NotFoundException` | `Error::NotFound` |
| `Conflict` | `ConflictError` | `ConflictError` | `ErrConflict` | `ConflictException` | `Error::Conflict` |
| `Invalid` | `InvalidError` | `InvalidError` | `ErrInvalid` | `InvalidException` | `Error::Invalid` |
| `Unauthorized` | `UnauthorizedError` | `UnauthorizedError` | `ErrUnauthorized` | `UnauthorizedException` | `Error::Unauthorized` |
| `Overloaded` | `OverloadedError` | `OverloadedError` | `ErrOverloaded` | `OverloadedException` | `Error::Overloaded` |
| `RateLimited` | `RateLimitedError` | `RateLimitedError` | `ErrRateLimited` | `RateLimitedException` | `Error::RateLimited` |
| no server reachable | `UnavailableError` | `UnavailableError` | `ErrUnavailable` | `UnavailableException` | `Error::Unavailable` |
| `Error` | `ServerError` | `ServerError` | `ErrServer` | `ServerException` | `Error::Server` |

Every error derives from one base: `StorineError` (Python, TypeScript), `Storine::Error` (Ruby), `*storine.Error` with `errors.Is` (Go), `StorineException` (Java, C#, PHP) and `storine::Error` (Rust, where `is_retryable()` says which may succeed later). A conflict carries the failed write's index and the current version: `write_index` and `current_version` in Python and Ruby, `writeIndex` and `currentVersion` elsewhere.

`NotLeader` and `CommitUnknown` never reach your code: the clients handle them.

## Common messages

| Message | Cause |
| :--- | :--- |
| `refusing to serve without authentication on a non-loopback address` | The server was given a network address without `STORAGE_AUTH_TOKENS` |
| `STORAGE_AUTH_TOKENS entries must be token:role` | A malformed token list. Roles are `read`, `write`, `admin` |
| `STORAGE_CLUSTER_TOKEN must be at least 16 characters` | The node-to-node token is too short |
| `--peers must list this node's own --node-id` | Each node must appear in its own `--peers` |
| `range clusters with authentication need STORAGE_CLUSTER_TOKEN` | Set the node token on every range node |
| `backup name must be a relative path without '..', ':' or '\'` | A remote backup name tried to leave the backup root |

More symptoms and fixes are in [troubleshooting](../troubleshooting.md).
