---
title: Encryption at rest
description: Encrypt everything a server writes to disk with a key file, and turn it on, rotate keys and turn it off with one restart each.
---

With a key file, a server seals everything it writes with ChaCha20-Poly1305: records, the write-ahead log, index files, and on a cluster the Raft logs and snapshots. Backups and archived WAL are copies of those files, so they are sealed too.

## Turning it on for a new database

```bash
storage-server gen-key /etc/storine/keys          # creates the file, readable by its owner only
STORAGE_ENCRYPTION_KEYS=/etc/storine/keys storage-server ./data 127.0.0.1:7878
```

The server refuses a key file that other users can read. From then on, it refuses plaintext data, and data sealed with a key it does not have.

> [!CAUTION]
> Keep a copy of the key file away from the data, for example in a secrets manager. Data sealed with a lost key cannot be recovered, and neither can its backups.

On a cluster, every node needs the same key file, because snapshots travel between nodes as stored.

## Turning it on for existing data

Restart once with the key file and `STORAGE_ENCRYPTION_ALLOW_PLAINTEXT=1`:

```bash
STORAGE_ENCRYPTION_KEYS=/etc/storine/keys STORAGE_ENCRYPTION_ALLOW_PLAINTEXT=1 storage-server ./data 127.0.0.1:7878
```

The server serves as usual and reseals everything in the background, then refuses plaintext again on its own. Its health report shows `"accept_plaintext": false` once it is done. Remove `STORAGE_ENCRYPTION_ALLOW_PLAINTEXT` before the next restart.

## Rotating keys

1. Add a key: `storage-server gen-key /etc/storine/keys`. The file now holds both keys.
2. Restart. The newest key seals new data, and the server reseals everything else with it.
3. Once that reseal has finished and a checkpoint has passed, the database no longer needs the old key.

Backups and archived WAL made before the rotation still need the old key. Keep it in the file, or somewhere safe, for as long as you might restore them.

`STORAGE_ENCRYPTION_ACTIVE_KEY=<id>` picks another key than the newest to seal with.

## Turning it off

Restart with `STORAGE_ENCRYPTION_ACTIVE_KEY=none`. The server decrypts everything. Then remove the encryption settings.

## When resealing happens

Every start reseals whatever does not match the settings. Standalone and single-group nodes reseal while they serve. Range nodes reseal before they start serving. The server prints a line when it is done:

```text
storage-server: encryption: resealed 18342 records (0 already in place)
```

## What stays visible

Encryption hides contents, not shapes. File names and sizes, and WAL frame headers (record ids and versions), remain visible on disk. Each record's seal is bound to its id and metadata, so sealed records cannot be swapped between documents without detection.

## Checking

The health report's `encryption` field is `"off"`, or the active key, the keys the server holds, and whether it still accepts plaintext.

## Restoring encrypted backups

Run `storage-cli restore-pitr` with the same `STORAGE_ENCRYPTION_KEYS` as the server. See [backups and recovery](backups.md).
