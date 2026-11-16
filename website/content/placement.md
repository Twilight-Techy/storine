---
title: Placement across regions
description: Tell a range cluster where each node runs, and write rules for where each collection's data lives, how many replicas it keeps, and which failures it survives.
---

A [range cluster](range-clusters.md) can place data by **region** and **zone**. You give every node its locality, and every node the same placement rules. Each node's allocator then splits ranges at rule boundaries, moves replicas to where the rules allow, and moves leases to preferred regions.

## Localities

Add `@region/zone` to each member in `--peers`:

```bash
storage-server ./n1 10.0.0.1:7878 --ranges --node-id 1 --zones /etc/storine/zones \
  --peers 1=10.0.0.1:7878@us-east/a,2=10.0.0.2:7878@us-east/b,3=10.1.0.1:7878@eu-west/a,4=10.1.0.2:7878@eu-west/b,5=10.2.0.1:7878@ap-south/a
```

Changing a node's locality takes a restart of every node with the new `--peers`.

## Rules

`--zones <file>` names the rules file, the same on every node. One rule per line, and `#` starts a comment:

```text
default replicas=3 survival=zone                             # everything without a rule of its own
collection accounts survival=region lease=us-east,eu-west    # survive losing a region; leases in us-east first
collection eu_customers require=region:eu-west lease=eu-west # this collection's data stays in eu-west
collection orders prefix=eu/ require=region:eu-west          # only the keys starting with eu/
collection logs avoid=zone:c                                 # never in zone c
collection rates global lead=1500ms                          # present-time reads from any replica
collection profiles regional-by-row regions=us-east,eu-west  # each row in its home region
```

A rule starts with `default`, or `collection <name>` with an optional `prefix=<key prefix>`, followed by options:

| Option | Meaning |
| :--- | :--- |
| `replicas=N` | Voters per range, 1 to 15 |
| `survival=zone` | Losing any one zone keeps a majority: no zone may hold a majority of the voters. Needs at least three zones |
| `survival=region` | Losing any one region keeps a majority. Needs at least three regions |
| `require=region:R`, `require=zone:Z` | Replicas only there. Several of one kind are alternatives |
| `avoid=region:R`, `avoid=zone:Z` | Never there |
| `lease=R1,R2` | Preferred leaseholder regions, in order. The leaseholder serves writes and current reads |
| `follower-reads=3s`, `follower-reads=off` | How far behind a follower may be and still serve reads. Default 3 seconds |
| `global lead=1500ms` | Writes are timestamped ahead and wait out the lead, so every replica can serve present-time reads. The lead must exceed twice the maximum clock offset of 500 ms |
| `regional-by-row regions=R1,R2` | Each key lives in its own region: keys are written as `<region>\u0001<key>`, and their ranges keep replicas and leases there |

### Choosing a pattern

| You want | Use |
| :--- | :--- |
| Survive a data-center outage | `survival=zone` (the usual default) |
| Survive losing a whole region | `survival=region` and nodes in at least three regions |
| Keep data in a jurisdiction | `require=region:eu-west` on the collection, or a `prefix` for part of it |
| Fast writes from one region | `lease=<that region>` |
| Fast reads everywhere of data that rarely changes | `global` |
| Users near their data | `regional-by-row` |

## Checking and changing rules

The server refuses rules the localities cannot satisfy: too few nodes in a required region, or region survival with fewer than three regions, for example.

To change the rules, edit the file on every node. Each node reloads it within 2 seconds, and the allocator moves data from its next round. A node that refuses a new version (it does not parse, or cannot be satisfied) keeps its current rules, and reports the error in its health report under `placement_rules`.

Every node's [health report](monitoring.md#health) lists the ranges it holds under `placement`: the span, the voters, and whether it leads each. That is how to confirm where data lives.
