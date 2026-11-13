//! Multi-region placement for range clusters (`TASK-156`, `F-GEO-05`).
//!
//! Every node is told where every node runs, in `--peers`: `1=10.0.0.1:7878@us-east/a` means
//! node 1 is in region `us-east`, zone `a`. Placement rules come from a file given with
//! `--zones`; every node must be given the same file (each node's allocator applies its copy).
//! One rule per line:
//!
//! ```text
//! # Applies to everything without a rule of its own.
//! default replicas=3 survival=zone
//! # Survive losing a whole region; leases (and so writes) in us-east when possible.
//! collection accounts survival=region lease=us-east,eu-west
//! # Keep a collection, or the keys of a collection starting with a prefix, in one region.
//! collection eu_customers require=region:eu-west lease=eu-west
//! collection orders prefix=eu/ require=region:eu-west
//! # Never in a zone.
//! collection logs avoid=zone:c
//! # Readable from any replica at the present time; writes wait out the lead.
//! collection rates global lead=1500ms
//! # Each row lives in its home region: keys are `<region>\u{1}<key>`.
//! collection profiles regional-by-row regions=us-east,eu-west
//! ```
//!
//! Options: `replicas=N`; `survival=zone|region`; `require=region:R` / `require=zone:Z`
//! (repeatable; several of one tier are alternatives); `avoid=region:R` / `avoid=zone:Z`;
//! `lease=R1,R2` (preferred leaseholder regions, in order); `follower-reads=3s` (how stale
//! follower reads may be) or `follower-reads=off`; `global lead=<duration>`;
//! `regional-by-row regions=R1,R2`. Durations take `ms`, `s` or `m`.

use std::collections::BTreeMap;

use storage_core::dist::zone::{self, ClosedTs, Constraint, Locality, Survival, Tier, ZoneConfig, ZoneConfigs};
use storage_core::raft::KeySpan;
use storage_core::NodeId;
use storage_types::{Result, StorageError};

fn bad(line: usize, msg: impl std::fmt::Display) -> StorageError {
    StorageError::config(format!("placement rules, line {line}: {msg}"))
}

fn duration_ns(text: &str) -> Option<u64> {
    let (n, mult) = if let Some(n) = text.strip_suffix("ms") {
        (n, 1_000_000)
    } else if let Some(n) = text.strip_suffix('s') {
        (n, 1_000_000_000)
    } else {
        (text.strip_suffix('m')?, 60_000_000_000)
    };
    n.parse::<u64>().ok()?.checked_mul(mult)
}

/// The span of `collection`'s keys starting with `prefix`.
fn prefix_span(collection: &str, prefix: &str) -> KeySpan {
    let start = format!("{collection}/{prefix}").into_bytes();
    // The smallest key above every key with this prefix: the prefix with its last byte below
    // 0xFF incremented (bytes after it dropped).
    let mut end = start.clone();
    while let Some(last) = end.pop() {
        if last < 0xFF {
            end.push(last + 1);
            break;
        }
    }
    KeySpan::new(start, (!end.is_empty()).then_some(end))
}

/// Parses placement rules (see the module docs).
pub fn parse(text: &str) -> Result<ZoneConfigs> {
    let mut zones = ZoneConfigs::default();
    let mut seen_default = false;
    for (i, raw) in text.lines().enumerate() {
        let n = i + 1;
        let line = raw.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let mut words = line.split_whitespace();
        let (collection, rest): (Option<&str>, Vec<&str>) = match words.next() {
            Some("default") => (None, words.collect()),
            Some("collection") => {
                let name = words.next().ok_or_else(|| bad(n, "`collection` needs a collection name"))?;
                (Some(name), words.collect())
            }
            Some(other) => return Err(bad(n, format!("a rule starts with `default` or `collection`, not {other:?}"))),
            None => continue,
        };
        let mut cfg = ZoneConfig::default();
        let (mut prefix, mut lead, mut regional) = (None::<String>, None::<u64>, None::<Vec<String>>);
        let mut global = false;
        for word in rest {
            if word == "global" {
                global = true;
                continue;
            }
            if word == "regional-by-row" {
                regional = Some(Vec::new());
                continue;
            }
            let (key, value) = word.split_once('=').ok_or_else(|| bad(n, format!("expected key=value, found {word:?}")))?;
            match key {
                "replicas" => cfg.replicas = value.parse().ok().filter(|r| (1..=15).contains(r)).ok_or_else(|| bad(n, "replicas must be 1 to 15"))?,
                "survival" => {
                    cfg.survival = match value {
                        "zone" => Survival::Zone,
                        "region" => Survival::Region,
                        _ => return Err(bad(n, "survival is `zone` or `region`")),
                    }
                }
                "require" | "avoid" => {
                    let (tier, v) = value.split_once(':').ok_or_else(|| bad(n, format!("{key}= takes region:<name> or zone:<name>")))?;
                    let tier = match tier {
                        "region" => Tier::Region,
                        "zone" => Tier::Zone,
                        _ => return Err(bad(n, format!("{key}= takes region:<name> or zone:<name>"))),
                    };
                    cfg.constraints.push(if key == "require" { Constraint::required(tier, v) } else { Constraint::prohibited(tier, v) });
                }
                "lease" => cfg.lease_preferences = value.split(',').filter(|r| !r.is_empty()).map(str::to_string).collect(),
                "follower-reads" => {
                    cfg.closed_ts = if value == "off" {
                        ClosedTs::Off
                    } else {
                        ClosedTs::Lag(duration_ns(value).ok_or_else(|| bad(n, "follower-reads takes a duration like 3s, or off"))?)
                    }
                }
                "lead" => lead = Some(duration_ns(value).ok_or_else(|| bad(n, "lead takes a duration like 1500ms"))?),
                "prefix" => prefix = Some(value.to_string()),
                "regions" => regional = Some(value.split(',').filter(|r| !r.is_empty()).map(str::to_string).collect()),
                _ => return Err(bad(n, format!("unknown option {key:?}"))),
            }
        }
        match collection {
            None => {
                if seen_default {
                    return Err(bad(n, "a second `default` rule"));
                }
                if prefix.is_some() || global || regional.is_some() {
                    return Err(bad(n, "`default` takes only placement options"));
                }
                seen_default = true;
                zones.default = cfg;
            }
            Some(coll) => {
                if global {
                    let lead = lead.ok_or_else(|| bad(n, "`global` needs lead=<duration> (above twice the maximum clock offset)"))?;
                    zone::global(&mut zones, coll, lead, &cfg).map_err(|e| bad(n, e))?;
                } else if let Some(regions) = regional {
                    if regions.is_empty() {
                        return Err(bad(n, "`regional-by-row` needs regions=R1,R2,..."));
                    }
                    let regions: Vec<&str> = regions.iter().map(String::as_str).collect();
                    zone::regional_by_row(&mut zones, coll, &regions, &cfg).map_err(|e| bad(n, e))?;
                } else {
                    if lead.is_some() {
                        return Err(bad(n, "lead= goes with `global`"));
                    }
                    let span = match &prefix {
                        Some(p) => prefix_span(coll, p),
                        None => zone::collection_span(coll),
                    };
                    zones.set(span, cfg).map_err(|e| bad(n, e))?;
                }
            }
        }
    }
    Ok(zones)
}

/// Splits a `--peers` entry's address from its locality: `host:port@region/zone`.
pub fn split_locality(entry: &str) -> Result<(&str, Option<Locality>)> {
    let Some((addr, loc)) = entry.split_once('@') else {
        return Ok((entry, None));
    };
    let (region, zone) = loc.split_once('/').ok_or_else(|| StorageError::config(format!("--peers: {entry:?}: the locality is region/zone, like us-east/a")))?;
    if region.is_empty() || zone.is_empty() {
        return Err(StorageError::config(format!("--peers: {entry:?}: the locality is region/zone, like us-east/a")));
    }
    Ok((addr, Some(Locality::new(region, zone))))
}

/// Checks localities and rules together: either every node has a locality or none has, and
/// rules that place by region or zone need localities.
pub fn check(localities: &BTreeMap<NodeId, Locality>, nodes: usize, zones: &ZoneConfigs) -> Result<()> {
    if !localities.is_empty() && localities.len() != nodes {
        return Err(StorageError::config("--peers: give every node a locality (host:port@region/zone), or none"));
    }
    if localities.is_empty() && *zones != ZoneConfigs::default() {
        return Err(StorageError::config("--zones needs every node's locality in --peers (host:port@region/zone)"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn locs(spec: &[(NodeId, &str, &str)]) -> BTreeMap<NodeId, Locality> {
        spec.iter().map(|&(n, r, z)| (n, Locality::new(r, z))).collect()
    }

    #[test]
    fn rules_parse_into_zone_configs() {
        let z = parse(
            "# comment\n\
             default replicas=3 survival=zone follower-reads=4s\n\
             collection accounts survival=region lease=us,eu\n\
             collection eu_only require=region:eu lease=eu\n\
             collection orders prefix=eu/ require=region:eu avoid=zone:eu-c\n\
             collection rates global lead=1500ms\n\
             collection profiles regional-by-row regions=us,eu\n",
        )
        .unwrap();
        assert_eq!(z.default.replicas, 3);
        assert_eq!(z.default.closed_ts, ClosedTs::Lag(4_000_000_000));
        assert_eq!(z.get(b"accounts/x").survival, Survival::Region);
        assert_eq!(z.get(b"accounts/x").lease_preferences, ["us", "eu"]);
        assert!(!z.get(b"eu_only/k").allows(&Locality::new("us", "a")));
        assert!(z.get(b"eu_only/k").allows(&Locality::new("eu", "a")));
        assert!(!z.get(b"orders/eu/1").allows(&Locality::new("eu", "eu-c")));
        assert!(z.get(b"orders/us/1").constraints.is_empty(), "outside the prefix: the default");
        assert_eq!(z.get(b"rates/usd").closed_ts, ClosedTs::Lead(1_500_000_000));
        assert_eq!(z.get(b"profiles/eu\x01bob").lease_preferences, ["eu", "us"]);
        assert!(z.get(b"people/x").constraints.is_empty());
    }

    #[test]
    fn bad_rules_say_which_line_and_why() {
        for (text, says) in [
            ("collection\n", "needs a collection name"),
            ("tables x\n", "starts with"),
            ("default replicas=0\n", "1 to 15"),
            ("collection a survival=planet\n", "survival"),
            ("collection a require=country:x\n", "region:<name>"),
            ("collection a global\n", "lead="),
            ("collection a lead=1s\n", "goes with `global`"),
            ("collection a colour=red\n", "unknown option"),
            ("default\ndefault\n", "second `default`"),
            ("collection a follower-reads=soon\n", "duration"),
        ] {
            let e = parse(text).unwrap_err().to_string();
            assert!(e.contains(says) && e.contains("line"), "{text:?}: {e}");
        }
    }

    #[test]
    fn localities_come_with_peers_and_rules_need_them() {
        assert_eq!(split_locality("10.0.0.1:7878@us/a").unwrap(), ("10.0.0.1:7878", Some(Locality::new("us", "a"))));
        assert_eq!(split_locality("h:1").unwrap(), ("h:1", None));
        assert!(split_locality("h:1@us").is_err());
        let rules = parse("collection a require=region:us\n").unwrap();
        assert!(check(&BTreeMap::new(), 3, &rules).is_err());
        assert!(check(&locs(&[(1, "us", "a")]), 3, &ZoneConfigs::default()).is_err());
        assert!(check(&locs(&[(1, "us", "a"), (2, "us", "b"), (3, "eu", "a")]), 3, &rules).is_ok());
        let span = prefix_span("c", "a\u{ff}");
        assert!(span.end.as_deref().is_some_and(|e| e > &b"c/a\xc3\xbf"[..]));
    }
}
