//! Localities and zone configurations (`F-GEO-05`, `TASK-141`).
//!
//! - **Locality**: every node declares a region and a zone (a failure domain inside the region).
//! - **Zone configuration** of a key span:
//!   - the number of voters;
//!   - **constraints**: required or prohibited regions / zones. Required values of one tier
//!     are alternatives (any of them); tiers combine (all of them);
//!   - **lease preferences**: regions in order; the lease goes to the first one with a live,
//!     caught-up voter;
//!   - **survival goal**: losing any one zone (`Zone`) or any one region (`Region`) keeps a
//!     quorum. No failure domain may hold a majority of the voters;
//!   - **closed timestamps** ([`ClosedTs`]): how far behind (`Lag`) or ahead (`Lead`, global
//!     tables) of the leaseholder's clock writes are closed. Followers serve reads at or below
//!     the closed timestamp.
//! - [`ZoneConfigs`] maps non-overlapping spans to configurations, with a default for the rest.
//!   A range never straddles two configurations: the allocator splits at their boundaries and
//!   never merges across one.
//! - **Regional-by-row tables** ([`regional_by_row`]): each row's key starts with its home
//!   region, and each region's span gets a configuration preferring leases there.
//! - **Global tables** ([`global`]): writes are closed ahead of time (`Lead`), so every replica
//!   serves consistent present-time reads; writers wait out the lead (commit-wait).
//!
//! Everything here is pure data; the allocator ([`crate::dist::alloc`]) and the driver
//! ([`crate::dist::node`]) apply it.

use std::collections::BTreeMap;

use storage_types::{Result, StorageError};

use crate::raft::KeySpan;
use crate::sharding::NodeId;

/// Where a node runs.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Locality {
    pub region: String,
    pub zone: String,
}

impl Locality {
    pub fn new(region: &str, zone: &str) -> Self {
        Self { region: region.to_string(), zone: zone.to_string() }
    }
}

/// A failure domain level.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Tier {
    Region,
    Zone,
}

/// Which failure the range must survive.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Survival {
    /// Losing any one zone keeps a quorum.
    #[default]
    Zone,
    /// Losing any one region keeps a quorum (needs voters in at least three regions).
    Region,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Constraint {
    pub tier: Tier,
    pub value: String,
    /// `true`: replicas only there (or in another required value of the tier); `false`:
    /// never there.
    pub required: bool,
}

impl Constraint {
    pub fn required(tier: Tier, value: &str) -> Self {
        Self { tier, value: value.to_string(), required: true }
    }

    pub fn prohibited(tier: Tier, value: &str) -> Self {
        Self { tier, value: value.to_string(), required: false }
    }
}

/// Closed-timestamp policy: what the leaseholder promises followers about future writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClosedTs {
    /// No follower reads.
    Off,
    /// Writes land above `leaseholder clock - lag` (nanoseconds): followers serve reads that
    /// are at least `lag` old (plus propagation).
    Lag(u64),
    /// Writes land above `leaseholder clock + lead` and their transactions wait until that
    /// time has passed before acknowledging: followers serve present-time reads. The lead
    /// must exceed twice the maximum clock offset.
    Lead(u64),
}

impl ClosedTs {
    /// The signed offset (nanoseconds) added to the leaseholder's clock to get the timestamp
    /// it closes.
    pub fn offset_ns(self) -> Option<i64> {
        match self {
            Self::Off => None,
            Self::Lag(ns) => Some(-(ns.min(i64::MAX as u64) as i64)),
            Self::Lead(ns) => Some(ns.min(i64::MAX as u64) as i64),
        }
    }
}

/// Default closed-timestamp lag (3 s, as recent-history reads tolerate).
pub const DEFAULT_CLOSED_LAG_NS: u64 = 3_000_000_000;

/// Placement and read policy of a key span.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ZoneConfig {
    /// Voters (0: the allocator's replication factor).
    pub replicas: usize,
    pub constraints: Vec<Constraint>,
    /// Regions, most preferred first.
    pub lease_preferences: Vec<String>,
    pub survival: Survival,
    pub closed_ts: ClosedTs,
}

impl Default for ZoneConfig {
    fn default() -> Self {
        Self { replicas: 0, constraints: Vec::new(), lease_preferences: Vec::new(), survival: Survival::Zone, closed_ts: ClosedTs::Lag(DEFAULT_CLOSED_LAG_NS) }
    }
}

impl ZoneConfig {
    /// Whether a store at `loc` may hold a replica.
    pub fn allows(&self, loc: &Locality) -> bool {
        let value = |t: Tier| if t == Tier::Region { &loc.region } else { &loc.zone };
        for tier in [Tier::Region, Tier::Zone] {
            let mut required = self.constraints.iter().filter(|c| c.tier == tier && c.required).peekable();
            if required.peek().is_some() && !required.any(|c| &c.value == value(tier)) {
                return false;
            }
        }
        !self.constraints.iter().any(|c| !c.required && &c.value == value(c.tier))
    }

    /// Checks the configuration against the cluster's localities: the voters it asks for can
    /// be placed, and its survival goal can be met.
    pub fn validate(&self, replication: usize, max_offset_ns: u64, nodes: &BTreeMap<NodeId, Locality>) -> Result<()> {
        let n = if self.replicas == 0 { replication } else { self.replicas };
        if n == 0 || n > 15 {
            return Err(StorageError::invalid_value("zone config: replicas must be between 1 and 15"));
        }
        let allowed: Vec<&Locality> = nodes.values().filter(|l| self.allows(l)).collect();
        if allowed.len() < n {
            return Err(StorageError::invalid_value("zone config: fewer nodes satisfy its constraints than the replicas it asks for"));
        }
        // Survival: spread n voters over the domains so that none holds a majority.
        let quorum = n / 2 + 1;
        let tier = if self.survival == Survival::Region { Tier::Region } else { Tier::Zone };
        let mut domains: BTreeMap<(&str, &str), usize> = BTreeMap::new();
        for l in &allowed {
            let key = if tier == Tier::Region { (l.region.as_str(), "") } else { (l.region.as_str(), l.zone.as_str()) };
            *domains.entry(key).or_default() += 1;
        }
        // Greedy round-robin placement, capping every domain below a quorum.
        let cap = quorum - 1;
        let placeable: usize = domains.values().map(|&c| c.min(cap)).sum();
        if n > 1 && placeable < n {
            return Err(StorageError::invalid_value(if tier == Tier::Region {
                "zone config: region survival needs voters spread so that no region holds a majority (at least three regions)"
            } else {
                "zone config: zone survival needs voters spread so that no zone holds a majority (at least three zones)"
            }));
        }
        if let ClosedTs::Lead(lead) = self.closed_ts {
            // Writes land more than twice the maximum offset ahead of the leaseholder's clock,
            // so every clock sees them as synthetic (never absorbed), and coordinators wait
            // them out; followers' closed timestamps then cover present-time reads.
            if lead <= max_offset_ns.saturating_mul(2) {
                return Err(StorageError::invalid_value("zone config: a global (lead) closed timestamp must exceed twice the maximum clock offset"));
            }
        }
        if let ClosedTs::Lag(lag) = self.closed_ts {
            // A new leaseholder starts its writes at its clock, which may trail the old one's by
            // the maximum offset: the lag keeps closed timestamps below that.
            if lag < max_offset_ns {
                return Err(StorageError::invalid_value("zone config: the closed-timestamp lag must be at least the maximum clock offset"));
            }
        }
        Ok(())
    }
}

/// Zone configurations by key span.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ZoneConfigs {
    pub default: ZoneConfig,
    /// Sorted by start, non-overlapping.
    spans: Vec<(KeySpan, ZoneConfig)>,
}

impl ZoneConfigs {
    pub fn new(default: ZoneConfig) -> Self {
        Self { default, spans: Vec::new() }
    }

    /// Sets the configuration of `span` (which must not overlap another configured span).
    pub fn set(&mut self, span: KeySpan, cfg: ZoneConfig) -> Result<()> {
        if span.end.as_ref().is_some_and(|e| *e <= span.start) {
            return Err(StorageError::invalid_value("zone config: empty span"));
        }
        if let Some(i) = self.spans.iter().position(|(s, _)| *s == span) {
            self.spans[i].1 = cfg;
            return Ok(());
        }
        if self.spans.iter().any(|(s, _)| s.overlaps(&span)) {
            return Err(StorageError::invalid_value("zone config: span overlaps another configured span"));
        }
        let at = self.spans.partition_point(|(s, _)| s.start < span.start);
        self.spans.insert(at, (span, cfg));
        Ok(())
    }

    /// Removes the configuration of exactly `span` (it falls back to the default).
    pub fn remove(&mut self, span: &KeySpan) -> bool {
        let before = self.spans.len();
        self.spans.retain(|(s, _)| s != span);
        self.spans.len() != before
    }

    pub fn spans(&self) -> &[(KeySpan, ZoneConfig)] {
        &self.spans
    }

    /// The configuration in force at `key`. `O(log Z)`.
    pub fn get(&self, key: &[u8]) -> &ZoneConfig {
        let at = self.spans.partition_point(|(s, _)| s.start.as_slice() <= key);
        match at.checked_sub(1).map(|i| &self.spans[i]) {
            Some((s, c)) if s.contains(key) => c,
            _ => &self.default,
        }
    }

    /// The first configuration boundary strictly inside `span` (the range must split there).
    pub fn boundary_inside(&self, span: &KeySpan) -> Option<Vec<u8>> {
        let inside = |k: &[u8]| k > span.start.as_slice() && span.end.as_deref().is_none_or(|e| k < e);
        self.spans.iter().flat_map(|(s, _)| [Some(s.start.as_slice()), s.end.as_deref()]).flatten().filter(|k| inside(k)).min().map(<[u8]>::to_vec)
    }

    /// Whether `a` and `b` (span starts) fall under the same configuration entry.
    pub fn same_entry(&self, a: &[u8], b: &[u8]) -> bool {
        let entry = |k: &[u8]| self.spans.iter().position(|(s, _)| s.contains(k));
        entry(a) == entry(b)
    }

    /// The largest lead of any configuration (`0`: no global spans). New leaseholders and
    /// ranges just split or merged start their writes above it: a lagging replica may still
    /// serve a key it no longer owns at a timestamp its old range closed ahead of time.
    pub fn max_lead_ns(&self) -> u64 {
        let lead = |c: &ZoneConfig| if let ClosedTs::Lead(l) = c.closed_ts { l } else { 0 };
        self.spans.iter().map(|(_, c)| lead(c)).chain([lead(&self.default)]).max().unwrap_or(0)
    }

    /// Checks every configuration (see [`ZoneConfig::validate`]).
    pub fn validate(&self, replication: usize, max_offset_ns: u64, nodes: &BTreeMap<NodeId, Locality>) -> Result<()> {
        self.default.validate(replication, max_offset_ns, nodes)?;
        for (_, c) in &self.spans {
            c.validate(replication, max_offset_ns, nodes)?;
        }
        Ok(())
    }
}

/// Raw-id span of a collection's keys (`<collection>/...`).
pub fn collection_span(collection: &str) -> KeySpan {
    let mut start = collection.as_bytes().to_vec();
    start.push(b'/');
    let mut end = collection.as_bytes().to_vec();
    end.push(b'/' + 1);
    KeySpan::new(start, Some(end))
}

/// Separates a regional-by-row key's home region from the row's own key.
pub const REGION_SEP: char = '\u{1}';

/// The key of a regional-by-row row: its home region first, so each region's rows form one span.
pub fn regional_key(region: &str, key: &str) -> String {
    format!("{region}{REGION_SEP}{key}")
}

/// The home region of a regional-by-row key.
pub fn home_region(key: &str) -> Option<&str> {
    key.split_once(REGION_SEP).map(|(r, _)| r)
}

/// Raw-id span of the rows of `collection` homed in `region`.
pub fn region_span(collection: &str, region: &str) -> KeySpan {
    let mut start = format!("{collection}/{region}").into_bytes();
    let mut end = start.clone();
    start.push(REGION_SEP as u8);
    end.push(REGION_SEP as u8 + 1);
    KeySpan::new(start, Some(end))
}

/// Regional-by-row placement of `collection`: per home region, voters that survive `survival`
/// and leases preferring the home region.
pub fn regional_by_row(zones: &mut ZoneConfigs, collection: &str, regions: &[&str], base: &ZoneConfig) -> Result<()> {
    for &region in regions {
        let mut prefs = vec![region.to_string()];
        prefs.extend(regions.iter().filter(|&&r| r != region).map(|r| r.to_string()));
        zones.set(region_span(collection, region), ZoneConfig { lease_preferences: prefs, ..base.clone() })?;
    }
    Ok(())
}

/// Global placement of `collection`: present-time reads from any replica; writes wait out
/// `lead` (which must exceed the maximum clock offset plus closed-timestamp propagation).
pub fn global(zones: &mut ZoneConfigs, collection: &str, lead_ns: u64, base: &ZoneConfig) -> Result<()> {
    zones.set(collection_span(collection), ZoneConfig { closed_ts: ClosedTs::Lead(lead_ns), ..base.clone() })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nodes(spec: &[(NodeId, &str, &str)]) -> BTreeMap<NodeId, Locality> {
        spec.iter().map(|&(n, r, z)| (n, Locality::new(r, z))).collect()
    }

    #[test]
    fn constraints_combine_by_tier() {
        let c = ZoneConfig {
            constraints: vec![Constraint::required(Tier::Region, "eu"), Constraint::required(Tier::Region, "us"), Constraint::prohibited(Tier::Zone, "us-b")],
            ..ZoneConfig::default()
        };
        assert!(c.allows(&Locality::new("eu", "eu-a")));
        assert!(c.allows(&Locality::new("us", "us-a")));
        assert!(!c.allows(&Locality::new("us", "us-b")));
        assert!(!c.allows(&Locality::new("ap", "ap-a")));
    }

    #[test]
    fn survival_goals_are_checked_against_the_cluster() {
        let three_regions = nodes(&[(1, "eu", "a"), (2, "eu", "b"), (3, "us", "a"), (4, "us", "b"), (5, "ap", "a")]);
        let region = ZoneConfig { replicas: 5, survival: Survival::Region, ..ZoneConfig::default() };
        assert!(region.validate(3, 1_000, &three_regions).is_ok());
        let two_regions = nodes(&[(1, "eu", "a"), (2, "eu", "b"), (3, "eu", "c"), (4, "us", "a"), (5, "us", "b")]);
        assert!(region.validate(3, 1_000, &two_regions).is_err());
        let zone = ZoneConfig { replicas: 3, ..ZoneConfig::default() };
        assert!(zone.validate(3, 1_000, &nodes(&[(1, "eu", "a"), (2, "eu", "b"), (3, "eu", "c")])).is_ok());
        assert!(zone.validate(3, 1_000, &nodes(&[(1, "eu", "a"), (2, "eu", "a"), (3, "eu", "b")])).is_err());
        let lead = ZoneConfig { closed_ts: ClosedTs::Lead(500), ..zone.clone() };
        assert!(lead.validate(3, 1_000, &nodes(&[(1, "eu", "a"), (2, "eu", "b"), (3, "eu", "c")])).is_err());
    }

    #[test]
    fn spans_resolve_and_split_at_boundaries() {
        let mut z = ZoneConfigs::new(ZoneConfig::default());
        global(&mut z, "rates", 900_000_000, &ZoneConfig::default()).unwrap();
        regional_by_row(&mut z, "users", &["eu", "us"], &ZoneConfig::default()).unwrap();
        assert_eq!(z.get(b"rates/usd").closed_ts, ClosedTs::Lead(900_000_000));
        assert_eq!(z.get(b"orders/1"), &z.default);
        let eu = format!("users/{}", regional_key("eu", "42"));
        assert_eq!(z.get(eu.as_bytes()).lease_preferences, vec!["eu".to_string(), "us".to_string()]);
        assert_eq!(home_region(&regional_key("eu", "42")), Some("eu"));
        // A range over everything must split at the first boundary.
        assert_eq!(z.boundary_inside(&KeySpan::all()), Some(b"rates/".to_vec()));
        assert_eq!(z.boundary_inside(&collection_span("rates")), None);
        assert!(z.same_entry(b"rates/a", b"rates/z"));
        assert!(!z.same_entry(b"rates/a", b"orders/a"));
        assert!(z.set(KeySpan::new(b"rates/m".to_vec(), None), ZoneConfig::default()).is_err());
    }
}
