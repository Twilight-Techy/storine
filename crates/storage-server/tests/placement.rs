//! Multi-region placement in a range cluster (`TASK-156`): six nodes in two regions (three
//! zones each) and placement rules. The allocator moves a region-bound collection's replicas
//! into its region and its leases there, and a lease preference pulls another collection's
//! leases to the preferred region; health reports show where every range is.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use storage_codec::PropertyValue;
use storage_core::dist::zone::Locality;
use storage_core::{DocOp, DocWrite, Precondition};
use storage_server::ranges::{start_range_server, RangeExtras, RangeServerConfig};
use storage_server::{SecurityConfig, StorageClient};

/// One range as a node's health report lists it.
#[derive(Debug)]
struct Placed {
    start: String,
    end: Option<String>,
    voters: Vec<u16>,
    leader: bool,
}

/// Reads the `"placement":[...]` array of a health report (the fields this server writes).
fn placement(health: &str) -> Vec<Placed> {
    let Some(at) = health.find("\"placement\":[") else { return Vec::new() };
    let body = &health[at + 13..];
    let mut out = Vec::new();
    for obj in body.split("{\"range\":").skip(1) {
        let field = |name: &str| -> Option<&str> {
            let i = obj.find(&format!("\"{name}\":"))? + name.len() + 3;
            Some(&obj[i..])
        };
        let string = |s: &str| -> Option<String> {
            let s = s.strip_prefix('"')?;
            let mut out = String::new();
            let mut chars = s.chars();
            while let Some(c) = chars.next() {
                match c {
                    '"' => return Some(out),
                    '\\' => match chars.next()? {
                        'u' => {
                            let hex: String = chars.by_ref().take(4).collect();
                            out.push(char::from_u32(u32::from_str_radix(&hex, 16).ok()?)?);
                        }
                        c => out.push(c),
                    },
                    c => out.push(c),
                }
            }
            None
        };
        let start = string(field("start").unwrap()).unwrap();
        let end_raw = field("end").unwrap();
        let end = if end_raw.starts_with("null") { None } else { string(end_raw) };
        let voters_raw = field("voters").unwrap();
        let voters = voters_raw[1..voters_raw.find(']').unwrap()].split(',').filter(|v| !v.is_empty()).map(|v| v.parse().unwrap()).collect();
        let leader = field("leader").unwrap().starts_with("true");
        out.push(Placed { start, end, voters, leader });
    }
    out
}

/// Whether the range covers some key of `collection`.
fn covers(p: &Placed, collection: &str) -> bool {
    let lo = format!("{collection}/");
    let hi = format!("{collection}0"); // '0' follows '/'
    p.start.as_str() < hi.as_str() && p.end.as_deref().is_none_or(|e| e > lo.as_str())
}

fn put(collection: &str, key: &str) -> DocWrite {
    let mut f = BTreeMap::new();
    f.insert("n".to_string(), PropertyValue::Int64(1));
    DocWrite { collection: collection.into(), key: key.into(), op: DocOp::Put(f), precondition: Precondition::None }
}

#[test]
fn replicas_and_leases_follow_the_placement_rules() {
    let root = std::env::temp_dir().join(format!("stge_placement_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let addrs: Vec<SocketAddr> = (0..6).map(|_| storage_types::testing::free_local_addr()).collect();
    let peers: HashMap<u16, SocketAddr> = addrs.iter().enumerate().map(|(i, a)| ((i + 1) as u16, *a)).collect();
    let localities: BTreeMap<u16, Locality> = (1..=6u16)
        .map(|n| {
            let region = if n <= 3 { "us" } else { "eu" };
            (n, Locality::new(region, &format!("{region}-{}", (n - 1) % 3)))
        })
        .collect();
    // The rules start without the eu-only one; it is added while the cluster runs, by editing
    // the file every node watches.
    std::fs::create_dir_all(&root).unwrap();
    let rules_file = root.join("zones");
    let first = "default replicas=3 survival=zone\ncollection us_first lease=us\n";
    std::fs::write(&rules_file, first).unwrap();
    let zones = storage_server::placement::parse(first).unwrap();
    storage_server::placement::check(&localities, 6, &zones).unwrap();
    let servers: Vec<_> = (0..6)
        .map(|i| {
            start_range_server(RangeServerConfig {
                data_dir: root.join(format!("n{}", i + 1)),
                bind_addr: addrs[i].to_string(),
                node_id: (i + 1) as u16,
                peers: peers.clone(),
                token: None,
                security: SecurityConfig::default(),
                max_connections: 256,
                max_offset: Duration::from_millis(100),
                peer_tls: None,
                extras: RangeExtras { localities: localities.clone(), zones: zones.clone(), zones_file: Some(rules_file.clone()), ..Default::default() },
            })
            .unwrap()
        })
        .collect();

    let write = |c: &str, k: &str| {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let ok = StorageClient::connect(addrs[0]).and_then(|mut cl| cl.doc_write(&[put(c, k)], None)).is_ok();
            if ok {
                return;
            }
            assert!(Instant::now() < deadline, "write {c}/{k} never succeeded");
            std::thread::sleep(Duration::from_millis(100));
        }
    };
    for i in 0..30 {
        write("eu_only", &format!("k{i}"));
        write("us_first", &format!("k{i}"));
        write("other", &format!("k{i}"));
    }

    std::thread::sleep(Duration::from_millis(1100)); // a newer modification time
    std::fs::write(&rules_file, format!("{first}collection eu_only require=region:eu lease=eu\n")).unwrap();

    // Converged: every replica of an eu_only range is in eu and its leader too; us_first ranges
    // are led from us; every range has three voters.
    let eu = |n: u16| localities[&n].region == "eu";
    let deadline = Instant::now() + Duration::from_secs(180);
    let mut last = String::new();
    loop {
        let mut ok = true;
        let mut report = Vec::new();
        for (i, a) in addrs.iter().enumerate() {
            let node = (i + 1) as u16;
            let Ok(h) = StorageClient::connect(*a).and_then(|mut c| c.health()) else {
                ok = false;
                continue;
            };
            for p in placement(&h) {
                report.push(format!("n{node} {p:?}"));
                if covers(&p, "eu_only") && (!p.voters.iter().all(|&v| eu(v)) || (p.leader && !eu(node))) {
                    ok = false;
                }
                if covers(&p, "us_first") && !covers(&p, "eu_only") && p.leader && eu(node) {
                    ok = false;
                }
                if p.voters.len() != 3 {
                    ok = false;
                }
            }
        }
        if ok && !report.is_empty() {
            break;
        }
        last = report.join("\n");
        assert!(Instant::now() < deadline, "placement never converged:\n{last}");
        std::thread::sleep(Duration::from_secs(1));
    }
    let _ = last;
    // Rules that do not fit the localities are refused: every node reports why and keeps the
    // rules it has.
    std::thread::sleep(Duration::from_millis(1100));
    std::fs::write(&rules_file, "collection eu_only require=region:asia\n").unwrap();
    for a in &addrs {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let h = StorageClient::connect(*a).and_then(|mut c| c.health()).unwrap();
            if h.contains("\"placement_rules\"") && h.contains("fewer nodes satisfy") {
                break;
            }
            assert!(Instant::now() < deadline, "the bad rules were not reported: {h}");
            std::thread::sleep(Duration::from_millis(500));
        }
    }
    // Still one database: every document reads from any node.
    let mut c = StorageClient::connect(addrs[2]).unwrap();
    for coll in ["eu_only", "us_first", "other"] {
        assert!(c.doc_get(coll, "k29").unwrap().is_some(), "{coll}/k29");
    }
    drop(servers);
    let _ = std::fs::remove_dir_all(&root);
}
