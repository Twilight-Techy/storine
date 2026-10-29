//! End-to-end tests against a real storage-server (single node and a 3-node cluster). The binary is
//! $STORINE_SERVER or <repo>/target/release/storage-server (cargo build --release -p storage-server);
//! the tests pass trivially without it.

use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use storine::{
    add, append, between, dec, doc, eq, gt, list, matches, set, unset, ByteRange, Client, Error, Options, Query, Value,
    VectorIndex,
};

fn server_binary() -> Option<PathBuf> {
    let p = std::env::var_os("STORINE_SERVER")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/release/storage-server"));
    p.exists().then_some(p)
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn temp_dir(tag: &str) -> PathBuf {
    static N: AtomicUsize = AtomicUsize::new(0);
    let d = std::env::temp_dir().join(format!(
        "storine_rs_{tag}_{}_{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start(bin: &PathBuf, dir: &PathBuf, port: u16, extra: &[String]) -> Server {
    let child = Command::new(bin)
        .arg(dir)
        .arg(format!("127.0.0.1:{port}"))
        .args(extra)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let server = Server(child);
    let deadline = Instant::now() + Duration::from_secs(10);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < deadline, "server on port {port} did not start");
        thread::sleep(Duration::from_millis(50));
    }
    server
}

fn single_node() -> Option<(Server, Client, PathBuf)> {
    let bin = server_binary()?;
    let dir = temp_dir("single");
    let port = free_port();
    let server = start(&bin, &dir, port, &[]);
    Some((server, storine::connect(&format!("storine://127.0.0.1:{port}")).unwrap(), dir))
}

#[test]
fn large_values() {
    let Some((_server, db, dir)) = single_node() else { return };
    let c = db.collection("rs_files");
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    let blob: Vec<u8> = (0..3 * 1024 * 1024 + 17)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect();
    let len = blob.len() as u64;
    let v = c
        .put(
            "f1",
            doc! {"data" => blob.clone(), "name" => "cat.png", "meta" => doc! {"size" => len, "type" => "image/png"}},
        )
        .unwrap();

    // Projection: only the named paths, nested paths nested.
    let d = c.get_fields("f1", &["name", "meta.size", "absent"]).unwrap().unwrap();
    assert_eq!((d.version, d.fields), (v, doc! {"name" => "cat.png", "meta" => doc! {"size" => len}}));
    assert!(c.get_fields("nope", &["name"]).unwrap().is_none());

    // Ranges.
    let r = c.read_bytes("f1", "data", 1000, 16).unwrap().unwrap();
    assert_eq!(r, ByteRange { version: v, total: len, data: blob[1000..1016].to_vec() });
    assert!(c.read_bytes("f1", "data", len + 5, 10).unwrap().unwrap().data.is_empty());
    assert_eq!(c.read_bytes("f1", "name", 0, 3).unwrap().unwrap().data, b"cat");
    assert!(c.read_bytes("nope", "data", 0, 10).unwrap().is_none());
    assert!(matches!(c.read_bytes("f1", "meta", 0, 10), Err(Error::Invalid(_)))); // not bytes
    assert!(matches!(c.read_bytes("f1", "data", 0, 17 * 1024 * 1024), Err(Error::Invalid(_))));

    // Streaming.
    let chunks: Vec<Vec<u8>> = c.stream_bytes("f1", "data", 1 << 20).collect::<Result<_, _>>().unwrap();
    assert_eq!(chunks.len(), 4);
    assert_eq!(chunks.concat(), blob);
    let mut missing = c.stream_bytes("nope", "data", 1 << 20);
    assert!(matches!(missing.next(), Some(Err(Error::NotFound { .. }))));
    assert!(missing.next().is_none());
    assert!(matches!(c.stream_bytes("f1", "data", 0).next(), Some(Err(Error::Invalid(_)))));
    let mut chunks = c.stream_bytes("f1", "data", 1 << 20);
    assert!(chunks.next().unwrap().is_ok());
    c.update("f1", [set("name", "dog.png")]).unwrap();
    assert!(matches!(chunks.next(), Some(Err(Error::Conflict { .. }))));
    drop(db);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn single_node_end_to_end() {
    let Some((_server, db, dir)) = single_node() else { return };

    // CRUD, types and versions.
    let c = db.collection("people");
    let d = doc! {
        "name" => "Ann", "age" => 37, "big" => 1i128 << 100, "score" => 9.5, "price" => dec("19.99"),
        "joined" => Value::Timestamp(1_714_566_600_123_456_789), "id" => Value::Uuid([7; 16]), "raw" => vec![0u8, 0xff],
        "tags" => list!["a", "b"], "address" => doc! {"city" => "Lagos"}, "nothing" => Value::Null, "ok" => true,
    };
    let v1 = c.insert("ann", d.clone()).unwrap();
    let got = c.get("ann").unwrap().unwrap();
    assert_eq!((got.version, &got.fields), (v1, &d));
    assert!(matches!(c.insert("ann", d.clone()), Err(Error::Conflict { .. })));
    let v2 = c.update("ann", [set("address.city", "Abuja"), unset("nothing"), append("tags", "c")]).unwrap();
    assert!(v2 > v1);
    let got = c.get("ann").unwrap().unwrap();
    assert_eq!(got.get("address").and_then(Value::as_map).unwrap()["city"], Value::from("Abuja"));
    assert!(got.get("nothing").is_none());
    assert_eq!(got.get("tags"), Some(&list!["a", "b", "c"]));
    match c.replace_if_version("ann", v1, doc! {"name" => "x"}) {
        Err(Error::Conflict { current_version, .. }) => assert_eq!(current_version, Some(v2)),
        other => panic!("{other:?}"),
    }
    c.replace_if_version("ann", v2, doc! {"name" => "Ann B"}).unwrap();
    assert_eq!(c.get("ann").unwrap().unwrap().fields, doc! {"name" => "Ann B"});
    assert!(matches!(c.update("nobody", [set("a", 1)]), Err(Error::NotFound { .. })));
    assert!(c.delete("ann").unwrap());
    assert!(!c.delete("ann").unwrap());
    assert!(c.get("ann").unwrap().is_none());

    // Money is exact and never lost.
    let acct = db.collection("accounts");
    acct.put("a", doc! {"balance" => dec("0.00")}).unwrap();
    thread::scope(|s| {
        for _ in 0..8 {
            s.spawn(|| {
                for _ in 0..100 {
                    db.collection("accounts").update("a", [add("balance", dec("0.01"))]).unwrap();
                }
            });
        }
    });
    let balance =
        |k: &str| acct.get(k).unwrap().unwrap().get("balance").and_then(Value::as_decimal).unwrap().to_string();
    assert_eq!(balance("a"), "8.00");
    assert!(matches!(acct.update("a", [add("balance", dec("-8.01")).min(0)]), Err(Error::Invalid(_))));
    assert_eq!(balance("a"), "8.00");

    // Transactions are atomic.
    let bank = db.collection("bank");
    bank.put("alice", doc! {"balance" => dec("50")}).unwrap();
    bank.put("bob", doc! {"balance" => dec("0")}).unwrap();
    let transfer = |amount: &str| {
        db.transaction(|tx| {
            tx.update("bank", "alice", [add("balance", dec(&format!("-{amount}"))).min(0)]);
            tx.update("bank", "bob", [add("balance", dec(amount))]);
            Ok(())
        })
    };
    transfer("20").unwrap();
    assert!(matches!(transfer("31"), Err(Error::Invalid(_))));
    let bal = |k: &str| bank.get(k).unwrap().unwrap().get("balance").and_then(Value::as_decimal).unwrap().to_string();
    assert_eq!((bal("alice"), bal("bob")), ("30".into(), "20".into()));
    let alice_v = bank.get("alice").unwrap().unwrap().version;
    let r = db.transaction(|tx| {
        tx.require_version("bank", "alice", alice_v + 100).put("bank", "carol", doc! {});
        Ok(())
    });
    assert!(matches!(r, Err(Error::Conflict { write_index: Some(0), .. })), "{r:?}");
    assert!(db
        .transaction(|tx| {
            tx.put("bank", "carol", doc! {});
            Err(Error::Invalid("changed my mind".into()))
        })
        .is_err());
    assert!(bank.get("carol").unwrap().is_none());

    // Queries, indexes and pagination.
    let items = db.collection("items");
    for i in 0..120i64 {
        let kind = if i % 2 == 0 { "even" } else { "odd" };
        items
            .put(
                &format!("i{i:03}"),
                doc! {"n" => i, "kind" => kind, "price" => storine::Decimal::new(i as i128 * 25, 2).unwrap()},
            )
            .unwrap();
    }
    let q = Query::new().filter(between("n", 10, 19) & eq("kind", "odd")).order_by("-n");
    let keys = |q: &Query| items.find(q.clone()).unwrap().into_iter().map(|d| d.key).collect::<Vec<_>>();
    let want = ["i019", "i017", "i015", "i013", "i011"];
    assert_eq!(keys(&q), want);
    items.create_index("n").unwrap();
    items.create_index("kind").unwrap();
    assert_eq!(keys(&q), want);
    let info = items.info().unwrap();
    assert!(info.indexes.iter().chain(&info.building).any(|f| f == "n"));
    assert_eq!(items.count(gt("price", dec("25"))).unwrap(), 19);
    assert_eq!(items.count(matches(&doc! {"kind" => "even"})).unwrap(), 60);
    assert_eq!(items.scan(None).map(Result::unwrap).count(), 120);
    assert_eq!(items.first(Query::new().order_by("-price")).unwrap().unwrap().key, "i119");
    assert!(db.collections().unwrap().iter().any(|n| n == "items"));

    // Vector search.
    let c = db.collection("rs_docs");
    c.create_vector_index("emb", 3).unwrap();
    assert_eq!(c.vector_indexes().unwrap(), vec![VectorIndex { field: "emb".into(), dims: 3, ready: true }]);
    for (key, vec, kind) in [("a", [1.0, 0.0, 0.0], "x"), ("b", [0.0, 1.0, 0.0], "y"), ("c", [0.7, 0.7, 0.0], "x")] {
        c.put(key, doc! {"emb" => list![vec[0], vec[1], vec[2]], "kind" => kind}).unwrap();
    }
    let hits = c.nearest("emb", &[1.0, 0.1, 0.0], 2, None).unwrap();
    assert_eq!(hits.iter().map(|d| d.key.as_str()).collect::<Vec<_>>(), ["a", "c"]);
    assert!(hits[0].score().unwrap() > hits[1].score().unwrap());
    let hits = c.nearest_exact("emb", &[1.0, 0.1, 0.0], 5, eq("kind", "x")).unwrap();
    assert_eq!(hits.iter().map(|d| d.key.as_str()).collect::<Vec<_>>(), ["a", "c"]);
    assert!(matches!(c.put("bad", doc! {"emb" => list![1.0, 2.0]}), Err(Error::Invalid(_)))); // wrong dimensions
    assert!(matches!(c.nearest("emb", &[1.0, 2.0], 1, None), Err(Error::Invalid(_))));
    c.drop_vector_index("emb").unwrap();
    assert_eq!(c.vector_indexes().unwrap(), vec![]);

    // TTL and errors.
    let s = db.collection("sessions");
    s.put_with_ttl("short", doc! {"u" => 1}, Duration::from_millis(200)).unwrap();
    assert!(s.get("short").unwrap().is_some());
    thread::sleep(Duration::from_millis(400));
    assert!(s.get("short").unwrap().is_none());
    assert!(matches!(s.put("bad", doc! {"a.b" => 1}), Err(Error::Invalid(_))));
    assert!(matches!(db.collection("bad/name").put("k", doc! {}), Err(Error::Invalid(_))));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn three_nodes_redirect_and_failover() {
    let Some(bin) = server_binary() else { return };
    let base = temp_dir("cluster");
    let ports = [free_port(), free_port(), free_port()];
    let peers = ports.iter().enumerate().map(|(i, p)| format!("{}=127.0.0.1:{p}", i + 1)).collect::<Vec<_>>().join(",");
    let mut servers: Vec<Option<Server>> = ports
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let args = vec!["--node-id".to_owned(), (i + 1).to_string(), "--peers".to_owned(), peers.clone()];
            Some(start(&bin, &base.join(format!("n{}", i + 1)), *p, &args))
        })
        .collect();
    // Any seed works: the client follows redirects to the leader.
    let seeds = ports.iter().map(|p| format!("127.0.0.1:{p}")).collect::<Vec<_>>().join(",");
    let db = storine::connect_with(
        &format!("storine://{seeds}"),
        Options { timeout: Duration::from_secs(2), ..Options::default() },
    )
    .unwrap();
    let acct = db.collection("acct");
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match acct.put("a", doc! {"bal" => dec("100")}) {
            Ok(_) => break,
            Err(Error::Unavailable(_)) if Instant::now() < deadline => thread::sleep(Duration::from_millis(100)),
            Err(e) => panic!("{e}"),
        }
    }
    let deposit = |n: usize| {
        for _ in 0..n {
            acct.update("a", [add("bal", dec("1"))]).unwrap();
        }
    };
    let bal = || acct.get("a").unwrap().unwrap().get("bal").and_then(Value::as_decimal).unwrap().to_string();
    deposit(10);
    assert_eq!(bal(), "110");
    let info = db.cluster_info().unwrap();
    assert!(info.is_leader);
    servers[info.node_id as usize - 1] = None; // kill the leader
                                               // The client fails over to the new leader; every deposit applies exactly once.
    deposit(5);
    assert_eq!(bal(), "115");
    drop(servers);
    let _ = std::fs::remove_dir_all(base);
}

/// Analytics (TASK-129): expectations computed here from the inserted documents must match the server exactly,
/// on the row store, with columnar enabled, and after disabling it.
#[test]
fn analytics() {
    use std::collections::{BTreeMap, BTreeSet};
    use storine::{
        and, avg, bucket, count, count_distinct, field, ge, max, min, sum, AggregateQuery, AggregateResult, Decimal,
    };

    let Some((_server, db, dir)) = single_node() else { return };
    let sales = db.collection("sales");
    struct Sale {
        key: String,
        region: &'static str,
        qty: i64,
        price: i64, // cents
        day: i64,
    }
    let regions = ["eu", "us", "apac"];
    let data: Vec<Sale> = (0..3000i64)
        .map(|i| Sale {
            key: format!("s{i:05}"),
            region: regions[(i % 3) as usize],
            qty: i % 17 + 1,
            price: (i * 7919) % 10000 + 100,
            day: i % 60,
        })
        .collect();
    for chunk in data.chunks(200) {
        db.transaction(|tx| {
            for s in chunk {
                tx.put(
                    "sales",
                    &s.key,
                    doc! { "region" => s.region, "qty" => s.qty, "price" => Decimal::new(i128::from(s.price), 2)?, "day" => s.day },
                );
            }
            Ok(())
        })
        .unwrap();
    }
    let cents = |m: i128| Value::Decimal(Decimal::new(m, 2).unwrap());
    // The server's exact mean: 6 more fractional digits, half-even.
    let mean = |sum: i128, scale: u8, n: i128| {
        let scaled = sum * 1_000_000;
        let (mut q, r) = (scaled / n, scaled % n);
        if (r.abs() * 2 > n) || (r.abs() * 2 == n && q % 2 != 0) {
            q += scaled.signum();
        }
        Value::Decimal(Decimal::new(q, scale + 6).unwrap())
    };

    // Grouped by region: count, sum(price * qty), avg(qty), min / max(price), count_distinct(day); -revenue, limit 2.
    #[derive(Default)]
    struct G {
        n: i64,
        qty: i128,
        revenue: i128,
        min: i64,
        max: i64,
        days: BTreeSet<i64>,
    }
    let mut groups: BTreeMap<&str, G> = BTreeMap::new();
    for s in &data {
        let g = groups.entry(s.region).or_insert_with(|| G { min: s.price, max: s.price, ..G::default() });
        g.n += 1;
        g.qty += i128::from(s.qty);
        g.revenue += i128::from(s.price * s.qty);
        g.min = g.min.min(s.price);
        g.max = g.max.max(s.price);
        g.days.insert(s.day);
    }
    let mut ranked: Vec<(&str, &G)> = groups.iter().map(|(r, g)| (*r, g)).collect();
    ranked.sort_by(|a, b| b.1.revenue.cmp(&a.1.revenue).then(a.0.cmp(b.0)));
    let grouped: Vec<Vec<Value>> = ranked[..2]
        .iter()
        .map(|(r, g)| {
            vec![
                Value::from(*r),
                Value::Int(g.n),
                cents(g.revenue),
                mean(g.qty, 0, i128::from(g.n)),
                cents(i128::from(g.min)),
                cents(i128::from(g.max)),
                Value::Int(g.days.len() as i64),
            ]
        })
        .collect();
    let grouped_query = AggregateQuery::new()
        .agg("orders", count())
        .agg("revenue", sum(field("price") * field("qty")))
        .agg("avg_qty", avg("qty"))
        .agg("min_price", min("price"))
        .agg("max_price", max("price"))
        .agg("days", count_distinct("day"))
        .group_by("region")
        .order_by("-revenue")
        .limit(2);

    // Weekly buckets of day (width 7, offset 3): sum(qty), count.
    let mut buckets: BTreeMap<i64, (i64, i64)> = BTreeMap::new();
    for s in &data {
        let b = 3 + (s.day - 3).div_euclid(7) * 7;
        let e = buckets.entry(b).or_default();
        e.0 += s.qty;
        e.1 += 1;
    }
    let bucket_rows: Vec<Vec<Value>> =
        buckets.iter().map(|(b, (q, n))| vec![Value::Int(*b), Value::Int(*q), Value::Int(*n)]).collect();
    let bucket_query =
        AggregateQuery::new().agg("units", sum("qty")).agg("n", count()).group_by(bucket("day", 7, 3)).order_by("day");

    // Filtered totals: region us and day >= 30.
    let picked: Vec<&Sale> = data.iter().filter(|s| s.region == "us" && s.day >= 30).collect();
    let (fqty, fsum) = picked.iter().fold((0i64, 0i128), |(q, p), s| (q + s.qty, p + i128::from(s.price)));
    let filtered =
        vec![vec![Value::Int(picked.len() as i64), Value::Int(fqty), cents(fsum), mean(fsum, 2, picked.len() as i128)]];
    let filtered_query = AggregateQuery::new()
        .agg("n", count())
        .agg("units", sum("qty"))
        .agg("total", sum("price"))
        .agg("avg_price", avg("price"))
        .filter(and([eq("region", "us"), ge("day", 30)]));

    let run_all = |mode: &str| {
        let r: AggregateResult = sales.aggregate(grouped_query.clone()).unwrap();
        assert_eq!(r.matched, 3000, "{mode}");
        assert_eq!(r.columns, ["region", "orders", "revenue", "avg_qty", "min_price", "max_price", "days"], "{mode}");
        assert_eq!(r.rows, grouped, "{mode}: grouped");
        assert_eq!(r.get(0, "revenue"), Some(&grouped[0][2]));
        assert_eq!(sales.aggregate(bucket_query.clone()).unwrap().rows, bucket_rows, "{mode}: buckets");
        let r = sales.aggregate(filtered_query.clone()).unwrap();
        assert_eq!(r.matched, picked.len() as u64, "{mode}");
        assert_eq!(r.rows, filtered, "{mode}: filtered");
    };

    run_all("row store");
    assert!(matches!(sales.aggregate(grouped_query.clone().order_by("nope")), Err(Error::Invalid(_))));

    sales.enable_columnar().unwrap();
    run_all("columnar");

    let eu: Vec<&Sale> = data.iter().filter(|s| s.region == "eu").collect();
    let mut scan = sales.scan_columns(&["qty", "price"], eq("region", "eu"), 500);
    let rows: Vec<_> = scan.by_ref().collect::<storine::Result<_>>().unwrap();
    assert_eq!(scan.total(), Some(1000));
    assert_eq!(rows.len(), eu.len());
    for (row, s) in rows.iter().zip(&eu) {
        assert_eq!(row.key, s.key);
        assert_eq!(row.values, vec![Value::Int(s.qty), cents(i128::from(s.price))]);
        assert_eq!(row.get("qty"), Some(&Value::Int(s.qty)));
    }

    sales.disable_columnar().unwrap();
    run_all("columnar disabled");
    let _ = std::fs::remove_dir_all(dir);
}
