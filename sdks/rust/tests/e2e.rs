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
    add, append, between, dec, doc, eq, gt, list, matches, set, unset, Client, Error, Options, Query, Value,
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
