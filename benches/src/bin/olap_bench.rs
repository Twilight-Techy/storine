//! Analytics benchmark against DuckDB (`TASK-131`): TPC-H-shaped `lineitem` data, Q1 and Q6.
//!
//! ```text
//! olap-bench gen   <dir> <rows>          # dir/lineitem.csv (for DuckDB) + dir/store (Storine)
//! olap-bench query <dir> <q1|q6> <reps>  # timings (ms) and the result rows
//! ```
//!
//! The same deterministic rows go to both systems, so their answers can be compared exactly
//! (`benches/olap/duckdb_bench.py` runs the DuckDB side).

use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;
use std::time::Instant;

use storage_codec::types::PropertyValue as P;
use storage_core::analytics::{AggFunc, Aggregate, AggregateQuery, Expr, GroupKey};
use storage_core::{DocOp, DocWrite, DocumentStore, EngineConfig, Filter, Precondition};
use storage_types::Decimal;
use storage_wal::SyncPolicy;

const DAY_NS: i64 = 86_400_000_000_000;

/// Days since 1970-01-01 of a civil date.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

struct Row {
    orderkey: i64,
    partkey: i64,
    suppkey: i64,
    linenumber: i64,
    quantity: i64, // cents
    extprice: i64, // cents
    discount: i64, // hundredths
    tax: i64,      // hundredths
    returnflag: &'static str,
    linestatus: &'static str,
    ship: i64,
    commit: i64,
    receipt: i64,
    instruct: &'static str,
    mode: &'static str,
    comment: String,
}

fn row(i: u64) -> Row {
    let mut x = i.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
    let mut rnd = move |m: u64| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x % m
    };
    let qty = rnd(50) as i64 + 1;
    let ship = days_from_civil(1992, 1, 2) + rnd(2526) as i64;
    // As in TPC-H: returned items were received before 1995-06-17; open lines ship after it.
    let receipt = ship + rnd(30) as i64 + 1;
    let cutoff = days_from_civil(1995, 6, 17);
    let returnflag = if receipt <= cutoff {
        ["R", "A"][rnd(2) as usize]
    } else {
        "N"
    };
    let linestatus = if ship > cutoff { "O" } else { "F" };
    let words = [
        "carefully",
        "final",
        "deposits",
        "sleep",
        "furiously",
        "regular",
        "accounts",
        "ironic",
        "packages",
        "bold",
    ];
    let comment: Vec<&str> = (0..3 + rnd(4))
        .map(|_| words[rnd(words.len() as u64) as usize])
        .collect();
    Row {
        orderkey: (i / 4) as i64 + 1,
        partkey: rnd(200_000) as i64 + 1,
        suppkey: rnd(10_000) as i64 + 1,
        linenumber: (i % 4) as i64 + 1,
        quantity: qty * 100,
        extprice: qty * (90_000 + rnd(10_000_000) as i64) / 100,
        discount: rnd(11) as i64,
        tax: rnd(9) as i64,
        returnflag,
        linestatus,
        ship,
        commit: ship + rnd(60) as i64 - 30,
        receipt,
        instruct: [
            "DELIVER IN PERSON",
            "COLLECT COD",
            "NONE",
            "TAKE BACK RETURN",
        ][rnd(4) as usize],
        mode: ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"][rnd(7) as usize],
        comment: comment.join(" "),
    }
}

fn dec(m: i64, s: u8) -> P {
    P::Decimal(Decimal::new(i128::from(m), s).unwrap_or_else(|_| Decimal::new(0, 0).unwrap()))
}

fn date(d: i64) -> String {
    let (y, m, dd) = civil_from_days(d);
    format!("{y:04}-{m:02}-{dd:02}")
}

fn cents(v: i64) -> String {
    format!("{}.{:02}", v / 100, v % 100)
}

fn doc(r: &Row) -> BTreeMap<String, P> {
    let mut m = BTreeMap::new();
    m.insert("l_orderkey".into(), P::Int64(r.orderkey));
    m.insert("l_partkey".into(), P::Int64(r.partkey));
    m.insert("l_suppkey".into(), P::Int64(r.suppkey));
    m.insert("l_linenumber".into(), P::Int64(r.linenumber));
    m.insert("l_quantity".into(), dec(r.quantity, 2));
    m.insert("l_extendedprice".into(), dec(r.extprice, 2));
    m.insert("l_discount".into(), dec(r.discount, 2));
    m.insert("l_tax".into(), dec(r.tax, 2));
    m.insert("l_returnflag".into(), P::Text(r.returnflag.into()));
    m.insert("l_linestatus".into(), P::Text(r.linestatus.into()));
    m.insert("l_shipdate".into(), P::TimestampNs(r.ship * DAY_NS));
    m.insert("l_commitdate".into(), P::TimestampNs(r.commit * DAY_NS));
    m.insert("l_receiptdate".into(), P::TimestampNs(r.receipt * DAY_NS));
    m.insert("l_shipinstruct".into(), P::Text(r.instruct.into()));
    m.insert("l_shipmode".into(), P::Text(r.mode.into()));
    m.insert("l_comment".into(), P::Text(r.comment.clone()));
    m
}

fn gen(dir: &Path, rows: u64) {
    std::fs::create_dir_all(dir).expect("create dir");
    let mut csv =
        std::io::BufWriter::new(std::fs::File::create(dir.join("lineitem.csv")).expect("csv"));
    let config = EngineConfig {
        wal_sync_policy: SyncPolicy::AsyncMemoryFirst,
        ..EngineConfig::default()
    };
    let store = DocumentStore::open(dir.join("store"), config).expect("open store");
    let t = Instant::now();
    let mut batch = Vec::with_capacity(2000);
    for i in 0..rows {
        let r = row(i);
        writeln!(
            csv,
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
            r.orderkey,
            r.partkey,
            r.suppkey,
            r.linenumber,
            cents(r.quantity),
            cents(r.extprice),
            cents(r.discount),
            cents(r.tax),
            r.returnflag,
            r.linestatus,
            date(r.ship),
            date(r.commit),
            date(r.receipt),
            r.instruct,
            r.mode,
            r.comment
        )
        .expect("write csv");
        batch.push(DocWrite {
            collection: "lineitem".into(),
            key: format!("{i:010}"),
            op: DocOp::Put(doc(&r)),
            precondition: Precondition::None,
        });
        if batch.len() == 2000 {
            store.write(std::mem::take(&mut batch), None).expect("load");
        }
    }
    if !batch.is_empty() {
        store.write(batch, None).expect("load");
    }
    csv.flush().expect("flush csv");
    println!("loaded {rows} rows in {:?}", t.elapsed());
    let t = Instant::now();
    store.create_columnar("lineitem").expect("columnar");
    let c = store
        .columnar("lineitem")
        .expect("columnar")
        .expect("columnar");
    c.merge().expect("merge");
    println!(
        "built column segments in {:?} ({} rows)",
        t.elapsed(),
        c.segment_rows()
    );
    store.engine().flush_all().expect("flush");
}

fn ts(y: i64, m: i64, d: i64) -> P {
    P::TimestampNs(days_from_civil(y, m, d) * DAY_NS)
}

fn f(p: &str) -> Box<Expr> {
    Box::new(Expr::Field(p.into()))
}

fn one_minus(p: &str) -> Box<Expr> {
    Box::new(Expr::Sub(Box::new(Expr::Const(dec(1, 0))), f(p)))
}

fn q1() -> AggregateQuery {
    let a = |func, arg: Option<Expr>| Aggregate { func, arg };
    let disc_price = Expr::Mul(f("l_extendedprice"), one_minus("l_discount"));
    let charge = Expr::Mul(
        Box::new(disc_price.clone()),
        Box::new(Expr::Add(Box::new(Expr::Const(dec(1, 0))), f("l_tax"))),
    );
    AggregateQuery {
        collection: "lineitem".into(),
        filter: Some(Filter::Le("l_shipdate".into(), ts(1998, 9, 2))),
        group_by: vec![
            GroupKey::Field("l_returnflag".into()),
            GroupKey::Field("l_linestatus".into()),
        ],
        aggregates: vec![
            a(AggFunc::Sum, Some(Expr::Field("l_quantity".into()))),
            a(AggFunc::Sum, Some(Expr::Field("l_extendedprice".into()))),
            a(AggFunc::Sum, Some(disc_price)),
            a(AggFunc::Sum, Some(charge)),
            a(AggFunc::Avg, Some(Expr::Field("l_quantity".into()))),
            a(AggFunc::Avg, Some(Expr::Field("l_extendedprice".into()))),
            a(AggFunc::Avg, Some(Expr::Field("l_discount".into()))),
            a(AggFunc::Count, None),
        ],
        ..Default::default()
    }
}

fn q6() -> AggregateQuery {
    AggregateQuery {
        collection: "lineitem".into(),
        filter: Some(Filter::And(vec![
            Filter::Ge("l_shipdate".into(), ts(1994, 1, 1)),
            Filter::Lt("l_shipdate".into(), ts(1995, 1, 1)),
            Filter::Between("l_discount".into(), dec(5, 2), dec(7, 2)),
            Filter::Lt("l_quantity".into(), dec(24, 0)),
        ])),
        aggregates: vec![Aggregate {
            func: AggFunc::Sum,
            arg: Some(Expr::Mul(f("l_extendedprice"), f("l_discount"))),
        }],
        ..Default::default()
    }
}

fn query(dir: &Path, which: &str, reps: usize) {
    let store =
        DocumentStore::open(dir.join("store"), EngineConfig::default()).expect("open store");
    let q = match which {
        "q1" => q1(),
        "q6" => q6(),
        other => panic!("unknown query {other}"),
    };
    let t = Instant::now();
    let c = store
        .columnar("lineitem")
        .expect("columnar")
        .expect("columnar");
    let snap_time = {
        let t = Instant::now();
        let s = c.snapshot().expect("snapshot");
        drop(s);
        t.elapsed()
    };
    eprintln!("open {:?}, snapshot {snap_time:?}", t.elapsed());
    let mut times = Vec::with_capacity(reps);
    let mut result = None;
    for _ in 0..reps {
        let t = Instant::now();
        let r = store.aggregate(&q).expect("aggregate");
        times.push(t.elapsed().as_secs_f64() * 1e3);
        result = Some(r);
    }
    times.sort_by(f64::total_cmp);
    println!(
        "storine {which}: min {:.1} ms, median {:.1} ms ({reps} runs)",
        times[0],
        times[times.len() / 2]
    );
    if let Some(r) = result {
        println!("matched {}", r.matched);
        for row in r.rows {
            let cells: Vec<String> = row
                .iter()
                .map(|v| match v {
                    P::Decimal(d) => d.to_string(),
                    P::Text(s) => s.clone(),
                    P::Int64(i) => i.to_string(),
                    other => format!("{other:?}"),
                })
                .collect();
            println!("  {}", cells.join(" | "));
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("gen") => gen(Path::new(&args[2]), args[3].parse().expect("rows")),
        Some("query") => query(
            Path::new(&args[2]),
            &args[3],
            args.get(4).map_or(5, |r| r.parse().expect("reps")),
        ),
        _ => eprintln!("usage: olap-bench gen <dir> <rows> | query <dir> <q1|q6> [reps]"),
    }
}
