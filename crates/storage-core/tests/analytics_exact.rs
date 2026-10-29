//! Analytic queries (`TASK-127`, `TASK-128`) are exact: over column segments plus delta they
//! return exactly what the same aggregation over the documents returns, and they select exactly
//! the documents that document queries select, for every filter shape and value kind.

use std::collections::BTreeMap;
use std::path::PathBuf;

use storage_codec::types::PropertyValue as P;
use storage_core::analytics::{run_documents, AggFunc, Aggregate, AggregateQuery, Expr, GroupKey};
use storage_core::{columnar, DocQuery, DocumentStore, EngineConfig, Filter};
use storage_types::Decimal;

fn dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("stge_olap_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

struct Rng(u64);
impl Rng {
    fn next(&mut self, m: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % m.max(1)
    }
}

fn dec(m: i128, s: u8) -> P {
    P::Decimal(Decimal::new(m, s).unwrap())
}

const CITIES: [&str; 6] = ["Lagos", "Berlin", "Lima", "Osaka", "Accra", "Bern"];

fn doc(r: &mut Rng, i: u64) -> BTreeMap<String, P> {
    let mut m = BTreeMap::new();
    m.insert("n".into(), P::Int64(r.next(1000) as i64 - 300));
    m.insert("price".into(), dec(i128::from(r.next(100_000)) - 20_000, 2));
    m.insert("qty".into(), P::Int64(r.next(50) as i64));
    m.insert("city".into(), P::Text(CITIES[r.next(6) as usize].into()));
    m.insert(
        "f".into(),
        P::Float64(match r.next(40) {
            0 => f64::NAN,
            1 => -0.0,
            2 => f64::INFINITY,
            _ => (r.next(20_000) as f64 - 10_000.0) / 8.0,
        }),
    );
    m.insert(
        "mixed".into(),
        match r.next(5) {
            0 => P::Int64(r.next(100) as i64),
            1 => P::Text(format!("t{}", r.next(10))),
            2 => P::Float64(r.next(100) as f64 + 0.5),
            3 => dec(i128::from(r.next(1000)), 1),
            _ => P::Bool(r.next(2) == 0),
        },
    );
    m.insert("ts".into(), P::TimestampNs(1_700_000_000_000_000_000 + r.next(1_000_000) as i64 * 1_000_000));
    m.insert("u".into(), P::UInt64(u64::MAX - r.next(1000)));
    m.insert("flag".into(), P::Bool(i % 3 == 0));
    match r.next(4) {
        0 => {}
        1 => {
            m.insert("opt".into(), P::Null);
        }
        _ => {
            m.insert("opt".into(), P::Int64(r.next(10) as i64));
        }
    }
    if r.next(3) > 0 {
        let mut inner = BTreeMap::new();
        inner.insert("a".into(), P::Int64(r.next(20) as i64));
        inner.insert("b".into(), P::Text(CITIES[r.next(6) as usize].into()));
        m.insert("meta".into(), P::Map(inner));
    }
    if r.next(4) == 0 {
        m.insert("list".into(), P::List(vec![P::Int64(r.next(5) as i64), P::Text("x".into())]));
    }
    m
}

fn probe(r: &mut Rng) -> P {
    match r.next(9) {
        0 => P::Int64(r.next(1000) as i64 - 300),
        1 => dec(i128::from(r.next(100_000)) - 20_000, 2),
        2 => P::Float64((r.next(20_000) as f64 - 10_000.0) / 8.0),
        3 => P::Text(CITIES[r.next(6) as usize].into()),
        4 => P::Bool(r.next(2) == 0),
        5 => P::Null,
        6 => dec(i128::from(r.next(1000)), 1),
        7 => P::UInt64(u64::MAX - r.next(1000)),
        _ => P::Int64(r.next(50) as i64),
    }
}

const PATHS: [&str; 13] = ["n", "price", "qty", "city", "f", "mixed", "ts", "u", "flag", "opt", "meta.a", "meta.b", "meta"];

fn leaf(r: &mut Rng) -> Filter {
    let p = PATHS[r.next(PATHS.len() as u64) as usize].to_owned();
    match r.next(12) {
        0 => Filter::Eq(p, probe(r)),
        1 => Filter::Ne(p, probe(r)),
        2 => Filter::Lt(p, probe(r)),
        3 => Filter::Le(p, probe(r)),
        4 => Filter::Gt(p, probe(r)),
        5 => Filter::Ge(p, probe(r)),
        6 => {
            let (a, b) = (probe(r), probe(r));
            Filter::Between(p, a, b)
        }
        7 => Filter::In(p, vec![probe(r), probe(r), probe(r)]),
        8 => Filter::Prefix(p, ["L", "Be", "O", "t", ""][r.next(5) as usize].into()),
        9 => Filter::Exists(p),
        10 => Filter::Missing(p),
        _ => Filter::KeyPrefix(format!("k{}", r.next(10))),
    }
}

fn filter(r: &mut Rng, depth: u32) -> Filter {
    if depth == 0 || r.next(3) == 0 {
        return leaf(r);
    }
    match r.next(3) {
        0 => Filter::And((0..2 + r.next(2)).map(|_| filter(r, depth - 1)).collect()),
        1 => Filter::Or((0..2 + r.next(2)).map(|_| filter(r, depth - 1)).collect()),
        _ => Filter::Not(Box::new(filter(r, depth - 1))),
    }
}

fn field(p: &str) -> Option<Expr> {
    Some(Expr::Field(p.to_owned()))
}

fn aggregates() -> Vec<Aggregate> {
    let a = |func, arg| Aggregate { func, arg };
    vec![
        a(AggFunc::Count, None),
        a(AggFunc::Count, field("opt")),
        a(AggFunc::Sum, field("price")),
        a(AggFunc::Sum, field("n")),
        a(AggFunc::Sum, field("f")),
        a(AggFunc::Sum, field("mixed")),
        a(AggFunc::Sum, field("u")),
        a(AggFunc::Avg, field("qty")),
        a(AggFunc::Avg, field("price")),
        a(AggFunc::Min, field("city")),
        a(AggFunc::Max, field("price")),
        a(AggFunc::Min, field("mixed")),
        a(AggFunc::Max, field("ts")),
        a(AggFunc::CountDistinct, field("city")),
        a(AggFunc::CountDistinct, field("mixed")),
        a(AggFunc::ApproxCountDistinct, field("n")),
        // sum(price * (1 - qty / 100)) the TPC-H way, exactly.
        a(
            AggFunc::Sum,
            Some(Expr::Mul(
                Box::new(Expr::Field("price".into())),
                Box::new(Expr::Sub(Box::new(Expr::Const(dec(1, 0))), Box::new(Expr::Mul(Box::new(Expr::Field("qty".into())), Box::new(Expr::Const(dec(1, 2))))))),
            )),
        ),
        a(AggFunc::Sum, Some(Expr::Add(Box::new(Expr::Field("n".into())), Box::new(Expr::Field("f".into()))))),
        a(AggFunc::Max, Some(Expr::Neg(Box::new(Expr::Field("meta.a".into()))))),
    ]
}

fn same(a: &P, b: &P) -> bool {
    match (a, b) {
        (P::Float64(x), P::Float64(y)) => x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan()),
        _ => a == b,
    }
}

#[test]
fn columnar_aggregates_equal_document_aggregates_for_random_filters() {
    let d = dir("exact");
    let s = DocumentStore::open(&d, EngineConfig::default()).unwrap();
    let mut r = Rng(0x1234_5678_9ABC_DEF1);
    for i in 0..6000u64 {
        s.put("t", &format!("k{i:05}"), doc(&mut r, i)).unwrap();
    }
    s.create_columnar("t").unwrap();
    s.columnar("t").unwrap().unwrap().merge().unwrap();
    // A delta on top of the segments: updates, deletes, inserts.
    for i in (0..6000u64).step_by(7) {
        s.put("t", &format!("k{i:05}"), doc(&mut r, i)).unwrap();
    }
    for i in (3..6000u64).step_by(11) {
        s.delete("t", &format!("k{i:05}")).unwrap();
    }
    for i in 6000..6300u64 {
        s.put("t", &format!("k{i:05}"), doc(&mut r, i)).unwrap();
    }
    let docs = columnar::snapshot_documents(s.engine(), "t").unwrap();
    let groupings: Vec<Vec<GroupKey>> = vec![
        vec![],
        vec![GroupKey::Field("city".into())],
        vec![GroupKey::Field("mixed".into())],
        vec![GroupKey::Field("city".into()), GroupKey::Field("flag".into())],
        vec![GroupKey::Bucket { field: "n".into(), width: 100, offset: 0 }],
        vec![GroupKey::Field("meta.b".into()), GroupKey::Field("opt".into())],
    ];
    // Columnar scans page through exactly what the document path returns.
    for round in 0..40 {
        let f = if round == 0 { None } else { Some(filter(&mut r, 2)) };
        let fields: Vec<String> = ["price", "meta", "meta.b", "list", "opt", "nope", "mixed"].iter().take(1 + round % 7).map(|s| (*s).to_owned()).collect();
        let mut q = storage_core::analytics::ScanQuery { collection: "t".into(), filter: f.clone(), fields, after_key: None, limit: 300 + (round * 397) % 2500 };
        let mut pages = 0;
        loop {
            let got = s.scan_columns(&q).unwrap();
            let want = storage_core::analytics::scan_documents(&q, &docs).unwrap();
            assert_eq!(got.total, want.total, "total for {f:?}");
            assert_eq!(got.next_after_key, want.next_after_key);
            assert_eq!(got.rows.len(), want.rows.len());
            for (g, w) in got.rows.iter().zip(&want.rows) {
                assert_eq!(g.0, w.0);
                for (a, b) in g.1.iter().zip(&w.1) {
                    assert!(same(a, b), "{} differs: {a:?} vs {b:?}", g.0);
                }
            }
            pages += 1;
            match got.next_after_key {
                Some(k) => q.after_key = Some(k),
                None => break,
            }
        }
        assert!(pages >= 1);
    }
    let mut checked = 0;
    for round in 0..300 {
        let f = if round == 0 { None } else { Some(filter(&mut r, 3)) };
        let q = AggregateQuery {
            collection: "t".into(),
            filter: f.clone(),
            group_by: groupings[round % groupings.len()].clone(),
            aggregates: aggregates(),
            ..Default::default()
        };
        let got = s.aggregate(&q).unwrap();
        let want = run_documents(&q, &docs).unwrap();
        assert_eq!(got.matched, want.matched, "matched rows for {f:?}");
        assert_eq!(got.rows.len(), want.rows.len(), "groups for {f:?}");
        for (g, w) in got.rows.iter().zip(&want.rows) {
            for (i, (a, b)) in g.iter().zip(w).enumerate() {
                assert!(same(a, b), "column {i} differs for {f:?} groups {:?}: {a:?} vs {b:?}", q.group_by);
            }
        }
        // The same filter through the document query engine selects the same documents.
        let page = s.query(&DocQuery { collection: "t".into(), filter: f.clone(), limit: Some(0), ..Default::default() }).unwrap();
        assert_eq!(page.total as u64, got.matched, "document query vs analytics for {f:?}");
        checked += 1;
    }
    assert_eq!(checked, 300);
    let _ = std::fs::remove_dir_all(&d);
}

/// Zone-map pruning (`TASK-128`) never changes a result: clustered columns over several row
/// groups, comparison filters with probes of every numeric kind around the zone bounds, nulls,
/// signed zeros and a mixed-kind row group.
#[test]
fn zone_map_pruning_is_exact() {
    use storage_core::{DocOp, DocWrite, Precondition};
    let d = dir("zones");
    let s = DocumentStore::open(&d, EngineConfig::default()).unwrap();
    let rows = 3 * storage_columnar::ROW_GROUP_ROWS as u64 + 500;
    let mut batch = Vec::new();
    for i in 0..rows {
        let mut m = BTreeMap::new();
        m.insert("seq".to_owned(), P::Int64(i as i64 - 20_000));
        m.insert("ts".to_owned(), P::TimestampNs(i as i64 * 1000));
        m.insert("d".to_owned(), dec(i128::from(i), 2));
        m.insert("f".to_owned(), P::Float64(if i % 97 == 0 { -0.0 } else { i as f64 / 4.0 - 3000.0 }));
        // The last row group's `g` mixes kinds; the second has a null.
        let g = match i {
            _ if i >= 3 * storage_columnar::ROW_GROUP_ROWS as u64 && i % 2 == 0 => P::Text("x".into()),
            20_000 => P::Null,
            _ => P::Int64((i / 1000) as i64),
        };
        m.insert("g".to_owned(), g);
        batch.push(DocWrite { collection: "z".into(), key: format!("k{i:06}"), op: DocOp::Put(m), precondition: Precondition::None });
        if batch.len() == 2000 {
            s.write(std::mem::take(&mut batch), None).unwrap();
        }
    }
    s.write(batch, None).unwrap();
    s.create_columnar("z").unwrap();
    s.columnar("z").unwrap().unwrap().merge().unwrap();
    let docs = columnar::snapshot_documents(s.engine(), "z").unwrap();
    let mut r = Rng(0xFEED_5EED_0123_4567);
    let bound = |r: &mut Rng| -> i64 { [0i64, 16_383, 16_384, 32_767, 32_768, 49_152, 49_651, 60_000][r.next(8) as usize] + r.next(3) as i64 - 1 };
    let mut pruned = 0;
    for _ in 0..400 {
        let p = ["seq", "ts", "d", "f", "g", "missing"][r.next(6) as usize];
        let x = bound(&mut r);
        let v = match r.next(6) {
            0 => P::Int64(x - 20_000),
            1 => P::TimestampNs(x * 1000),
            2 => dec(i128::from(x), 2),
            3 => P::Float64(x as f64 / 4.0 - 3000.0),
            4 => P::Float64(-0.0),
            _ => P::Int64(x / 1000),
        };
        let leaf = match r.next(8) {
            0 => Filter::Lt(p.into(), v),
            1 => Filter::Le(p.into(), v),
            2 => Filter::Gt(p.into(), v),
            3 => Filter::Ge(p.into(), v),
            4 => Filter::Eq(p.into(), v),
            5 => Filter::In(p.into(), vec![v, P::Int64(r.next(70) as i64)]),
            6 => Filter::Between(p.into(), v.clone(), P::Int64(r.next(70_000) as i64 - 20_000)),
            _ => Filter::Not(Box::new(Filter::Lt(p.into(), v))),
        };
        let f = match r.next(3) {
            0 => leaf,
            1 => Filter::And(vec![leaf, Filter::Ge("seq".into(), P::Int64(r.next(60_000) as i64 - 20_000))]),
            _ => Filter::Or(vec![leaf, Filter::Lt("ts".into(), P::TimestampNs(r.next(60_000) as i64 * 1000))]),
        };
        let q = AggregateQuery {
            collection: "z".into(),
            filter: Some(f.clone()),
            group_by: vec![GroupKey::Bucket { field: "seq".into(), width: 10_000, offset: 0 }],
            aggregates: vec![Aggregate { func: AggFunc::Count, arg: None }, Aggregate { func: AggFunc::Sum, arg: field("d") }],
            ..Default::default()
        };
        let got = s.aggregate(&q).unwrap();
        let want = run_documents(&q, &docs).unwrap();
        assert_eq!(got.matched, want.matched, "matched rows for {f:?}");
        assert_eq!(got.rows, want.rows, "rows for {f:?}");
        pruned += got.pruned_row_groups;
    }
    assert!(pruned > 100, "zone maps pruned only {pruned} row groups");
    let _ = std::fs::remove_dir_all(&d);
}

/// Group tables beyond the memory budget spill (`RULE-OLAP-04`): the result equals the
/// in-memory one exactly, for every aggregate and with ordering and limits; spill runs are
/// encrypted and removed; without a spill directory the budget fails cleanly.
#[test]
fn spilled_group_by_equals_in_memory() {
    use storage_core::{DocOp, DocWrite, Precondition};
    let d = dir("spill");
    let s = DocumentStore::open(&d, EngineConfig::default()).unwrap();
    let mut r = Rng(0x0BAD_5EED_7777_1234);
    let mut batch = Vec::new();
    for i in 0..40_000u64 {
        let mut m = doc(&mut r, i);
        m.insert("key".into(), P::Int64((i * 7919 % 25_000) as i64));
        m.insert("label".into(), P::Text(format!("label-{}", i % 9_000)));
        batch.push(DocWrite { collection: "s".into(), key: format!("k{i:06}"), op: DocOp::Put(m), precondition: Precondition::None });
        if batch.len() == 2000 {
            s.write(std::mem::take(&mut batch), None).unwrap();
        }
    }
    s.create_columnar("s").unwrap();
    s.columnar("s").unwrap().unwrap().merge().unwrap();
    // Some delta on top.
    for i in 0..500u64 {
        let mut m = doc(&mut r, i);
        m.insert("key".into(), P::Int64(30_000 + i as i64));
        m.insert("label".into(), P::Text(format!("late-{i}")));
        s.put("s", &format!("k{i:06}"), m).unwrap();
    }
    let docs = columnar::snapshot_documents(s.engine(), "s").unwrap();
    let spill_dir = d.join("columnar").join("spill");
    type Case = (Vec<GroupKey>, Vec<(usize, storage_core::Direction)>, Option<usize>);
    let cases: Vec<Case> = vec![
        (vec![GroupKey::Field("key".into())], vec![], None),
        (vec![GroupKey::Field("label".into()), GroupKey::Field("flag".into())], vec![], None),
        (vec![GroupKey::Field("key".into())], vec![(3, storage_core::Direction::Desc)], Some(25)),
        (vec![], vec![], None),
    ];
    for (group_by, order_by, limit) in cases {
        let mut q = AggregateQuery { collection: "s".into(), group_by, aggregates: aggregates(), order_by, limit, ..Default::default() };
        // The distinct-only case needs a budget below its sets' size.
        q.aggregates.push(Aggregate { func: AggFunc::CountDistinct, arg: field("key") });
        let want = run_documents(&q, &docs).unwrap();
        q.memory_budget = 256 << 10;
        let got = s.aggregate(&q).unwrap();
        assert!(got.spilled_bytes > 0, "the query should have spilled for {:?}", q.group_by);
        assert_eq!(got.matched, want.matched);
        assert_eq!(got.rows.len(), want.rows.len(), "groups for {:?}", q.group_by);
        for (g, w) in got.rows.iter().zip(&want.rows) {
            for (i, (a, b)) in g.iter().zip(w).enumerate() {
                assert!(same(a, b), "column {i} differs for {:?}: {a:?} vs {b:?}", q.group_by);
            }
        }
        // Runs are gone once the query returns.
        let left: Vec<_> = std::fs::read_dir(&spill_dir).map(|e| e.flatten().collect()).unwrap_or_default();
        assert!(left.is_empty(), "spill runs left behind: {left:?}");
        // Without a spill directory the same budget is a clean error.
        let err = run_documents(&q, &docs).unwrap_err();
        assert!(format!("{err:?}").contains("memory"), "{err:?}");
    }
    let _ = std::fs::remove_dir_all(&d);
}
