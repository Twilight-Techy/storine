//! Generates and pins the shared SDK conformance vectors (`protocol/vectors/vectors.json`).
//!
//! The vectors are produced by the server's own encoders, so every SDK that reproduces them is
//! byte-compatible with the server. Run with `STORINE_UPDATE_VECTORS=1` to rewrite the file after
//! an intentional protocol change; otherwise the test fails if the file and the encoders disagree.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::Duration;

use storage_codec::field_ops::FieldOp;
use storage_codec::value_wire::{encode_map_body, encode_value};
use storage_codec::PropertyValue;
use storage_core::document_wire::{encode_page, encode_query, encode_write_request, encode_write_response};
use storage_core::{DocOp, DocPage, DocQuery, DocWrite, Direction, Document, Filter, Precondition};
use storage_types::{InlineId, Version};

fn hex(b: &[u8]) -> String {
    b.iter().fold(String::new(), |mut s, x| {
        let _ = write!(s, "{x:02x}");
        s
    })
}

fn esc(s: &str) -> String {
    let mut o = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => o.push_str("\\\""),
            '\\' => o.push_str("\\\\"),
            c if (c as u32) < 0x20 => {
                let _ = write!(o, "\\u{:04x}", c as u32);
            }
            c => o.push(c),
        }
    }
    o.push('"');
    o
}

/// JSON description of a value: `{"type": ..., "value": ...}` with exact string forms.
fn describe(v: &PropertyValue) -> String {
    match v {
        PropertyValue::Null => r#"{"type":"null"}"#.into(),
        PropertyValue::Bool(b) => format!(r#"{{"type":"bool","value":{b}}}"#),
        PropertyValue::Int64(x) => format!(r#"{{"type":"int64","value":"{x}"}}"#),
        PropertyValue::UInt64(x) => format!(r#"{{"type":"uint64","value":"{x}"}}"#),
        PropertyValue::Float64(x) => format!(r#"{{"type":"float64","value":"{x:?}","bits":"{:016x}"}}"#, x.to_bits()),
        PropertyValue::Int128(x) => format!(r#"{{"type":"int128","value":"{x}"}}"#),
        PropertyValue::TimestampNs(x) => format!(r#"{{"type":"timestamp","value":"{x}"}}"#),
        PropertyValue::Uuid(u) => format!(r#"{{"type":"uuid","value":"{}"}}"#, hex(u)),
        PropertyValue::Id(id) => format!(r#"{{"type":"id","value":"{}"}}"#, hex(&id.to_raw_bytes())),
        PropertyValue::Text(s) => format!(r#"{{"type":"text","value":{}}}"#, esc(s)),
        PropertyValue::Bytes(b) => format!(r#"{{"type":"bytes","value":"{}"}}"#, hex(b)),
        PropertyValue::Decimal(d) => format!(r#"{{"type":"decimal","value":"{d}"}}"#),
        PropertyValue::List(items) => {
            let inner: Vec<String> = items.iter().map(describe).collect();
            format!(r#"{{"type":"list","value":[{}]}}"#, inner.join(","))
        }
        PropertyValue::Map(m) => {
            let inner: Vec<String> = m.iter().map(|(k, v)| format!("[{},{}]", esc(k), describe(v))).collect();
            format!(r#"{{"type":"map","value":[{}]}}"#, inner.join(","))
        }
    }
}

fn dec(s: &str) -> PropertyValue {
    PropertyValue::Decimal(s.parse().unwrap())
}

fn doc(pairs: &[(&str, PropertyValue)]) -> BTreeMap<String, PropertyValue> {
    pairs.iter().map(|(k, v)| ((*k).to_owned(), v.clone())).collect()
}

fn build() -> String {
    let mut out = String::from("{\n  \"format\": 1,\n  \"values\": [\n");
    let values: Vec<(&str, PropertyValue)> = vec![
        ("null", PropertyValue::Null),
        ("true", PropertyValue::Bool(true)),
        ("false", PropertyValue::Bool(false)),
        ("int64 zero", PropertyValue::Int64(0)),
        ("int64 negative", PropertyValue::Int64(-42)),
        ("int64 max", PropertyValue::Int64(i64::MAX)),
        ("int64 min", PropertyValue::Int64(i64::MIN)),
        ("uint64 max", PropertyValue::UInt64(u64::MAX)),
        ("float64", PropertyValue::Float64(1.25)),
        ("float64 negative zero", PropertyValue::Float64(-0.0)),
        ("int128 big", PropertyValue::Int128(170_141_183_460_469_231_731_687_303_715_884_105_727)),
        ("int128 negative", PropertyValue::Int128(-12_345_678_901_234_567_890_123_456_789)),
        ("timestamp", PropertyValue::TimestampNs(1_700_000_000_123_456_789)),
        ("timestamp before epoch", PropertyValue::TimestampNs(-1_000)),
        ("uuid", PropertyValue::Uuid([0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef])),
        ("id", PropertyValue::Id(InlineId::new("user_01").unwrap())),
        ("text", PropertyValue::Text("hé €".into())),
        ("empty text", PropertyValue::Text(String::new())),
        ("bytes", PropertyValue::Bytes(vec![0, 1, 254, 255])),
        ("decimal money", dec("1234.50")),
        ("decimal negative small", dec("-0.001")),
        ("decimal integer", dec("100")),
        ("decimal max precision", dec("0.12345678901234567890123456789012345678")),
        ("list", PropertyValue::List(vec![PropertyValue::Int64(1), PropertyValue::Text("a".into()), PropertyValue::Null])),
        ("empty list", PropertyValue::List(vec![])),
        ("map", PropertyValue::Map(doc(&[("b", PropertyValue::Bool(false)), ("a", PropertyValue::Int64(1))]))),
        (
            "nested",
            PropertyValue::Map(doc(&[(
                "order",
                PropertyValue::Map(doc(&[
                    ("items", PropertyValue::List(vec![PropertyValue::Map(doc(&[("sku", PropertyValue::Text("X1".into())), ("price", dec("9.99"))]))])),
                    ("total", dec("9.99")),
                ])),
            )])),
        ),
    ];
    let n = values.len();
    for (i, (name, v)) in values.iter().enumerate() {
        let mut b = Vec::new();
        encode_value(v, &mut b);
        let _ = writeln!(out, "    {{\"name\":{},\"value\":{},\"hex\":\"{}\"}}{}", esc(name), describe(v), hex(&b), if i + 1 < n { "," } else { "" });
    }
    out.push_str("  ],\n  \"documents\": [\n");
    let d = doc(&[("balance", dec("100.00")), ("owner", PropertyValue::Text("alice".into())), ("active", PropertyValue::Bool(true))]);
    let mut b = Vec::new();
    encode_map_body(&d, &mut b);
    let _ = writeln!(out, "    {{\"name\":\"account\",\"fields\":{},\"hex\":\"{}\"}}", describe(&PropertyValue::Map(d.clone())), hex(&b));
    out.push_str("  ],\n  \"requests\": [\n");
    let mut reqs: Vec<(String, u8, Vec<u8>)> = Vec::new();
    {
        let mut b = Vec::new();
        storage_core::document_wire::put_str(&mut b, "accounts");
        storage_core::document_wire::put_str(&mut b, "alice");
        reqs.push(("get accounts/alice".into(), 0x20, b.clone()));
        let mut r = b.clone();
        r.push(1);
        storage_core::document_wire::put_str(&mut r, "photo");
        r.extend_from_slice(&1000u64.to_le_bytes());
        r.extend_from_slice(&4u32.to_le_bytes());
        reqs.push(("get accounts/alice: bytes 1000..1004 of photo".into(), 0x20, r));
        let mut f = b;
        f.push(2);
        f.extend_from_slice(&2u32.to_le_bytes());
        storage_core::document_wire::put_str(&mut f, "owner");
        storage_core::document_wire::put_str(&mut f, "meta.size");
        reqs.push(("get accounts/alice: fields owner, meta.size".into(), 0x20, f));
    }
    let w = |c: &str, k: &str, op: DocOp, pre: Precondition| DocWrite { collection: c.into(), key: k.into(), op, precondition: pre };
    type WriteCase<'a> = (&'a str, Vec<DocWrite>, Option<[u8; 16]>);
    let write_cases: Vec<WriteCase<'_>> = vec![
        ("put", vec![w("accounts", "alice", DocOp::Put(d.clone()), Precondition::None)], None),
        ("insert (must not exist) with request id", vec![w("accounts", "alice", DocOp::Put(d.clone()), Precondition::Absent)], Some([0xAB; 16])),
        ("replace if version 7", vec![w("accounts", "alice", DocOp::Put(d.clone()), Precondition::Version(Version(7)))], None),
        ("put with ttl 1500 ms", vec![w("sessions", "s1", DocOp::PutWithTtl(doc(&[("u", PropertyValue::Int64(1))]), Duration::from_millis(1500)), Precondition::None)], None),
        ("delete", vec![w("accounts", "alice", DocOp::Delete, Precondition::None)], None),
        ("delete if exists", vec![w("accounts", "alice", DocOp::Delete, Precondition::Exists)], None),
        (
            "transfer 25.00 (two patches, no overdraft) with request id",
            vec![
                w("accounts", "alice", DocOp::Patch(vec![FieldOp::Add { path: "balance".into(), delta: dec("-25.00"), min: Some(PropertyValue::Int64(0)), max: None }]), Precondition::Exists),
                w("accounts", "bob", DocOp::Patch(vec![FieldOp::Add { path: "balance".into(), delta: dec("25.00"), min: None, max: None }]), Precondition::Exists),
            ],
            Some([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10]),
        ),
        (
            "patch set / unset / append / add with min and max",
            vec![w("c", "k", DocOp::Patch(vec![
                FieldOp::Set { path: "profile.name".into(), value: PropertyValue::Text("Ann".into()) },
                FieldOp::Unset { path: "tmp".into() },
                FieldOp::Append { path: "log".into(), value: PropertyValue::Int64(3) },
                FieldOp::Add { path: "n".into(), delta: PropertyValue::Int64(1), min: Some(PropertyValue::Int64(0)), max: Some(PropertyValue::Int64(10)) },
            ]), Precondition::Exists)],
            None,
        ),
        ("check version 3", vec![w("c", "k", DocOp::Check, Precondition::Version(Version(3)))], None),
    ];
    for (name, writes, rid) in write_cases {
        let mut b = Vec::new();
        encode_write_request(&writes, rid, &mut b);
        reqs.push((format!("write: {name}"), 0x21, b));
    }
    let q = |filter: Option<Filter>, order: Vec<(&str, Direction)>, limit: Option<usize>, offset: usize, after: Option<&str>| DocQuery {
        collection: "items".into(),
        filter,
        order_by: order.into_iter().map(|(p, d)| (p.to_owned(), d)).collect(),
        limit,
        offset,
        after_key: after.map(str::to_owned),
        nearest: None,
    };
    let query_cases: Vec<(&str, DocQuery)> = vec![
        ("all (default limit)", q(None, vec![], None, 0, None)),
        ("eq text", q(Some(Filter::Eq("kind".into(), PropertyValue::Text("book".into()))), vec![], Some(10), 0, None)),
        (
            "and(gte, lt) sorted desc, offset 20",
            q(Some(Filter::And(vec![Filter::Ge("price".into(), dec("10")), Filter::Lt("price".into(), dec("20.50"))])), vec![("price", Direction::Desc)], Some(10), 20, None),
        ),
        ("between", q(Some(Filter::Between("n".into(), PropertyValue::Int64(1), PropertyValue::Int64(5))), vec![], None, 0, None)),
        ("in", q(Some(Filter::In("n".into(), vec![PropertyValue::Int64(1), PropertyValue::Int64(2)])), vec![], None, 0, None)),
        ("prefix / contains / exists / missing", q(Some(Filter::Or(vec![
            Filter::Prefix("name".into(), "An".into()),
            Filter::Contains("tags".into(), PropertyValue::Text("x".into())),
            Filter::Exists("email".into()),
            Filter::Missing("deleted_at".into()),
        ])), vec![], None, 0, None)),
        ("not / ne / le / gt / key prefix", q(Some(Filter::And(vec![
            Filter::Not(Box::new(Filter::Ne("a".into(), PropertyValue::Int64(1)))),
            Filter::Le("b".into(), PropertyValue::Float64(2.5)),
            Filter::Gt("c".into(), PropertyValue::Int64(0)),
            Filter::KeyPrefix("user:".into()),
        ])), vec![], None, 0, None)),
        ("after key cursor", q(None, vec![], Some(100), 0, Some("k0099"))),
        ("multi-key sort", q(None, vec![("a", Direction::Asc), ("b", Direction::Desc)], Some(5), 0, None)),
        (
            "nearest neighbours with a filter",
            DocQuery {
                nearest: Some(storage_core::document::Nearest {
                    field: "embedding".into(),
                    vector: vec![0.25, -1.5, 3.0],
                    k: 5,
                    exact: false,
                }),
                ..q(Some(Filter::Eq("kind".into(), PropertyValue::Text("book".into()))), vec![], Some(5), 0, None)
            },
        ),
        (
            "nearest neighbours, exact",
            DocQuery {
                nearest: Some(storage_core::document::Nearest {
                    field: "v".into(),
                    vector: vec![1.0, 0.0],
                    k: 1,
                    exact: true,
                }),
                ..q(None, vec![], None, 0, None)
            },
        ),
    ];
    for (name, query) in query_cases {
        let mut b = Vec::new();
        encode_query(&query, &mut b);
        reqs.push((format!("query: {name}"), 0x22, b));
    }
    let mut ci = vec![1u8];
    storage_core::document_wire::put_str(&mut ci, "items");
    storage_core::document_wire::put_str(&mut ci, "price");
    reqs.push(("admin: create index items.price".into(), 0x23, ci));
    let mut info = vec![6u8];
    storage_core::document_wire::put_str(&mut info, "items");
    reqs.push(("info: collection items".into(), 0x25, info));
    reqs.push(("info: list collections".into(), 0x25, vec![3u8]));
    // Analytics (`TASK-129`).
    {
        use storage_core::analytics::{AggFunc, Aggregate, AggregateQuery, Expr, GroupKey, ScanQuery};
        use storage_core::document_wire::{encode_aggregate, encode_scan};
        let f = |p: &str| Some(Expr::Field(p.into()));
        let a = |func, arg| Aggregate { func, arg };
        let cases = vec![
            (
                "aggregate: by region where day >= 20, count / sum(price * qty) / avg(qty), sum desc, limit 2",
                AggregateQuery {
                    collection: "sales".into(),
                    filter: Some(Filter::Ge("day".into(), PropertyValue::Int64(20))),
                    group_by: vec![GroupKey::Field("region".into())],
                    aggregates: vec![
                        a(AggFunc::Count, None),
                        a(AggFunc::Sum, Some(Expr::Mul(Box::new(Expr::Field("price".into())), Box::new(Expr::Field("qty".into()))))),
                        a(AggFunc::Avg, f("qty")),
                    ],
                    order_by: vec![(2, Direction::Desc)],
                    limit: Some(2),
                    ..Default::default()
                },
            ),
            (
                "aggregate: totals (count distinct, approx count distinct, min, max, count of field)",
                AggregateQuery {
                    collection: "sales".into(),
                    aggregates: vec![
                        a(AggFunc::CountDistinct, f("region")),
                        a(AggFunc::ApproxCountDistinct, f("customer")),
                        a(AggFunc::Min, f("price")),
                        a(AggFunc::Max, f("price")),
                        a(AggFunc::Count, f("coupon")),
                    ],
                    ..Default::default()
                },
            ),
            (
                "aggregate: weekly buckets of day (width 7, offset 3) and region, sum(qty - 1) and sum(-price + 2.5), budgets",
                AggregateQuery {
                    collection: "sales".into(),
                    group_by: vec![GroupKey::Bucket { field: "day".into(), width: 7, offset: 3 }, GroupKey::Field("region".into())],
                    aggregates: vec![
                        a(AggFunc::Sum, Some(Expr::Sub(Box::new(Expr::Field("qty".into())), Box::new(Expr::Const(PropertyValue::Int64(1)))))),
                        a(AggFunc::Sum, Some(Expr::Add(Box::new(Expr::Neg(Box::new(Expr::Field("price".into())))), Box::new(Expr::Const(dec("2.5")))))),
                    ],
                    order_by: vec![(0, Direction::Asc), (1, Direction::Desc)],
                    memory_budget: 1 << 20,
                    spill_budget: 1 << 30,
                    ..Default::default()
                },
            ),
        ];
        for (name, q) in cases {
            let mut b = Vec::new();
            encode_aggregate(&q, &mut b);
            reqs.push((name.into(), 0x29, b));
        }
        let scans = vec![
            (
                "scan: qty, price where region = eu, limit 500",
                ScanQuery { collection: "sales".into(), filter: Some(Filter::Eq("region".into(), PropertyValue::Text("eu".into()))), fields: vec!["qty".into(), "price".into()], after_key: None, limit: 500 },
            ),
            ("scan: meta.size after s0099 (default limit)", ScanQuery { collection: "sales".into(), filter: None, fields: vec!["meta.size".into()], after_key: Some("s0099".into()), limit: 0 }),
        ];
        for (name, q) in scans {
            let mut b = Vec::new();
            encode_scan(&q, &mut b);
            reqs.push((name.into(), 0x2A, b));
        }
        for (cmd, name) in [(10u8, "admin: enable columnar sales"), (11u8, "admin: disable columnar sales")] {
            let mut b = vec![cmd];
            storage_core::document_wire::put_str(&mut b, "sales");
            reqs.push((name.into(), 0x23, b));
        }
    }
    let n = reqs.len();
    for (i, (name, op, body)) in reqs.iter().enumerate() {
        let _ = writeln!(out, "    {{\"name\":{},\"opcode\":{},\"body_hex\":\"{}\"}}{}", esc(name), op, hex(body), if i + 1 < n { "," } else { "" });
    }
    out.push_str("  ],\n  \"responses\": [\n");
    let mut wr = Vec::new();
    encode_write_response(&[Some(Version(12)), None], false, &mut wr);
    let page = DocPage {
        documents: vec![Document { key: "k1".into(), version: Version(3), fields: doc(&[("n", PropertyValue::Int64(1))]) }],
        total: 42,
        next_after_key: Some("k1".into()),
    };
    let mut pb = Vec::new();
    encode_page(&page, &mut pb);
    let _ = writeln!(out, "    {{\"name\":\"write ok: [v12, deleted]\",\"opcode\":33,\"body_hex\":\"{}\",\"versions\":[\"12\",null],\"duplicate\":false}},", hex(&wr));
    let _ = writeln!(out, "    {{\"name\":\"page: total 42, next k1, one doc\",\"opcode\":34,\"body_hex\":\"{}\"}},", hex(&pb));
    let mut rb = Vec::new();
    rb.extend_from_slice(&5000u64.to_le_bytes());
    rb.extend_from_slice(&4u32.to_le_bytes());
    rb.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
    let _ = writeln!(out, "    {{\"name\":\"byte range: total 5000, 4 bytes\",\"opcode\":32,\"body_hex\":\"{}\"}},", hex(&rb));
    let agg = storage_core::analytics::AggregateResult {
        rows: vec![
            vec![PropertyValue::Text("eu".into()), PropertyValue::Int64(3), dec("12.50")],
            vec![PropertyValue::Text("us".into()), PropertyValue::Int64(1), PropertyValue::Null],
        ],
        matched: 4,
        pruned_row_groups: 2,
        spilled_bytes: 0,
    };
    let mut ab = Vec::new();
    storage_core::document_wire::encode_aggregate_result(&agg, &mut ab);
    let _ = writeln!(out, "    {{\"name\":\"aggregate: matched 4, pruned 2, rows [eu, 3, 12.50] [us, 1, null]\",\"opcode\":41,\"body_hex\":\"{}\"}},", hex(&ab));
    let scan = storage_core::analytics::ScanPage { rows: vec![("s1".into(), vec![PropertyValue::Int64(7), dec("9.99")])], total: 2, next_after_key: Some("s1".into()) };
    let mut sb = Vec::new();
    storage_core::document_wire::encode_scan_page(&scan, 2, &mut sb);
    let _ = writeln!(out, "    {{\"name\":\"scan page: total 2, next s1, row s1 [7, 9.99]\",\"opcode\":42,\"body_hex\":\"{}\"}}", hex(&sb));
    out.push_str("  ]\n}\n");
    out
}

#[test]
fn test_protocol_vectors_are_pinned() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../protocol/vectors/vectors.json");
    let generated = build();
    if std::env::var("STORINE_UPDATE_VECTORS").is_ok() || !path.exists() {
        std::fs::write(&path, &generated).unwrap();
        return;
    }
    let existing = std::fs::read_to_string(&path).unwrap();
    assert_eq!(existing, generated, "protocol vectors changed: rerun with STORINE_UPDATE_VECTORS=1 if intentional");
}
