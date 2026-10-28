//! Conformance: the SDK reproduces the server's encodings byte for byte (protocol/vectors).

use std::collections::BTreeMap;
use std::time::Duration;

use crate::value::{decode, encode, encode_fields, Decimal, Doc, Value};
use crate::wire::*;
use crate::{dec, doc, Error};

const VECTORS: &str = include_str!("../../../protocol/vectors/vectors.json");

/// A tiny JSON reader for the vectors file.
#[derive(Debug, Clone)]
enum Json {
    Null,
    Bool(bool),
    Num,
    Str(String),
    Arr(Vec<Json>),
    Obj(BTreeMap<String, Json>),
}

impl Json {
    fn parse(s: &str) -> Json {
        let mut p = (s.as_bytes(), 0usize);
        let v = value(&mut p);
        return v;

        fn ws(p: &mut (&[u8], usize)) {
            while p.1 < p.0.len() && p.0[p.1].is_ascii_whitespace() {
                p.1 += 1;
            }
        }

        fn value(p: &mut (&[u8], usize)) -> Json {
            ws(p);
            match p.0[p.1] {
                b'{' => {
                    p.1 += 1;
                    let mut m = BTreeMap::new();
                    loop {
                        ws(p);
                        if p.0[p.1] == b'}' {
                            p.1 += 1;
                            return Json::Obj(m);
                        }
                        let Json::Str(k) = value(p) else { panic!("object key") };
                        ws(p);
                        p.1 += 1; // :
                        m.insert(k, value(p));
                        ws(p);
                        if p.0[p.1] == b',' {
                            p.1 += 1;
                        }
                    }
                }
                b'[' => {
                    p.1 += 1;
                    let mut a = Vec::new();
                    loop {
                        ws(p);
                        if p.0[p.1] == b']' {
                            p.1 += 1;
                            return Json::Arr(a);
                        }
                        a.push(value(p));
                        ws(p);
                        if p.0[p.1] == b',' {
                            p.1 += 1;
                        }
                    }
                }
                b'"' => {
                    p.1 += 1;
                    let mut out = String::new();
                    loop {
                        let c = p.0[p.1];
                        p.1 += 1;
                        match c {
                            b'"' => return Json::Str(out),
                            b'\\' => {
                                let e = p.0[p.1];
                                p.1 += 1;
                                match e {
                                    b'n' => out.push('\n'),
                                    b't' => out.push('\t'),
                                    b'u' => {
                                        let hex = std::str::from_utf8(&p.0[p.1..p.1 + 4]).unwrap();
                                        out.push(char::from_u32(u32::from_str_radix(hex, 16).unwrap()).unwrap());
                                        p.1 += 4;
                                    }
                                    other => out.push(other as char),
                                }
                            }
                            _ => {
                                // Copy one UTF-8 sequence.
                                let start = p.1 - 1;
                                let len = match c {
                                    0x00..=0x7F => 1,
                                    0xC0..=0xDF => 2,
                                    0xE0..=0xEF => 3,
                                    _ => 4,
                                };
                                out.push_str(std::str::from_utf8(&p.0[start..start + len]).unwrap());
                                p.1 = start + len;
                            }
                        }
                    }
                }
                b't' => {
                    p.1 += 4;
                    Json::Bool(true)
                }
                b'f' => {
                    p.1 += 5;
                    Json::Bool(false)
                }
                b'n' => {
                    p.1 += 4;
                    Json::Null
                }
                _ => {
                    let start = p.1;
                    while p.1 < p.0.len() && (p.0[p.1] == b'-' || p.0[p.1].is_ascii_digit()) {
                        p.1 += 1;
                    }
                    assert!(p.1 > start, "bad JSON");
                    Json::Num
                }
            }
        }
    }

    fn get(&self, k: &str) -> &Json {
        match self {
            Json::Obj(m) => m.get(k).unwrap_or(&Json::Null),
            _ => &Json::Null,
        }
    }

    fn str(&self) -> &str {
        match self {
            Json::Str(s) => s,
            other => panic!("not a string: {other:?}"),
        }
    }

    fn arr(&self) -> &[Json] {
        match self {
            Json::Arr(a) => a,
            other => panic!("not an array: {other:?}"),
        }
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
}

fn native(d: &Json) -> Value {
    let v = d.get("value");
    match d.get("type").str() {
        "null" => Value::Null,
        "bool" => Value::Bool(matches!(v, Json::Bool(true))),
        "int64" => Value::Int(v.str().parse().unwrap()),
        "uint64" => Value::UInt(v.str().parse().unwrap()),
        "float64" => Value::Float(f64::from_bits(u64::from_str_radix(d.get("bits").str(), 16).unwrap())),
        "int128" => Value::Int128(v.str().parse().unwrap()),
        "timestamp" => Value::Timestamp(v.str().parse().unwrap()),
        "uuid" => Value::Uuid(unhex(v.str()).try_into().unwrap()),
        "id" => Value::Id(unhex(v.str()).try_into().unwrap()),
        "text" => Value::Text(v.str().to_owned()),
        "bytes" => Value::Bytes(unhex(v.str())),
        "decimal" => Value::Decimal(dec(v.str())),
        "list" => Value::List(v.arr().iter().map(native).collect()),
        "map" => Value::Map(v.arr().iter().map(|p| (p.arr()[0].str().to_owned(), native(&p.arr()[1]))).collect()),
        t => panic!("unknown vector type {t}"),
    }
}

fn vectors() -> Json {
    Json::parse(VECTORS)
}

#[test]
fn values_encode_and_decode() {
    for vec in vectors().get("values").arr() {
        let name = vec.get("name").str();
        let v = native(vec.get("value"));
        assert_eq!(hex(&encode(&v).unwrap()), vec.get("hex").str(), "{name}");
        let back = decode(&unhex(vec.get("hex").str())).unwrap();
        // Floats compare by bits (negative zero).
        assert_eq!(format!("{back:?}"), format!("{v:?}"), "{name}");
    }
}

#[test]
fn document_body() {
    let v = vectors();
    let d = &v.get("documents").arr()[0];
    let Value::Map(fields) = native(d.get("fields")) else { panic!() };
    let mut out = Vec::new();
    encode_fields(&fields, &mut out, 1).unwrap();
    assert_eq!(hex(&out), d.get("hex").str());
}

#[test]
fn requests() {
    let d = doc! { "balance" => dec("100.00"), "owner" => "alice", "active" => true };
    let rid: [u8; 16] = unhex("0102030405060708090a0b0c0d0e0f10").try_into().unwrap();
    let ab = [0xabu8; 16];
    let ws = |id: Option<&[u8; 16]>, w: Vec<Write>| encode_writes(&w, id).unwrap();
    let q = |q: Query, after: Option<&str>| encode_query("items", &q, after, None).unwrap();
    let ttl = u64::try_from(Duration::from_millis(1500).as_millis()).unwrap();
    let mut built: BTreeMap<&str, Vec<u8>> = BTreeMap::new();
    built.insert("get accounts/alice", encode_get("accounts", "alice").unwrap());
    built.insert(
        "get accounts/alice: bytes 1000..1004 of photo",
        encode_get_range("accounts", "alice", "photo", 1000, 4).unwrap(),
    );
    built.insert(
        "get accounts/alice: fields owner, meta.size",
        encode_get_fields("accounts", "alice", &["owner", "meta.size"]).unwrap(),
    );
    built.insert("write: put", ws(None, vec![Write::put("accounts", "alice", d.clone(), 0, 0, 0)]));
    built.insert(
        "write: insert (must not exist) with request id",
        ws(Some(&ab), vec![Write::put("accounts", "alice", d.clone(), 0, 1, 0)]),
    );
    built.insert("write: replace if version 7", ws(None, vec![Write::put("accounts", "alice", d.clone(), 0, 3, 7)]));
    built.insert(
        "write: put with ttl 1500 ms",
        ws(None, vec![Write::put("sessions", "s1", doc! {"u" => 1}, ttl, 0, 0)]),
    );
    built.insert("write: delete", ws(None, vec![Write::new("accounts", "alice", 3, 0, 0)]));
    built.insert("write: delete if exists", ws(None, vec![Write::new("accounts", "alice", 3, 2, 0)]));
    built.insert(
        "write: transfer 25.00 (two patches, no overdraft) with request id",
        ws(
            Some(&rid),
            vec![
                Write::patch("accounts", "alice", 2, 0, vec![add("balance", dec("-25.00")).min(0)]),
                Write::patch("accounts", "bob", 2, 0, vec![add("balance", dec("25.00"))]),
            ],
        ),
    );
    built.insert(
        "write: patch set / unset / append / add with min and max",
        ws(
            None,
            vec![Write::patch(
                "c",
                "k",
                2,
                0,
                vec![set("profile.name", "Ann"), unset("tmp"), append("log", 3), add("n", 1).min(0).max(10)],
            )],
        ),
    );
    built.insert("write: check version 3", ws(None, vec![Write::new("c", "k", 5, 3, 3)]));
    built.insert("query: all (default limit)", q(Query::new(), None));
    built.insert("query: eq text", q(Query::new().filter(eq("kind", "book")).limit(10), None));
    built.insert(
        "query: and(gte, lt) sorted desc, offset 20",
        q(
            Query::new()
                .filter(ge("price", dec("10")) & lt("price", dec("20.50")))
                .order_by("-price")
                .limit(10)
                .offset(20),
            None,
        ),
    );
    built.insert("query: between", q(between("n", 1, 5).into(), None));
    built.insert("query: in", q(is_in("n", [1, 2]).into(), None));
    built.insert(
        "query: prefix / contains / exists / missing",
        q(or([prefix("name", "An"), contains("tags", "x"), exists("email"), missing("deleted_at")]).into(), None),
    );
    built.insert(
        "query: not / ne / le / gt / key prefix",
        q(and([!ne("a", 1), le("b", 2.5), gt("c", 0), key_prefix("user:")]).into(), None),
    );
    built.insert("query: after key cursor", q(Query::new().limit(100), Some("k0099")));
    built.insert("query: multi-key sort", q(Query::new().order_by("a").order_by("-b").limit(5), None));
    built.insert(
        "query: nearest neighbours with a filter",
        encode_query(
            "items",
            &Query::new().filter(eq("kind", "book")).limit(5),
            None,
            Some(Nearest { field: "embedding", vector: &[0.25, -1.5, 3.0], k: 5, exact: false }),
        )
        .unwrap(),
    );
    built.insert(
        "query: nearest neighbours, exact",
        encode_query(
            "items",
            &Query::new(),
            None,
            Some(Nearest { field: "v", vector: &[1.0, 0.0], k: 1, exact: true }),
        )
        .unwrap(),
    );
    built.insert("admin: create index items.price", encode_admin(1, "items", Some("price")).unwrap());
    built.insert("info: collection items", encode_admin(6, "items", None).unwrap());
    built.insert("info: list collections", vec![3]);
    let v = vectors();
    let reqs = v.get("requests").arr();
    for vec in reqs {
        let name = vec.get("name").str();
        let b = built.get(name).unwrap_or_else(|| panic!("no builder for {name}"));
        assert_eq!(hex(b), vec.get("body_hex").str(), "{name}");
    }
    assert_eq!(reqs.len(), built.len());
}

#[test]
fn responses() {
    let v = vectors();
    let r: BTreeMap<&str, Vec<u8>> =
        v.get("responses").arr().iter().map(|x| (x.get("name").str(), unhex(x.get("body_hex").str()))).collect();
    assert_eq!(decode_write_response(&r["write ok: [v12, deleted]"]).unwrap(), vec![Some(12), None]);
    let p = decode_page(&r["page: total 42, next k1, one doc"]).unwrap();
    assert_eq!((p.total, p.next_key.as_deref()), (42, Some("k1")));
    assert_eq!(p.docs, vec![Document { key: "k1".into(), version: 3, fields: doc! {"n" => 1} }]);
    let (total, data) = decode_byte_range(&r["byte range: total 5000, 4 bytes"]).unwrap();
    assert_eq!((total, data), (5000, vec![0xde, 0xad, 0xbe, 0xef]));
    assert!(matches!(decode_byte_range(&r["byte range: total 5000, 4 bytes"][..15]), Err(Error::Invalid(_))));
}

#[test]
fn hostile_bytes_never_panic() {
    let v = vectors();
    let values = v.get("values").arr();
    let good = unhex(values[values.len() - 1].get("hex").str());
    for cut in 0..good.len() {
        assert!(matches!(decode(&good[..cut]), Err(Error::Invalid(_))), "cut {cut}");
    }
    assert!(matches!(decode(&[0x0C, 0xFF, 0xFF, 0xFF, 0xFF]), Err(Error::Invalid(_))));
}

#[test]
fn value_mapping_rules() {
    for bad in ["NaN", "1e", "", "abc", "1.2.3", "1E-40", "--1"] {
        assert!(Decimal::parse(bad).is_err(), "{bad:?} must not parse");
    }
    assert_eq!(dec("1E+3").to_string(), "1000");
    for s in ["1234567890123456789012345678.0123456789", "-0.001", "0.00", "19.99"] {
        let back = decode(&encode(&Value::Decimal(dec(s))).unwrap()).unwrap();
        assert_eq!(back.as_decimal().unwrap().to_string(), s);
    }
    assert_eq!(Value::from(1i128 << 100), Value::Int128(1 << 100));
    assert_eq!(Value::from(5i128), Value::Int(5));
    let empty: Doc = doc! {};
    assert!(empty.is_empty());
}
