//! Footprint and decode-speed probe on a TPC-H `lineitem`-shaped table (run with
//! `cargo test --release -p storage-columnar --test footprint -- --ignored --nocapture`).

use std::collections::BTreeMap;
use std::time::Instant;

use storage_codec::sealed_file::FileSeal;
use storage_codec::types::PropertyValue as P;
use storage_columnar::{flatten_owned, Body, Chunk, Segment, SegmentRow, SegmentWriter};
use storage_types::Decimal;

fn lineitem(i: u64) -> BTreeMap<String, P> {
    let mut x = i.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
    let mut rnd = move |m: u64| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x % m
    };
    let mut m = BTreeMap::new();
    m.insert("l_orderkey".into(), P::Int64((i / 4) as i64 + 1));
    m.insert("l_partkey".into(), P::Int64(rnd(200_000) as i64 + 1));
    m.insert("l_suppkey".into(), P::Int64(rnd(10_000) as i64 + 1));
    m.insert("l_linenumber".into(), P::Int64((i % 4) as i64 + 1));
    let qty = rnd(50) + 1;
    m.insert("l_quantity".into(), P::Decimal(Decimal::new(i128::from(qty) * 100, 2).unwrap()));
    m.insert("l_extendedprice".into(), P::Decimal(Decimal::new(i128::from(qty * (90_000 + rnd(10_000_000))), 2).unwrap()));
    m.insert("l_discount".into(), P::Decimal(Decimal::new(i128::from(rnd(11)), 2).unwrap()));
    m.insert("l_tax".into(), P::Decimal(Decimal::new(i128::from(rnd(9)), 2).unwrap()));
    m.insert("l_returnflag".into(), P::Text(["A", "N", "R"][rnd(3) as usize].into()));
    m.insert("l_linestatus".into(), P::Text(["F", "O"][rnd(2) as usize].into()));
    let day = 8035 + rnd(2526) as i64; // 1992-01-01 .. 1998-12-01
    m.insert("l_shipdate".into(), P::TimestampNs(day * 86_400_000_000_000));
    m.insert("l_commitdate".into(), P::TimestampNs((day + rnd(60) as i64 - 30) * 86_400_000_000_000));
    m.insert("l_receiptdate".into(), P::TimestampNs((day + rnd(30) as i64 + 1) * 86_400_000_000_000));
    m.insert("l_shipinstruct".into(), P::Text(["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"][rnd(4) as usize].into()));
    m.insert("l_shipmode".into(), P::Text(["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"][rnd(7) as usize].into()));
    let words = ["carefully", "final", "deposits", "sleep", "furiously", "regular", "accounts", "ironic", "packages", "bold"];
    let comment: Vec<&str> = (0..3 + rnd(4)).map(|_| words[rnd(words.len() as u64) as usize]).collect();
    m.insert("l_comment".into(), P::Text(comment.join(" ")));
    m
}

#[test]
#[ignore]
fn lineitem_footprint_and_decode_speed() {
    let n = 1_000_000u64;
    let dir = std::env::temp_dir().join(format!("stge_col_fp_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("lineitem.col");
    let mut row_bytes = 0u64;
    let t = Instant::now();
    let mut w = SegmentWriter::create(&path, FileSeal::PLAIN, b"li", storage_columnar::ROW_GROUP_ROWS, 1).unwrap();
    let mut buf = Vec::new();
    for i in 0..n {
        let doc = lineitem(i);
        buf.clear();
        storage_codec::value_wire::encode_map_body(&doc, &mut buf);
        row_bytes += buf.len() as u64;
        let mut cells = Vec::new();
        flatten_owned(&doc, &mut cells);
        let key = format!("{:010}", i);
        w.push(SegmentRow { id_hash: i.wrapping_mul(0x9E37_79B9_7F4A_7C15), key: &key, version: 1, cells: &cells }).unwrap();
    }
    let s = w.finish().unwrap();
    let build = t.elapsed();
    let seg = Segment::open(&path, &FileSeal::PLAIN, b"li").unwrap();
    println!("rows {n}: row-store field bytes {row_bytes} ({:.1} B/row), segment {} bytes ({:.1} B/row, incl. keys + versions), {:.1}x smaller; build {:?}",
        row_bytes as f64 / n as f64, s.bytes, s.bytes as f64 / n as f64, row_bytes as f64 / s.bytes as f64, build);
    for name in ["l_quantity", "l_extendedprice", "l_discount", "l_shipdate", "l_returnflag", "l_comment", "l_orderkey"] {
        let c = seg.column_index(name).unwrap();
        let bytes: u64 = seg.row_groups().iter().map(|g| u64::from(g.chunks[c].len)).sum();
        println!("  {name:16} {:6.2} B/row", bytes as f64 / n as f64);
    }
    // Decode speed of an integer-domain column (shipdate) into i64 batches.
    let c = seg.column_index("l_shipdate").unwrap();
    let t = Instant::now();
    let mut sum = 0i64;
    let mut out = vec![0i64; 2048];
    for g in 0..seg.row_groups().len() {
        let rows = seg.row_groups()[g].rows as usize;
        let bytes = seg.chunk_bytes(g, c, &FileSeal::PLAIN).unwrap().unwrap();
        let chunk = Chunk::parse(&bytes, rows).unwrap();
        let Body::Int(_, ints) = &chunk.body else { panic!("not an int chunk") };
        let mut i = 0;
        while i < rows {
            let k = (rows - i).min(2048);
            ints.decode_into(i, &mut out[..k]);
            sum = sum.wrapping_add(out[..k].iter().fold(0i64, |a, &b| a.wrapping_add(b)));
            i += k;
        }
    }
    let el = t.elapsed();
    println!("decode+sum l_shipdate: {:?} ({:.2} ns/value, {:.0} M values/s) [{sum}]", el, el.as_nanos() as f64 / n as f64, n as f64 / el.as_secs_f64() / 1e6);
    let _ = std::fs::remove_dir_all(&dir);
}
