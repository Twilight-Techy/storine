//! Structure-aware fuzzing of the document server over TCP (`F-SIM-04`, `TASK-135`).
//!
//! Every request in the conformance vectors (`protocol/vectors/vectors.json`) is sent as is
//! and mutated: byte flips, truncations, splices, and edge values in length and count fields.
//! The opcode is occasionally swapped too, so each body reaches other handlers. The server must
//! answer or close the connection, never crash a worker. Every few hundred requests, a fresh
//! connection must still write and read a document correctly.
//!
//! `STORINE_FUZZ_ITERS` scales the request count.

use std::collections::BTreeMap;
use std::sync::Arc;

use storage_codec::PropertyValue;
use storage_core::{DocOp, DocWrite, DocumentStore, EngineConfig, Precondition};
use storage_server::{SecurityConfig, StorageClient, StorageServer};
use storage_types::testing::{fuzz_scale, SweepRng};

fn hex(s: &str) -> Vec<u8> {
    (0..s.len() / 2).filter_map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok()).collect()
}

fn request_vectors() -> Vec<(u8, Vec<u8>)> {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../protocol/vectors/vectors.json");
    let text = std::fs::read_to_string(path).unwrap();
    let start = text.find("\"requests\"").unwrap();
    let end = text[start..].find("\"responses\"").map_or(text.len(), |e| start + e);
    let mut out = Vec::new();
    for obj in text[start..end].split("{\"name\"").skip(1) {
        let op = obj.find("\"opcode\":").and_then(|i| obj[i + 9..].trim_start().split(|c: char| !c.is_ascii_digit()).next()?.parse().ok());
        let body = obj.find("\"body_hex\":\"").and_then(|i| obj[i + 12..].split('"').next().map(hex));
        if let (Some(op), Some(body)) = (op, body) {
            out.push((op, body));
        }
    }
    assert!(out.len() >= 30, "request vectors not parsed");
    out
}

fn mutate(rng: &mut SweepRng, b: &[u8], corpus: &[(u8, Vec<u8>)]) -> Vec<u8> {
    let mut out = b.to_vec();
    match rng.below(6) {
        0 => out = rng.hostile_copy(&out),
        1 if !out.is_empty() => out.truncate(rng.below(out.len())),
        2 if out.len() >= 4 => {
            let at = rng.below(out.len() - 3);
            let cur = u32::from_le_bytes(out[at..at + 4].try_into().unwrap());
            out[at..at + 4].copy_from_slice(&rng.edge_u32(cur).to_le_bytes());
        }
        3 => {
            let other = &corpus[rng.below(corpus.len())].1;
            out.truncate(rng.below(out.len() + 1));
            out.extend_from_slice(&other[rng.below(other.len() + 1)..]);
        }
        4 if !out.is_empty() => {
            let at = rng.below(out.len());
            out[at] ^= 1 << rng.below(8);
        }
        _ => {}
    }
    out
}

#[test]
fn mutated_document_requests_never_crash_the_server() {
    let dir = std::env::temp_dir().join(format!("stge_fuzz_requests_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let store = Arc::new(DocumentStore::open(&dir, EngineConfig::default()).unwrap());
    let mut server = StorageServer::start_documents("127.0.0.1:0", Arc::clone(&store), 1000, SecurityConfig::default(), None).unwrap();
    let addr = server.local_addr();
    let corpus = request_vectors();
    let mut rng = SweepRng::new(0x5E2);
    let mut client = StorageClient::connect(addr).unwrap();
    let (mut answered, mut closed) = (0usize, 0usize);
    let total = 4_000 * fuzz_scale();
    for i in 0..total {
        let (op, base) = &corpus[rng.below(corpus.len())];
        let op = match rng.below(20) {
            0 => corpus[rng.below(corpus.len())].0,
            1 => rng.next_u64() as u8,
            _ => *op,
        };
        let body = if i < corpus.len() { base.clone() } else { mutate(&mut rng, base, &corpus) };
        match client.doc_command(op, &body) {
            Ok(_) => answered += 1,
            Err(_) => {
                // Errors may close the connection: reconnect and continue.
                closed += 1;
                client = StorageClient::connect(addr).unwrap();
            }
        }
        if i % 500 == 499 || i + 1 == total {
            let mut c = StorageClient::connect(addr).unwrap();
            let mut f = BTreeMap::new();
            f.insert("i".to_string(), PropertyValue::Int64(i as i64));
            let w = DocWrite { collection: "health".into(), key: "k".into(), op: DocOp::Put(f), precondition: Precondition::None };
            c.doc_write(&[w], None).expect("server still writes after hostile requests");
            let doc = c.doc_get("health", "k").unwrap().expect("and reads");
            assert_eq!(doc.fields.get("i"), Some(&PropertyValue::Int64(i as i64)));
        }
    }
    eprintln!("fuzzed {total} requests: {answered} answered, {closed} refused or closed");
    server.stop();
    let _ = std::fs::remove_dir_all(&dir);
}
