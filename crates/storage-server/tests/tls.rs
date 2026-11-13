//! TLS on the native port: documents written and read over TLS from several clients at once,
//! values larger than one TLS record, a plaintext client refused, a client trusting another CA
//! refused, and authentication tokens carried inside the encrypted channel.

use std::collections::BTreeMap;
use std::sync::Arc;

use storage_codec::PropertyValue;
use storage_core::{DocOp, DocWrite, DocumentStore, EngineConfig, Precondition};
use storage_server::{ClientTls, RbacRole, SecurityConfig, StorageClient, StorageServer};
use storage_tls::{issue, new_ca, ClientConfig, TlsConfig};

fn doc(key: &str, text: String) -> DocWrite {
    let mut f = BTreeMap::new();
    f.insert("v".to_string(), PropertyValue::Text(text));
    DocWrite { collection: "c".into(), key: key.into(), op: DocOp::Put(f), precondition: Precondition::None }
}

#[test]
fn documents_over_tls_and_refusals() {
    let dir = std::env::temp_dir().join(format!("stge_tls_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let ca = new_ca("test cluster").unwrap();
    let node = issue(&ca, &["127.0.0.1", "node1"]).unwrap();
    let security = SecurityConfig::new(true)
        .with_token("secret-admin", RbacRole::Admin)
        .with_tls(TlsConfig::from_pem(&node.cert_pem, &node.key_pem).unwrap());
    let store = Arc::new(DocumentStore::open(&dir, EngineConfig::default()).unwrap());
    let mut server = StorageServer::start_documents("127.0.0.1:0", Arc::clone(&store), 100, security, None).unwrap();
    let addr = server.local_addr();
    let tls = ClientTls { config: Arc::new(ClientConfig::with_roots_pem(&ca.cert_pem).unwrap()), server_name: None };

    // Four clients at once, each writing and reading back values up to 100 KB (several records).
    let clients: Vec<_> = (0..4)
        .map(|t| {
            let tls = tls.clone();
            std::thread::spawn(move || {
                let mut c = StorageClient::connect_tls(addr, &tls).unwrap();
                c.authenticate("secret-admin").unwrap();
                for i in 0..40usize {
                    let key = format!("k{t}-{i}");
                    let text = "x".repeat(1 + (i * 2_531) % 100_000);
                    c.doc_write(&[doc(&key, text.clone())], None).unwrap();
                    let back = c.doc_get("c", &key).unwrap().unwrap();
                    assert_eq!(back.fields.get("v"), Some(&PropertyValue::Text(text)));
                }
            })
        })
        .collect();
    for c in clients {
        c.join().unwrap();
    }

    // The server name is checked: a certificate for 127.0.0.1 / node1 does not name node2.
    let wrong_name = ClientTls { server_name: Some("node2".into()), ..tls.clone() };
    assert!(StorageClient::connect_tls(addr, &wrong_name).is_err());
    let named = ClientTls { server_name: Some("node1".into()), ..tls.clone() };
    StorageClient::connect_tls(addr, &named).unwrap().authenticate("secret-admin").unwrap();

    // A client trusting another CA is refused.
    let other = ClientTls { config: Arc::new(ClientConfig::with_roots_pem(&new_ca("other").unwrap().cert_pem).unwrap()), server_name: None };
    assert!(StorageClient::connect_tls(addr, &other).is_err());

    // A plaintext client gets nowhere: its request is not a TLS handshake.
    let mut plain = StorageClient::connect_timeout(addr, std::time::Duration::from_secs(5)).unwrap();
    assert!(plain.authenticate("secret-admin").is_err());

    // The server still serves TLS clients after the refusals.
    let mut c = StorageClient::connect_tls(addr, &tls).unwrap();
    c.authenticate("secret-admin").unwrap();
    assert!(c.doc_get("c", "k0-1").unwrap().is_some());

    server.stop();
    drop(store);
    let _ = std::fs::remove_dir_all(&dir);
}
