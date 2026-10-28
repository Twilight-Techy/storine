//! OpenID Connect access-token logins: verifies a JWT (RFC 7519) signed by the configured
//! issuer and maps one of its claims to the session user. Works with any standards-compliant
//! provider (Keycloak, Dex, Authentik, Zitadel, Auth0, Okta, Entra ID, ...).
//!
//! The issuer's signing keys come from a JWKS file (RFC 7517) — the document the provider
//! publishes at its `jwks_uri` — which is re-read whenever it changes, so key rotation needs
//! only a refreshed file. Accepted algorithms: RS256, RS384, RS512 and EdDSA (Ed25519).

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::exec::func::{parse_json, Json};
use crate::rsa::Hash;

/// Clock skew tolerated on `exp` / `nbf` (seconds).
const LEEWAY: f64 = 60.0;

/// Claims tried, in order, for the user name when none is configured.
pub const DEFAULT_USER_CLAIMS: [&str; 4] = ["preferred_username", "upn", "email", "sub"];

/// Token-login settings for one issuer.
#[derive(Debug)]
pub struct Oidc {
    /// Expected `iss` claim.
    pub issuer: String,
    /// Accepted `aud` values (at least one must appear in the token).
    pub audiences: Vec<String>,
    /// JWKS file with the issuer's public keys.
    pub jwks: PathBuf,
    /// Claims tried, in order, for the session user name.
    pub user_claims: Vec<String>,
    keys: Mutex<Option<(SystemTime, Arc<Vec<Key>>)>>,
}

#[derive(Debug)]
struct Key {
    kid: Option<String>,
    alg: Option<String>,
    material: Material,
}

#[derive(Debug)]
enum Material {
    Rsa { n: Vec<u8>, e: Vec<u8> },
    Ed25519([u8; 32]),
}

impl Oidc {
    /// Checks the configuration and loads the keys once.
    pub fn new(
        issuer: String,
        audiences: Vec<String>,
        jwks: PathBuf,
        user_claims: Vec<String>,
    ) -> Result<Oidc, String> {
        if issuer.is_empty() || audiences.is_empty() {
            return Err("OpenID Connect logins need --oidc-issuer and --oidc-audience".into());
        }
        let o = Oidc {
            issuer,
            audiences,
            jwks,
            user_claims: if user_claims.is_empty() {
                DEFAULT_USER_CLAIMS
                    .iter()
                    .map(|c| (*c).to_owned())
                    .collect()
            } else {
                user_claims
            },
            keys: Mutex::new(None),
        };
        let keys = o.keys()?;
        if keys.is_empty() {
            return Err(format!(
                "{}: no usable RSA or Ed25519 signing keys",
                o.jwks.display()
            ));
        }
        Ok(o)
    }

    /// The current keys, re-reading the JWKS file when its modification time changed.
    fn keys(&self) -> Result<Arc<Vec<Key>>, String> {
        let path = &self.jwks;
        let modified = std::fs::metadata(path)
            .and_then(|m| m.modified())
            .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        let mut cache = self.keys.lock().unwrap_or_else(|p| p.into_inner());
        if let Some((at, keys)) = cache.as_ref() {
            if *at == modified {
                return Ok(Arc::clone(keys));
            }
        }
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        let keys = Arc::new(parse_jwks(&text).map_err(|e| format!("{}: {e}", path.display()))?);
        *cache = Some((modified, Arc::clone(&keys)));
        Ok(keys)
    }

    /// Verifies `token` and returns the session user name, or why the token was rejected.
    pub fn verify(&self, token: &str) -> Result<String, String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0.0, |d| d.as_secs_f64());
        self.verify_at(token, now)
    }

    fn verify_at(&self, token: &str, now: f64) -> Result<String, String> {
        let mut parts = token.trim().split('.');
        let (Some(h64), Some(p64), Some(s64), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err("not a signed JWT (expected header.payload.signature)".into());
        };
        let header = json_object(h64).ok_or("malformed JWT header")?;
        let claims = json_object(p64).ok_or("malformed JWT payload")?;
        let sig = base64url(s64).ok_or("malformed JWT signature")?;
        if header.contains_key("crit") {
            return Err("JWT uses critical header extensions".into());
        }
        let alg = str_of(header.get("alg")).ok_or("JWT header has no alg")?;
        let kid = str_of(header.get("kid"));
        let signed = &token.trim().as_bytes()[..h64.len() + 1 + p64.len()];
        let keys = self.keys()?;
        let candidates = keys.iter().filter(|k| {
            kid.is_none_or(|id| k.kid.as_deref() == Some(id))
                && k.alg.as_deref().is_none_or(|a| a == alg)
        });
        let mut found = false;
        let mut verified = false;
        for k in candidates {
            let ok = match (&k.material, alg) {
                (Material::Rsa { n, e }, "RS256" | "RS384" | "RS512") => {
                    let hash = match alg {
                        "RS256" => Hash::Sha256,
                        "RS384" => Hash::Sha384,
                        _ => Hash::Sha512,
                    };
                    crate::rsa::verify_pkcs1_v15(n, e, hash, signed, &sig)
                }
                (Material::Ed25519(pk), "EdDSA") => <&[u8; 64]>::try_from(sig.as_slice())
                    .is_ok_and(|s| storage_codec::curve25519::ed25519_verify(pk, signed, s)),
                _ => continue,
            };
            found = true;
            if ok {
                verified = true;
                break;
            }
        }
        if !found {
            return Err(match kid {
                Some(id) => format!("no {alg} key with kid \"{id}\" in the issuer's key set"),
                None => format!("no {alg} key in the issuer's key set"),
            });
        }
        if !verified {
            return Err("JWT signature does not verify".into());
        }
        let iss = str_of(claims.get("iss")).unwrap_or("");
        if iss.trim_end_matches('/') != self.issuer.trim_end_matches('/') {
            return Err(format!("token issuer \"{iss}\" is not {}", self.issuer));
        }
        let audiences: Vec<&str> = match claims.get("aud") {
            Some(Json::Str(a)) => vec![a.as_str()],
            Some(Json::Arr(list)) => list.iter().filter_map(|a| str_of(Some(a))).collect(),
            _ => Vec::new(),
        };
        if !audiences
            .iter()
            .any(|a| self.audiences.iter().any(|want| want == a))
        {
            return Err(format!(
                "token audience {audiences:?} does not include {:?}",
                self.audiences
            ));
        }
        let exp = num_of(claims.get("exp")).ok_or("token has no exp claim")?;
        if now > exp + LEEWAY {
            return Err("token has expired".into());
        }
        if num_of(claims.get("nbf")).is_some_and(|nbf| now + LEEWAY < nbf) {
            return Err("token is not valid yet (nbf)".into());
        }
        self.user_claims
            .iter()
            .find_map(|c| str_of(claims.get(c)).filter(|v| !v.is_empty()))
            .map(str::to_owned)
            .ok_or_else(|| format!("token has none of the claims {:?}", self.user_claims))
    }
}

fn base64url(s: &str) -> Option<Vec<u8>> {
    if s.contains(['+', '/', '=']) {
        return None;
    }
    let mut t = s.replace('-', "+").replace('_', "/");
    while t.len() % 4 != 0 {
        t.push('=');
    }
    crate::hash::base64_decode(&t)
}

fn json_object(b64: &str) -> Option<std::collections::BTreeMap<String, Json>> {
    let bytes = base64url(b64)?;
    match parse_json(std::str::from_utf8(&bytes).ok()?).ok()? {
        Json::Obj(m) => Some(m),
        _ => None,
    }
}

fn str_of(v: Option<&Json>) -> Option<&str> {
    match v {
        Some(Json::Str(s)) => Some(s),
        _ => None,
    }
}

fn num_of(v: Option<&Json>) -> Option<f64> {
    match v {
        Some(Json::Num(n)) => n.parse().ok(),
        _ => None,
    }
}

/// The signing keys of a JWKS document (or of a single JWK); other keys are skipped.
fn parse_jwks(text: &str) -> Result<Vec<Key>, String> {
    let doc = parse_json(text).map_err(|_| "not valid JSON".to_owned())?;
    let list = match &doc {
        Json::Obj(m) => match m.get("keys") {
            Some(Json::Arr(keys)) => keys.iter().collect(),
            _ => vec![&doc],
        },
        _ => return Err("not a JSON Web Key Set".into()),
    };
    let mut out = Vec::new();
    for k in list {
        let Json::Obj(m) = k else { continue };
        if str_of(m.get("use")).is_some_and(|u| u != "sig") {
            continue;
        }
        let field = |name: &str| str_of(m.get(name)).and_then(base64url);
        let material = match (str_of(m.get("kty")), str_of(m.get("crv"))) {
            (Some("RSA"), _) => match (field("n"), field("e")) {
                (Some(n), Some(e)) => Material::Rsa { n, e },
                _ => continue,
            },
            (Some("OKP"), Some("Ed25519")) => match field("x").and_then(|x| x.try_into().ok()) {
                Some(x) => Material::Ed25519(x),
                None => continue,
            },
            _ => continue,
        };
        out.push(Key {
            kid: str_of(m.get("kid")).map(str::to_owned),
            alg: str_of(m.get("alg")).map(str::to_owned),
            material,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage_codec::curve25519::Ed25519Key;

    fn b64url_enc(b: &[u8]) -> String {
        crate::hash::base64_encode(b)
            .trim_end_matches('=')
            .replace('+', "-")
            .replace('/', "_")
    }

    fn setup(dir: &str) -> (Oidc, Ed25519Key) {
        let key = Ed25519Key::from_seed(&[7u8; 32]);
        let path = std::env::temp_dir().join(format!("storine-oidc-{dir}-{}", std::process::id()));
        let jwks = format!(
            r#"{{"keys":[{{"kty":"OKP","crv":"Ed25519","kid":"k1","use":"sig","x":"{}"}},
                {{"kty":"EC","crv":"P-256","kid":"skip","x":"","y":""}}]}}"#,
            b64url_enc(&key.public_key())
        );
        std::fs::write(&path, jwks).unwrap();
        let o = Oidc::new(
            "https://id.example/realms/r".into(),
            vec!["storine".into()],
            path,
            Vec::new(),
        )
        .unwrap();
        (o, key)
    }

    fn token(key: &Ed25519Key, header: &str, claims: &str) -> String {
        let signed = format!(
            "{}.{}",
            b64url_enc(header.as_bytes()),
            b64url_enc(claims.as_bytes())
        );
        let sig = key.sign(signed.as_bytes());
        format!("{signed}.{}", b64url_enc(&sig))
    }

    #[test]
    fn eddsa_tokens() {
        let (o, key) = setup("eddsa");
        let h = r#"{"alg":"EdDSA","kid":"k1","typ":"JWT"}"#;
        let good = r#"{"iss":"https://id.example/realms/r/","aud":["x","storine"],"exp":2000,"preferred_username":"ann","sub":"123"}"#;
        assert_eq!(o.verify_at(&token(&key, h, good), 1000.0), Ok("ann".into()));
        // Expired (beyond the leeway), not yet valid, wrong issuer / audience.
        assert!(o
            .verify_at(&token(&key, h, good), 2061.0)
            .unwrap_err()
            .contains("expired"));
        let nbf = r#"{"iss":"https://id.example/realms/r","aud":"storine","exp":9000,"nbf":5000,"sub":"s"}"#;
        assert!(o.verify_at(&token(&key, h, nbf), 1000.0).is_err());
        assert_eq!(o.verify_at(&token(&key, h, nbf), 4950.0), Ok("s".into()));
        let iss = r#"{"iss":"https://evil.example","aud":"storine","exp":9000,"sub":"s"}"#;
        assert!(o
            .verify_at(&token(&key, h, iss), 1000.0)
            .unwrap_err()
            .contains("issuer"));
        let aud = r#"{"iss":"https://id.example/realms/r","aud":"other","exp":9000,"sub":"s"}"#;
        assert!(o
            .verify_at(&token(&key, h, aud), 1000.0)
            .unwrap_err()
            .contains("audience"));
        // Another key's signature, a tampered payload, alg "none", an unknown kid.
        let other = Ed25519Key::from_seed(&[8u8; 32]);
        assert!(o
            .verify_at(&token(&other, h, good), 1000.0)
            .unwrap_err()
            .contains("signature"));
        let t = token(&key, h, good);
        let mut parts: Vec<&str> = t.split('.').collect();
        let forged = b64url_enc(good.replace("ann", "root").as_bytes());
        parts[1] = &forged;
        assert!(o.verify_at(&parts.join("."), 1000.0).is_err());
        let none = format!(
            "{}.{}.",
            b64url_enc(br#"{"alg":"none"}"#),
            b64url_enc(good.as_bytes())
        );
        assert!(o.verify_at(&none, 1000.0).is_err());
        let unknown = token(&key, r#"{"alg":"EdDSA","kid":"k9"}"#, good);
        assert!(o.verify_at(&unknown, 1000.0).unwrap_err().contains("kid"));
        assert!(o.verify_at("garbage", 1000.0).is_err());
    }

    #[test]
    fn rsa_key_from_jwks_and_rotation() {
        let (o, key) = setup("rotate");
        // RFC 7515 A.2's RSA key and example token (iss "joe", no aud): the signature checks
        // out, then the claims are refused.
        let n = "ofgWCuLjybRlzo0tZWJjNiuSfb4p4fAkd_wWJcyQoTbji9k0l8W26mPddxHmfHQp-Vaw-4qPCJrcS2mJPMEzP1Pt0Bm4d4QlL-yRT-SFd2lZS-pCgNMsD1W_YpRPEwOWvG6b32690r2jZ47soMZo9wGzjb_7OMg0LOL-bSf63kpaSHSXndS5z5rexMdbBYUsLA9e-KXBdQOS-UTo7WTBEMa2R2CapHg665xsmtdVMTBQY4uDZlxvb3qCo5ZwKh9kG4LT6_I5IhlJH7aGhyxXFvUK-DWNmoudF8NAco9_h9iaGNj8q2ethFkMLs91kzk2PAcDTW9gb54h4FRWyuXpoQ";
        let t = "eyJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJqb2UiLA0KICJleHAiOjEzMDA4MTkzODAsDQogImh0dHA6Ly9leGFtcGxlLmNvbS9pc19yb290Ijp0cnVlfQ.cC4hiUPoj9Eetdgtv3hF80EGrhuB__dzERat0XF9g2VtQgr9PJbu3XOiZj5RZmh7AAuHIm4Bh-0Qc_lF5YKt_O8W2Fp5jujGbds9uJdbF9CUAr7t1dnZcAcQjbKBYNX4BAynRFdiuB--f_nZLgrnbyTyWzO75vRK5h6xBArLIARNPvkSjtQBMHlb1L07Qe7K0GarZRmB_eSN9383LcOLn6_dO--xi12jzDwusC-eOkHWEsqtFZESc6BfI7noOPqvhJ1phCnvWh6IeYI2w9QOYEUipUTI8np6LbgGY9Fs98rqVt5AXLIhWkWywlVmtVrBp0igcN_IoypGlUPQGe77Rw";
        assert!(o.verify_at(t, 1000.0).unwrap_err().contains("no RS256 key"));
        // Rotate: the file now carries the RSA key; the next login re-reads it.
        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::write(
            &o.jwks,
            format!(r#"{{"keys":[{{"kty":"RSA","n":"{n}","e":"AQAB"}}]}}"#),
        )
        .unwrap();
        let filetime = std::fs::File::options().write(true).open(&o.jwks).unwrap();
        filetime
            .set_modified(SystemTime::now() + std::time::Duration::from_secs(5))
            .unwrap();
        assert!(o.verify_at(t, 1000.0).unwrap_err().contains("issuer"));
        let h = r#"{"alg":"EdDSA","kid":"k1"}"#;
        let c = r#"{"iss":"https://id.example/realms/r","aud":"storine","exp":9000,"sub":"s"}"#;
        assert!(o.verify_at(&token(&key, h, c), 1000.0).is_err());
        let _ = std::fs::remove_file(&o.jwks);
    }
}
