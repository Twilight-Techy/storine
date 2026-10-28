# Test OpenID Connect issuer

`test-key.pem` is a throwaway RSA key that exists only for these tests; `jwks.json` is its
public half as an issuer would publish it (`kid` `storine-test`). `run_all.sh` starts a
storine-sql trusting it (`--oidc-issuer https://id.storine.test/realms/test --oidc-audience
storine --oidc-jwks oidc/jwks.json`), and each client suite signs its own tokens with the key,
using only its language's standard library. Never trust this key anywhere else.
