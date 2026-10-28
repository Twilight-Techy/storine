# Storine SDKs

Official clients of the Storine document API. Every SDK:

- has **no runtime dependencies** (standard library only);
- speaks the same wire protocol ([`protocol/PROTOCOL.md`](../protocol/PROTOCOL.md)) and passes the same
  byte-for-byte conformance vectors ([`protocol/vectors/vectors.json`](../protocol/vectors/vectors.json));
- uses the same vocabulary (`RULE-DX-01` – `RULE-DX-05`): `connect`, `collection`, `get` / `put` / `insert` / `replace` /
  `update` / `delete`, `find` / `first` / `count` / `scan`, `transaction`, filters `eq` … `key_prefix`, field operations
  `set` / `unset` / `add(...).min(...)` / `append`;
- maps money to the language's exact decimal type and never to a binary float;
- retries writes with the same request id (applied at most once), follows cluster leader redirects and fails over
  between seeds;
- is tested end to end against a real `storage-server`, single node and a 3-node cluster with the leader killed.

| Language | Directory | Money type | Tests |
| --- | --- | --- | --- |
| Python 3.8+ | [`python/`](python) | `decimal.Decimal` | `python3 -m unittest discover -s tests` |
| TypeScript / Node.js 18+ | [`typescript/`](typescript) | `Decimal` (exact, `bigint` mantissa) | `npm test` |
| Go 1.21+ | [`go/`](go) | `storine.Decimal` | `go test -race .` |
| Java 17+ | [`java/`](java) | `java.math.BigDecimal` | `mvn test` |
| C# / .NET 8+ | [`csharp/`](csharp) | `decimal` (or `BigDecimal` beyond 28 digits) | `dotnet test` |
| Rust 1.70+ | [`rust/`](rust) | `storine::Decimal` | `cargo test` |
| PHP 8.1+ | [`php/`](php) | `Storine\Decimal` (exact, keeps its scale) | `php tests/harness.php` |
| Ruby 3.2+ | [`ruby/`](ruby) | `Storine::Decimal` (exact, keeps its scale; converts to `BigDecimal`) | `ruby -Ilib test/test_vectors.rb && ruby -Ilib test/test_e2e.rb` |

The end-to-end tests use `target/release/storage-server` (build it with `cargo build --release -p storage-server`)
or the binary named by `$STORINE_SERVER`; without it they are skipped.

Adding an SDK: implement §3 (values) and §5 – §6 (requests / responses) of the protocol, make every vector in
`vectors.json` pass, then port the end-to-end suite of an existing SDK.
