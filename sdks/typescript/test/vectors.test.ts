// Conformance: the SDK reproduces the server's encodings byte for byte (protocol/vectors).
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";

import * as s from "../src";
import { decode, encode, encodeFields, Fields, Value, Writer } from "../src/values";
import * as w from "../src/wire";

const VECTORS = JSON.parse(readFileSync(join(__dirname, "..", "..", "..", "..", "protocol", "vectors", "vectors.json"), "utf8"));

const hex = (b: Uint8Array) => Buffer.from(b).toString("hex");
const unhex = (h: string) => new Uint8Array(Buffer.from(h, "hex"));

type Desc = { type: string; value?: any; bits?: string };

function native(d: Desc): Value {
  switch (d.type) {
    case "null": return null;
    case "bool": return d.value;
    case "int64": {
      const b = BigInt(d.value);
      return Number.isSafeInteger(Number(b)) && BigInt(Number(b)) === b ? Number(b) : b;
    }
    case "uint64": return new s.UInt64(BigInt(d.value));
    case "float64": {
      const v = new DataView(new ArrayBuffer(8));
      v.setBigUint64(0, BigInt("0x" + d.bits));
      return new s.Float(v.getFloat64(0));
    }
    case "int128": return BigInt(d.value);
    case "timestamp": return new s.Timestamp(BigInt(d.value));
    case "uuid": return new s.Uuid(d.value);
    case "id": return new s.Id(unhex(d.value));
    case "text": return d.value;
    case "bytes": return unhex(d.value);
    case "decimal": return new s.Decimal(d.value);
    case "list": return (d.value as Desc[]).map(native);
    case "map": {
      const o: Fields = {};
      for (const [k, v] of d.value as [string, Desc][]) o[k] = native(v);
      return o;
    }
  }
  throw new Error(d.type);
}

/** Normalizes decoded values for comparison (Float vs number, Timestamp vs Date, Decimal). */
function norm(v: Value): unknown {
  if (v instanceof s.Float) return ["f", Object.is(v.value, -0) ? "-0" : v.value];
  if (typeof v === "number" && !Number.isInteger(v)) return ["f", v];
  if (typeof v === "number" && Object.is(v, -0)) return ["f", "-0"];
  if (typeof v === "number") return ["i", BigInt(v)];
  if (typeof v === "bigint") return ["i", v];
  if (v instanceof s.UInt64) return ["i", v.value];
  if (v instanceof s.Timestamp) return ["t", v.ns];
  if (v instanceof Date) return ["t", BigInt(v.getTime()) * 1_000_000n];
  if (v instanceof s.Decimal) return ["d", v.toString()];
  if (v instanceof s.Uuid || v instanceof s.Id) return ["b", hex(v.bytes)];
  if (v instanceof Uint8Array) return ["b", hex(v)];
  if (Array.isArray(v)) return v.map(norm);
  if (v && typeof v === "object") return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, norm(x as Value)]));
  return v;
}

test("values encode and decode", () => {
  for (const vec of VECTORS.values) {
    const value = native(vec.value);
    assert.equal(hex(encode(value)), vec.hex, vec.name);
    assert.deepEqual(norm(decode(unhex(vec.hex))), norm(value), vec.name);
  }
});

test("document body", () => {
  const doc = VECTORS.documents[0];
  const wr = new Writer();
  encodeFields(native(doc.fields) as Fields, wr);
  assert.equal(hex(wr.finish()), doc.hex);
});

test("requests", () => {
  const d: Fields = { balance: new s.Decimal("100.00"), owner: "alice", active: true };
  const rid = unhex("0102030405060708090a0b0c0d0e0f10");
  const built: Record<string, Uint8Array> = {
    "get accounts/alice": w.encodeGet("accounts", "alice"),
    "get accounts/alice: bytes 1000..1004 of photo": w.encodeGetRange("accounts", "alice", "photo", 1000, 4),
    "get accounts/alice: fields owner, meta.size": w.encodeGetFields("accounts", "alice", ["owner", "meta.size"]),
    "write: put": w.encodeWrites([{ collection: "accounts", key: "alice", kind: 1, fields: d }]),
    "write: insert (must not exist) with request id": w.encodeWrites([{ collection: "accounts", key: "alice", kind: 1, precondition: 1, fields: d }], new Uint8Array(16).fill(0xab)),
    "write: replace if version 7": w.encodeWrites([{ collection: "accounts", key: "alice", kind: 1, precondition: 3, version: 7n, fields: d }]),
    "write: put with ttl 1500 ms": w.encodeWrites([{ collection: "sessions", key: "s1", kind: 2, ttlMs: 1500, fields: { u: 1 } }]),
    "write: delete": w.encodeWrites([{ collection: "accounts", key: "alice", kind: 3 }]),
    "write: delete if exists": w.encodeWrites([{ collection: "accounts", key: "alice", kind: 3, precondition: 2 }]),
    "write: transfer 25.00 (two patches, no overdraft) with request id": w.encodeWrites([
      { collection: "accounts", key: "alice", kind: 4, precondition: 2, ops: [s.add("balance", new s.Decimal("-25.00"), { min: 0 })] },
      { collection: "accounts", key: "bob", kind: 4, precondition: 2, ops: [s.add("balance", new s.Decimal("25.00"))] },
    ], rid),
    "write: patch set / unset / append / add with min and max": w.encodeWrites([{
      collection: "c", key: "k", kind: 4, precondition: 2,
      ops: [s.set("profile.name", "Ann"), s.unset("tmp"), s.append("log", 3), s.add("n", 1, { min: 0, max: 10 })],
    }]),
    "write: check version 3": w.encodeWrites([{ collection: "c", key: "k", kind: 5, precondition: 3, version: 3n }]),
    "query: all (default limit)": w.encodeQuery("items", undefined, [], 0, 0),
    "query: eq text": w.encodeQuery("items", s.eq("kind", "book"), [], 10, 0),
    "query: and(gte, lt) sorted desc, offset 20": w.encodeQuery("items", s.and(s.ge("price", new s.Decimal("10")), s.lt("price", new s.Decimal("20.50"))), w.parseOrderBy("-price"), 10, 20),
    "query: between": w.encodeQuery("items", s.between("n", 1, 5), [], 0, 0),
    "query: in": w.encodeQuery("items", s.isIn("n", [1, 2]), [], 0, 0),
    "query: prefix / contains / exists / missing": w.encodeQuery("items", s.or(s.prefix("name", "An"), s.contains("tags", "x"), s.exists("email"), s.missing("deleted_at")), [], 0, 0),
    "query: not / ne / le / gt / key prefix": w.encodeQuery("items", s.and(s.not(s.ne("a", 1)), s.le("b", 2.5), s.gt("c", 0), s.keyPrefix("user:")), [], 0, 0),
    "query: after key cursor": w.encodeQuery("items", undefined, [], 100, 0, "k0099"),
    "query: multi-key sort": w.encodeQuery("items", undefined, w.parseOrderBy(["a", "-b"]), 5, 0),
    "query: nearest neighbours with a filter": w.encodeQuery("items", s.eq("kind", "book"), [], 5, 0, undefined, { field: "embedding", vector: [0.25, -1.5, 3.0], k: 5, exact: false }),
    "query: nearest neighbours, exact": w.encodeQuery("items", undefined, [], 0, 0, undefined, { field: "v", vector: [1.0, 0.0], k: 1, exact: true }),
  };
  let seen = 0;
  for (const vec of VECTORS.requests) {
    if (built[vec.name]) {
      assert.equal(hex(built[vec.name]), vec.body_hex, vec.name);
      seen++;
    }
  }
  assert.equal(seen, Object.keys(built).length);
});

test("responses", () => {
  const r = Object.fromEntries(VECTORS.responses.map((v: any) => [v.name, v]));
  assert.deepEqual(w.decodeWriteResponse(unhex(r["write ok: [v12, deleted]"].body_hex)), { versions: [12n, null], duplicate: false });
  const page = w.decodePage(unhex(r["page: total 42, next k1, one doc"].body_hex));
  assert.deepEqual(page, { total: 42, next: "k1", docs: [{ key: "k1", version: 3n, fields: { n: 1 } }] });
  const range = w.decodeByteRange(unhex(r["byte range: total 5000, 4 bytes"].body_hex));
  assert.deepEqual(range, { total: 5000, data: unhex("deadbeef") });
});

test("hostile bytes never crash and __proto__ stays data", () => {
  const good = unhex(VECTORS.values[VECTORS.values.length - 1].hex);
  for (let cut = 0; cut < good.length; cut++) assert.throws(() => decode(good.subarray(0, cut)), s.InvalidError);
  const wr = new Writer();
  wr.u8(13);
  encodeFields(JSON.parse('{"__proto__": {"polluted": true}}'), wr);
  const out = decode(wr.finish()) as any;
  assert.equal(({} as any).polluted, undefined);
  assert.equal(Object.getPrototypeOf(out), Object.prototype);
  assert.deepEqual(Object.keys(out), ["__proto__"]);
});

test("value rules", () => {
  assert.throws(() => encode(2 ** 60), s.InvalidError, "unsafe integers must be bigint");
  assert.throws(() => new s.Decimal(0.1), s.InvalidError, "no float -> decimal conversion");
  assert.equal(new s.Decimal("1E+3").toString(), "1000");
  assert.equal(new s.Decimal("-0.001").toString(), "-0.001");
  assert.ok(new s.Decimal("1.50").equals(new s.Decimal("1.5")));
});
