// End-to-end tests against a real storage-server (single node and a 3-node cluster).
import assert from "node:assert/strict";
import { ChildProcess, spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import * as net from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, test } from "node:test";

import * as s from "../src";

const SERVER = process.env.STORINE_SERVER ?? join(__dirname, "..", "..", "..", "..", "target", "release", "storage-server");
const skip = !existsSync(SERVER);

async function freePort(): Promise<number> {
  return new Promise((resolve) => {
    const srv = net.createServer();
    srv.listen(0, "127.0.0.1", () => {
      const port = (srv.address() as net.AddressInfo).port;
      srv.close(() => resolve(port));
    });
  });
}

async function waitPort(port: number): Promise<void> {
  for (let i = 0; i < 200; i++) {
    const ok = await new Promise<boolean>((r) => {
      const c = net.createConnection({ host: "127.0.0.1", port }, () => {
        c.destroy();
        r(true);
      });
      c.on("error", () => r(false));
    });
    if (ok) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error("server did not start");
}

let proc: ChildProcess;
let dir: string;
let db: s.Client;

before(async () => {
  if (skip) return;
  dir = mkdtempSync(join(tmpdir(), "storine_ts_"));
  const port = await freePort();
  proc = spawn(SERVER, [dir, `127.0.0.1:${port}`], { stdio: "ignore" });
  await waitPort(port);
  db = s.connect(`storine://127.0.0.1:${port}`);
});

after(() => {
  if (skip) return;
  db.close();
  proc.kill();
  rmSync(dir, { recursive: true, force: true });
});

test("crud, types and versions", { skip }, async () => {
  const c = db.collection("people");
  const when = new Date("2024-05-01T12:30:00Z");
  const doc: s.Fields = {
    name: "Ann", age: 37, big: 1n << 100n, score: 9.5, price: new s.Decimal("19.99"), joined: when,
    id: new s.Uuid("123e4567-e89b-12d3-a456-426614174000"), raw: Uint8Array.of(0, 255), tags: ["a", "b"],
    address: { city: "Lagos" }, nothing: null, ok: true,
  };
  const v1 = await c.insert("ann", doc);
  const got = await c.get("ann");
  assert.equal(got!.version, v1);
  assert.equal(got!.fields.name, "Ann");
  assert.equal(got!.fields.big, 1n << 100n);
  assert.ok((got!.fields.price as s.Decimal).equals(new s.Decimal("19.99")));
  assert.equal((got!.fields.joined as Date).getTime(), when.getTime());
  assert.equal((got!.fields.id as s.Uuid).toString(), "123e4567-e89b-12d3-a456-426614174000");
  await assert.rejects(c.insert("ann", doc), s.ConflictError);
  const v2 = await c.update("ann", s.set("address.city", "Abuja"), s.unset("nothing"), s.append("tags", "c"));
  assert.ok(v2 > v1);
  const g2 = await c.get("ann");
  assert.deepEqual(g2!.fields.address, { city: "Abuja" });
  assert.deepEqual(g2!.fields.tags, ["a", "b", "c"]);
  assert.ok(!("nothing" in g2!.fields));
  await assert.rejects(c.replace("ann", { name: "x" }, { ifVersion: v1 }), s.ConflictError);
  await c.replace("ann", { name: "Ann B" }, { ifVersion: v2 });
  await assert.rejects(c.update("nobody", s.set("a", 1)), s.NotFoundError);
  assert.equal(await c.delete("ann"), true);
  assert.equal(await c.delete("ann"), false);
  assert.equal(await c.get("ann"), undefined);
});

test("money is exact and never lost", { skip }, async () => {
  const acct = db.collection("accounts");
  await acct.put("a", { balance: new s.Decimal("0.00") });
  await Promise.all(Array.from({ length: 8 }, async () => {
    for (let i = 0; i < 50; i++) await acct.update("a", s.add("balance", new s.Decimal("0.01")));
  }));
  assert.equal(((await acct.get("a"))!.fields.balance as s.Decimal).toString(), "4.00");
  await assert.rejects(acct.update("a", s.add("balance", new s.Decimal("-4.01"), { min: 0 })), s.InvalidError);
  assert.equal(((await acct.get("a"))!.fields.balance as s.Decimal).toString(), "4.00");
});

test("transactions are atomic", { skip }, async () => {
  const bank = db.collection("bank");
  await bank.put("alice", { balance: new s.Decimal("50") });
  await bank.put("bob", { balance: new s.Decimal("0") });
  await db.transaction((tx) => {
    tx.update("bank", "alice", s.add("balance", new s.Decimal("-20"), { min: 0 }));
    tx.update("bank", "bob", s.add("balance", new s.Decimal("20")));
  });
  await assert.rejects(db.transaction((tx) => {
    tx.update("bank", "alice", s.add("balance", new s.Decimal("-31"), { min: 0 }));
    tx.update("bank", "bob", s.add("balance", new s.Decimal("31")));
  }), s.InvalidError);
  assert.equal(((await bank.get("alice"))!.fields.balance as s.Decimal).toString(), "30");
  assert.equal(((await bank.get("bob"))!.fields.balance as s.Decimal).toString(), "20");
});

test("queries, indexes and pagination", { skip }, async () => {
  const items = db.collection("items");
  for (let i = 0; i < 120; i++) await items.put(`i${String(i).padStart(3, "0")}`, { n: i, kind: i % 2 ? "odd" : "even" });
  const q = s.and(s.between("n", 10, 19), s.eq("kind", "odd"));
  const before = (await items.find(q, { orderBy: "-n" })).map((d) => d.key);
  assert.deepEqual(before, ["i019", "i017", "i015", "i013", "i011"]);
  await items.createIndex("n");
  assert.deepEqual((await items.find(q, { orderBy: "-n" })).map((d) => d.key), before);
  assert.ok((await items.info()).indexes.includes("n"));
  assert.equal(await items.count({ kind: "even" }), 60);
  let n = 0;
  for await (const _d of items.scan(undefined, 7)) n++;
  assert.equal(n, 120);
  assert.ok((await db.collections()).includes("items"));
});

test("vector search", { skip }, async () => {
  const c = db.collection("ts_docs");
  await c.createVectorIndex("emb", 3);
  assert.deepEqual(await c.vectorIndexes(), [{ field: "emb", dims: 3, ready: true }]);
  for (const [key, emb, kind] of [["a", [1, 0, 0], "x"], ["b", [0, 1, 0], "y"], ["c", [0.7, 0.7, 0], "x"]] as const) {
    await c.put(key, { emb: [...emb], kind });
  }
  const hits = await c.nearest("emb", [1, 0.1, 0], { k: 2 });
  assert.deepEqual(hits.map((d) => d.key), ["a", "c"]);
  assert.ok(hits[0].score! > hits[1].score!);
  assert.deepEqual((await c.nearest("emb", [1, 0.1, 0], { k: 5, where: { kind: "x" }, exact: true })).map((d) => d.key), ["a", "c"]);
  await assert.rejects(c.put("bad", { emb: [1, 2] }), s.InvalidError); // wrong dimensions
  await assert.rejects(c.nearest("emb", [1, 2], { k: 1 }), s.InvalidError);
  await c.dropVectorIndex("emb");
  assert.deepEqual(await c.vectorIndexes(), []);
});

test("large values: projection, byte ranges and streaming", { skip }, async () => {
  const c = db.collection("ts_files");
  const blob = new Uint8Array(randomBytes(3 * 1024 * 1024 + 17));
  const v = await c.put("f1", { data: blob, name: "cat.png", meta: { size: blob.length, type: "image/png" } });
  const d = await c.get("f1", { fields: ["name", "meta.size", "absent"] });
  assert.deepEqual(d, { key: "f1", version: v, fields: { name: "cat.png", meta: { size: blob.length } } });
  const r = await c.readBytes("f1", "data", 1000, 16);
  assert.deepEqual(r, { version: v, total: blob.length, data: blob.subarray(1000, 1016) });
  assert.equal((await c.readBytes("f1", "data", blob.length + 5, 10))!.data.length, 0);
  const parts: Uint8Array[] = [];
  for await (const chunk of c.streamBytes("f1", "data", { chunkSize: 1 << 20 })) parts.push(chunk);
  assert.ok(Buffer.concat(parts).equals(Buffer.from(blob)));
  assert.equal(new TextDecoder().decode((await c.readBytes("f1", "name", 0, 3))!.data), "cat");
  assert.equal(await c.readBytes("nope", "data"), undefined);
  await assert.rejects(async () => {
    for await (const _ of c.streamBytes("nope", "data")) { /* drain */ }
  }, s.NotFoundError);
  await assert.rejects(c.readBytes("f1", "meta"), s.InvalidError); // not bytes
  await assert.rejects(c.readBytes("f1", "data", 0, 17 * 1024 * 1024), s.InvalidError);
  const chunks = c.streamBytes("f1", "data", { chunkSize: 1 << 20 });
  await chunks.next();
  await c.update("f1", s.set("name", "dog.png"));
  await assert.rejects(async () => {
    for await (const _ of chunks) { /* drain */ }
  }, s.ConflictError);
});

test("analytics: aggregate, columnar and column scans", { skip, timeout: 120000 }, async () => {
  const sales = db.collection("ts_sales");
  const regions = ["eu", "us", "apac"];
  type Row = { key: string; region: string; qty: number; price: bigint; day: number }; // price in cents
  const data: Row[] = [];
  const insert = async (from: number, to: number) => {
    for (let start = from; start < to; start += 100) {
      await db.transaction((tx) => {
        for (let i = start; i < Math.min(start + 100, to); i++) {
          const row = { key: `s${String(i).padStart(4, "0")}`, region: regions[i % 3], qty: ((i * 7) % 50) + 1, price: BigInt(((i * 37) % 10000) + 1), day: i % 60 };
          data.push(row);
          tx.insert("ts_sales", row.key, { region: row.region, qty: row.qty, price: new s.Decimal(row.price, 2), day: row.day });
        }
      });
    }
  };
  await insert(0, 3000);

  const halfEven = (num: bigint, den: bigint): bigint => {
    const q = num / den;
    const twice = 2n * (num % den);
    return twice > den || (twice === den && q % 2n !== 0n) ? q + 1n : q;
  };
  const dec = (v: s.Value): s.Decimal => {
    assert.ok(v instanceof s.Decimal, `expected a Decimal, got ${String(v)}`);
    return v;
  };
  const aggs = {
    n: s.count(),
    revenue: s.sum(s.field("price").times("qty")),
    avg_qty: s.avg("qty"),
    min_price: s.min("price"),
    max_price: s.max("price"),
    days: s.countDistinct("day"),
  };
  const checkGrouped = async () => {
    const groups = new Map<string, { n: number; revenue: bigint; qty: bigint; min: bigint; max: bigint; days: Set<number> }>();
    for (const d of data) {
      const g = groups.get(d.region) ?? { n: 0, revenue: 0n, qty: 0n, min: d.price, max: d.price, days: new Set<number>() };
      g.n++;
      g.revenue += d.price * BigInt(d.qty);
      g.qty += BigInt(d.qty);
      if (d.price < g.min) g.min = d.price;
      if (d.price > g.max) g.max = d.price;
      g.days.add(d.day);
      groups.set(d.region, g);
    }
    const expected = [...groups.entries()].sort((a, b) => (a[1].revenue > b[1].revenue ? -1 : a[1].revenue < b[1].revenue ? 1 : 0)).slice(0, 2);
    const res = await sales.aggregate(aggs, { groupBy: "region", orderBy: "-revenue", limit: 2 });
    assert.equal(res.matched, data.length);
    assert.deepEqual(res.columns, ["region", "n", "revenue", "avg_qty", "min_price", "max_price", "days"]);
    assert.equal(res.rows.length, 2);
    res.rows.forEach((row, i) => {
      const [region, g] = expected[i];
      assert.deepEqual(Object.keys(row), res.columns);
      assert.equal(row.region, region);
      assert.equal(row.n, g.n);
      assert.ok(dec(row.revenue).equals(new s.Decimal(g.revenue, 2)), `revenue ${row.revenue}`);
      assert.equal(dec(row.avg_qty).toString(), new s.Decimal(halfEven(g.qty * 1_000_000n, BigInt(g.n)), 6).toString());
      assert.ok(dec(row.min_price).equals(new s.Decimal(g.min, 2)));
      assert.ok(dec(row.max_price).equals(new s.Decimal(g.max, 2)));
      assert.equal(row.days, g.days.size);
    });
    return res;
  };
  const checkBuckets = async () => {
    const byWeek = new Map<number, { n: number; qty: number }>();
    for (const d of data) {
      const b = Math.floor(d.day / 7) * 7;
      const g = byWeek.get(b) ?? { n: 0, qty: 0 };
      g.n++;
      g.qty += d.qty;
      byWeek.set(b, g);
    }
    const expected = [...byWeek.entries()].sort((a, b) => a[0] - b[0]).map(([day, g]) => ({ day, n: g.n, qty: g.qty }));
    const res = await sales.aggregate([["n", s.count()], ["qty", s.sum("qty")]], { groupBy: s.bucket("day", 7) , orderBy: "day" });
    assert.deepEqual(res.rows, expected);
  };
  const checkFiltered = async () => {
    const sel = data.filter((d) => d.region === "us" && d.day >= 30);
    const res = await sales.aggregate({ n: s.count(), qty: s.sum("qty"), qty_minus_1: s.sum(s.field("qty").minus(1)) }, { where: s.and(s.eq("region", "us"), s.ge("day", 30)) });
    const qty = sel.reduce((a, d) => a + d.qty, 0);
    assert.deepEqual(res.rows, [{ n: sel.length, qty, qty_minus_1: qty - sel.length }]);
    assert.equal(res.matched, sel.length);
  };

  await checkGrouped();
  await sales.enableColumnar();
  await checkGrouped();
  await checkBuckets();
  await checkFiltered();
  await insert(3000, 3030); // not-yet-merged writes are included too
  const after = await checkGrouped();
  assert.equal(after.spilledBytes, 0);
  await checkBuckets();
  await checkFiltered();

  const eu = data.filter((d) => d.region === "eu").sort((a, b) => (a.key < b.key ? -1 : 1));
  const got: s.ScanRow[] = [];
  for await (const row of sales.scanColumns(["qty", "price"], { where: s.eq("region", "eu"), pageSize: 500 })) got.push(row);
  assert.ok(eu.length > 1000, "several pages");
  assert.equal(got.length, eu.length);
  got.forEach((row, i) => {
    assert.equal(row.key, eu[i].key);
    assert.deepEqual(Object.keys(row.fields), ["qty", "price"]);
    assert.equal(row.fields.qty, eu[i].qty);
    assert.ok(dec(row.fields.price).equals(new s.Decimal(eu[i].price, 2)));
  });
  const missingField = [];
  for await (const row of sales.scanColumns(["nope"], { where: { region: "apac", day: 2 } })) missingField.push(row);
  assert.ok(missingField.length > 0 && missingField.every((r) => r.fields.nope === null));

  await assert.rejects(sales.aggregate({ n: s.count() }, { orderBy: "missing" }), s.InvalidError);

  await sales.disableColumnar();
  await checkGrouped();
  await checkFiltered();
});

test("three-node cluster: redirects and failover", { skip, timeout: 60000 }, async () => {
  const base = mkdtempSync(join(tmpdir(), "storine_ts_cluster_"));
  const ports = [await freePort(), await freePort(), await freePort()];
  const peers = ports.map((p, i) => `${i + 1}=127.0.0.1:${p}`).join(",");
  const procs = ports.map((p, i) => spawn(SERVER, [join(base, `n${i + 1}`), `127.0.0.1:${p}`, "--node-id", String(i + 1), "--peers", peers], { stdio: "ignore" }));
  try {
    for (const p of ports) await waitPort(p);
    const cdb = s.connect("storine://" + ports.map((p) => `127.0.0.1:${p}`).join(","), { timeoutMs: 2000 });
    const acct = cdb.collection("acct");
    for (let i = 0; ; i++) {
      try {
        await acct.put("a", { bal: new s.Decimal("100") });
        break;
      } catch (e) {
        if (i > 100) throw e;
        await new Promise((r) => setTimeout(r, 100));
      }
    }
    for (let i = 0; i < 10; i++) await acct.update("a", s.add("bal", new s.Decimal("1")));
    const leader = (await cdb.clusterInfo()).nodeId;
    procs[leader - 1].kill("SIGKILL");
    for (let i = 0; i < 5; i++) await acct.update("a", s.add("bal", new s.Decimal("1")));
    assert.equal(((await acct.get("a"))!.fields.bal as s.Decimal).toString(), "115");
    cdb.close();
  } finally {
    for (const p of procs) p.kill();
    rmSync(base, { recursive: true, force: true });
  }
});
