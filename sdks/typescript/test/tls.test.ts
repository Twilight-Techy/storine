// TLS on the native port, with certificates from `storage-server gen-ca` / `gen-cert`.
import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import * as net from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import * as s from "../src";

const SERVER = process.env.STORINE_SERVER ?? join(__dirname, "..", "..", "..", "..", "target", "release", "storage-server");

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

test("TLS round trip, and a server from another CA refused", { skip: !existsSync(SERVER) }, async () => {
  const d = mkdtempSync(join(tmpdir(), "storine_ts_tls_"));
  execFileSync(SERVER, ["gen-ca", join(d, "ca")]);
  execFileSync(SERVER, ["gen-cert", join(d, "ca"), join(d, "node"), "127.0.0.1,localhost"]);
  execFileSync(SERVER, ["gen-ca", join(d, "other")]);
  const port = await freePort();
  const proc = spawn(SERVER, [join(d, "data"), `127.0.0.1:${port}`], {
    stdio: "ignore",
    env: { ...process.env, STORAGE_TLS_CERT: join(d, "node", "node.crt"), STORAGE_TLS_KEY: join(d, "node", "node.key") },
  });
  try {
    await waitPort(port);
    const db = s.connect(`storines://127.0.0.1:${port}`, { tlsCa: join(d, "ca", "ca.crt") });
    const big = "x".repeat(100_000);
    await db.collection("t").put("k", { v: big });
    assert.equal((await db.collection("t").get("k"))?.fields.v, big);
    db.close();
    const named = s.connect(`storines://127.0.0.1:${port}?tls_ca=${join(d, "ca", "ca.crt")}&tls_server_name=localhost`);
    assert.ok(await named.collection("t").get("k"));
    named.close();
    const other = s.connect(`storines://127.0.0.1:${port}`, { tlsCa: join(d, "other", "ca.crt"), timeoutMs: 1000 });
    await assert.rejects(other.collection("t").get("k"));
    other.close();
  } finally {
    proc.kill();
    rmSync(d, { recursive: true, force: true });
  }
});
