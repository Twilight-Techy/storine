// Prisma ORM against storine-sql on all three endpoints: `prisma db push` (the schema engine's
// introspection and DDL), then the generated client (relations, aggregates, atomic
// increments, transactions, unique violations).
//
//   npm install prisma@6 @prisma/client@6
//   node test_prisma.js [pg_port] [mysql_port] [mssql_port]

const { execFileSync, spawnSync } = require("child_process");
const assert = require("assert");
const os = require("os");
const path = require("path");

const PG = process.argv[2] || 55432;
const MY = process.argv[3] || 53306;
const MS = process.argv[4] || 51433;
const urls = {
  sqlserver: `sqlserver://127.0.0.1:${MS};database=shop;user=sa;password=x;encrypt=true;trustServerCertificate=true`,
  postgresql: `postgresql://app:x@127.0.0.1:${PG}/shop?schema=public`,
  mysql: `mysql://root@127.0.0.1:${MY}/shop`,
};
const prismaBin = path.join(path.dirname(require.resolve("prisma/package.json")), "build", "index.js");

function tedious(sql) {
  const { Connection, Request } = require("tedious");
  return new Promise((resolve, reject) => {
    const c = new Connection({
      server: "127.0.0.1",
      authentication: { type: "default", options: { userName: "sa", password: "x" } },
      options: { port: Number(MS), database: "shop", encrypt: false },
    });
    c.on("connect", (err) => {
      if (err) return reject(err);
      c.execSql(new Request(sql, (e) => (c.close(), e ? reject(e) : resolve())));
    });
    c.connect();
  });
}

async function dropTables(provider) {
  if (provider === "sqlserver") {
    await tedious("DROP TABLE IF EXISTS [PrPost]; DROP TABLE IF EXISTS [PrUser]");
  } else if (provider === "mysql") {
    const c = await require("mysql2/promise").createConnection({ host: "127.0.0.1", port: MY, user: "root", database: "shop" });
    await c.query("DROP TABLE IF EXISTS `PrPost`, `PrUser`");
    await c.end();
  } else {
    const { Client } = require("pg");
    const c = new Client({ host: "127.0.0.1", port: PG, user: "app", password: "x", database: "shop" });
    await c.connect();
    await c.query('DROP TABLE IF EXISTS "PrPost", "PrUser" CASCADE');
    await c.end();
  }
}

async function suite(provider) {
  const out = path.join(os.tmpdir(), `storine-prisma-${provider}-${process.pid}`);
  const env = { ...process.env, [`DATABASE_URL_${provider}`]: urls[provider], [`PRISMA_OUT_${provider}`]: out };
  const schema = path.join(__dirname, `schema.${provider}.prisma`);
  // Start from no tables (the test's own two), then let the schema engine create them.
  await dropTables(provider);
  execFileSync(process.execPath, [prismaBin, "db", "push", "--skip-generate", `--schema=${schema}`], { env, stdio: "pipe" });
  execFileSync(process.execPath, [prismaBin, "generate", `--schema=${schema}`], { env, stdio: "pipe" });
  const { PrismaClient, Prisma } = require(out);
  const db = new PrismaClient({ datasources: { db: { url: urls[provider] } } });
  // Introspecting the created tables must give back exactly the schema (exit 0 = no diff).
  const diff = spawnSync(
    process.execPath,
    [prismaBin, "migrate", "diff", "--from-schema-datasource", schema, "--to-schema-datamodel", schema, "--script", "--exit-code"],
    { env }
  );
  assert.strictEqual(diff.status, 0, `schema drift after db push:\n${diff.stdout}${diff.stderr}`);

  const ada = await db.prUser.create({
    data: { email: "ada@x", balance: new Prisma.Decimal("10.50"), posts: { create: [{ title: "one" }, { title: "two", views: 3 }] } },
    include: { posts: true },
  });
  assert.strictEqual(ada.posts.length, 2);
  assert.strictEqual(ada.balance.toFixed(2), "10.50");
  await db.prUser.create({ data: { email: "bob@x" } });

  const users = await db.prUser.findMany({ orderBy: { email: "asc" }, include: { _count: { select: { posts: true } } } });
  assert.deepStrictEqual(users.map((u) => [u.email, u._count.posts]), [["ada@x", 2], ["bob@x", 0]]);
  const agg = await db.prPost.aggregate({ _sum: { views: true }, _max: { views: true }, where: { author: { email: { startsWith: "a" } } } });
  assert.strictEqual(agg._sum.views, 3);
  const grouped = await db.prPost.groupBy({ by: ["authorId"], _count: { _all: true } });
  assert.strictEqual(grouped[0]._count._all, 2);

  // Atomic increments from parallel requests: none lost.
  await Promise.all(Array.from({ length: 40 }, () => db.prUser.update({ where: { id: ada.id }, data: { balance: { increment: 1 } } })));
  assert.strictEqual((await db.prUser.findUnique({ where: { id: ada.id } })).balance.toFixed(2), "50.50");

  // Interactive transaction that rolls back, then one that commits.
  await assert.rejects(
    db.$transaction(async (tx) => {
      await tx.prUser.update({ where: { email: "bob@x" }, data: { balance: 99 } });
      throw new Error("abort");
    }),
    /abort/
  );
  assert.strictEqual((await db.prUser.findUnique({ where: { email: "bob@x" } })).balance.toFixed(2), "0.00");
  await db.$transaction([
    db.prUser.update({ where: { email: "ada@x" }, data: { balance: { decrement: 5 } } }),
    db.prUser.update({ where: { email: "bob@x" }, data: { balance: { increment: 5 } } }),
  ]);
  assert.strictEqual((await db.prUser.findUnique({ where: { email: "bob@x" } })).balance.toFixed(2), "5.00");

  await assert.rejects(db.prUser.create({ data: { email: "ada@x" } }), (e) => e.code === "P2002");
  const up = await db.prUser.upsert({ where: { email: "cy@x" }, create: { email: "cy@x", balance: 1 }, update: { balance: 2 } });
  assert.strictEqual(up.balance.toFixed(2), "1.00");
  await db.prUser.delete({ where: { id: ada.id } });
  assert.strictEqual(await db.prPost.count(), 0);
  const raw = await db.$queryRaw`SELECT count(*) AS n FROM ${Prisma.raw({ mysql: "`PrUser`", sqlserver: "[PrUser]" }[provider] || '"PrUser"')}`;
  assert.strictEqual(Number(raw[0].n), 2);
  await db.$disconnect();
  console.log(`OK  Prisma (${provider})`);
}

(async () => {
  const providers = ["postgresql", "mysql"];
  if (process.argv[4]) providers.push("sqlserver");
  for (const p of providers) await suite(p);
  console.log("all prisma tests passed");
})().catch((e) => {
  console.error(e.stderr ? e.stderr.toString() : e);
  process.exit(1);
});
