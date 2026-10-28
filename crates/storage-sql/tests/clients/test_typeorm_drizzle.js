// TypeORM (schema synchronize + repositories + transactions) and Drizzle ORM against
// storine-sql on both endpoints.
//
//   npm install typeorm reflect-metadata drizzle-orm pg mysql2
//   node test_typeorm_drizzle.js [pg_port] [mysql_port]

require("reflect-metadata");
const assert = require("assert");
const { DataSource, EntitySchema } = require("typeorm");

const PG = Number(process.argv[2] || 55432);
const MY = Number(process.argv[3] || 53306);

const Wallet = new EntitySchema({
  name: "Wallet",
  tableName: "to_wallets",
  columns: {
    id: { type: Number, primary: true, generated: true },
    owner: { type: String, unique: true, length: 100 },
    balance: { type: "decimal", precision: 12, scale: 2, default: 0 },
    version: { type: Number, version: true },
    created: { type: Date, createDate: true },
  },
  relations: { moves: { type: "one-to-many", target: "Move", inverseSide: "wallet" } },
});
const Move = new EntitySchema({
  name: "Move",
  tableName: "to_moves",
  columns: {
    id: { type: Number, primary: true, generated: true },
    amount: { type: "decimal", precision: 12, scale: 2 },
    note: { type: String, nullable: true },
  },
  relations: { wallet: { type: "many-to-one", target: "Wallet", joinColumn: true, onDelete: "CASCADE" } },
});

async function typeorm(type, port, username) {
  const ds = new DataSource({ type, host: "127.0.0.1", port, username, password: type === "postgres" ? "x" : "", database: "shop", entities: [Wallet, Move], logging: false });
  await ds.initialize();
  await ds.query(type === "postgres" ? 'DROP TABLE IF EXISTS "to_moves", "to_wallets" CASCADE' : "DROP TABLE IF EXISTS `to_moves`, `to_wallets`");
  await ds.synchronize();
  await ds.synchronize(); // a second pass must find the schema up to date
  const wallets = ds.getRepository("Wallet");
  const moves = ds.getRepository("Move");
  const a = await wallets.save({ owner: "ada", balance: "10.00" });
  const b = await wallets.save({ owner: "bob" });
  await moves.save([{ amount: "1.50", wallet: a }, { amount: "2.50", wallet: a, note: "x" }]);
  const loaded = await wallets.findOne({ where: { owner: "ada" }, relations: { moves: true } });
  assert.strictEqual(loaded.moves.length, 2);
  assert.strictEqual(loaded.balance, "10.00");
  const sum = await moves.createQueryBuilder("m").select("SUM(m.amount)", "s").innerJoin("m.wallet", "w").where("w.owner = :o", { o: "ada" }).getRawOne();
  assert.strictEqual(Number(sum.s), 4);
  // Concurrent atomic increments.
  await Promise.all(Array.from({ length: 30 }, () => wallets.increment({ id: a.id }, "balance", 1)));
  assert.strictEqual((await wallets.findOneBy({ id: a.id })).balance, "40.00");
  // Transaction with pessimistic lock; and one that rolls back.
  await ds.transaction(async (m) => {
    const src = await m.getRepository("Wallet").findOne({ where: { id: a.id }, lock: { mode: "pessimistic_write" } });
    await m.getRepository("Wallet").update(src.id, { balance: (Number(src.balance) - 5).toFixed(2) });
    await m.getRepository("Wallet").increment({ id: b.id }, "balance", 5);
  });
  await assert.rejects(
    ds.transaction(async (m) => {
      await m.getRepository("Wallet").update(b.id, { balance: "0" });
      throw new Error("abort");
    }),
    /abort/
  );
  assert.strictEqual((await wallets.findOneBy({ id: b.id })).balance, "5.00");
  await assert.rejects(wallets.insert({ owner: "ada" }), (e) => ["23505", "ER_DUP_ENTRY"].includes(e.code));
  await wallets.delete({ id: a.id });
  assert.strictEqual(await moves.count(), 0);
  await ds.destroy();
  console.log(`OK  TypeORM (${type})`);
}

async function drizzlePg() {
  const { Pool } = require("pg");
  const { drizzle } = require("drizzle-orm/node-postgres");
  const { pgTable, serial, text, numeric, integer } = require("drizzle-orm/pg-core");
  const { eq, sql, gt, desc } = require("drizzle-orm");
  const pool = new Pool({ host: "127.0.0.1", port: PG, user: "app", password: "x", database: "shop" });
  await pool.query("DROP TABLE IF EXISTS dz_items");
  await pool.query("CREATE TABLE dz_items (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE, price NUMERIC(10,2) NOT NULL, stock INT NOT NULL DEFAULT 0)");
  const items = pgTable("dz_items", { id: serial("id").primaryKey(), name: text("name").notNull(), price: numeric("price", { precision: 10, scale: 2 }).notNull(), stock: integer("stock").notNull().default(0) });
  const db = drizzle(pool);
  const [row] = await db.insert(items).values({ name: "pen", price: "1.25" }).returning();
  assert.strictEqual(row.id, 1);
  await db.insert(items).values([{ name: "ink", price: "3.00", stock: 2 }, { name: "pad", price: "2.50" }]);
  await db.update(items).set({ stock: sql`${items.stock} + 5` }).where(eq(items.name, "pen"));
  const list = await db.select().from(items).where(gt(items.price, "1.00")).orderBy(desc(items.price));
  assert.deepStrictEqual(list.map((r) => [r.name, r.price, r.stock]), [["ink", "3.00", 2], ["pad", "2.50", 0], ["pen", "1.25", 5]]);
  await db.transaction(async (tx) => {
    await tx.update(items).set({ stock: 0 }).where(eq(items.name, "ink"));
    tx.rollback();
  }).catch(() => {});
  assert.strictEqual((await db.select().from(items).where(eq(items.name, "ink")))[0].stock, 2);
  const [{ n }] = await db.select({ n: sql`count(*)`.mapWith(Number) }).from(items);
  assert.strictEqual(n, 3);
  await pool.end();
  console.log("OK  Drizzle (node-postgres)");
}

async function drizzleMy() {
  const mysql = require("mysql2/promise");
  const { drizzle } = require("drizzle-orm/mysql2");
  const { mysqlTable, int, varchar, decimal } = require("drizzle-orm/mysql-core");
  const { eq, sql } = require("drizzle-orm");
  const conn = await mysql.createConnection({ host: "127.0.0.1", port: MY, user: "root", database: "shop" });
  await conn.query("DROP TABLE IF EXISTS dzm_items");
  await conn.query("CREATE TABLE dzm_items (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50) NOT NULL UNIQUE, price DECIMAL(10,2) NOT NULL, stock INT NOT NULL DEFAULT 0)");
  const items = mysqlTable("dzm_items", { id: int("id").primaryKey().autoincrement(), name: varchar("name", { length: 50 }).notNull(), price: decimal("price", { precision: 10, scale: 2 }).notNull(), stock: int("stock").notNull().default(0) });
  const db = drizzle(conn);
  const [res] = await db.insert(items).values({ name: "pen", price: "1.25" });
  assert.strictEqual(res.insertId, 1);
  await db.update(items).set({ stock: sql`${items.stock} + 3` }).where(eq(items.id, 1));
  const [r] = await db.select().from(items).where(eq(items.name, "pen"));
  assert.deepStrictEqual([r.price, r.stock], ["1.25", 3]);
  await conn.end();
  console.log("OK  Drizzle (mysql2)");
}

(async () => {
  await typeorm("postgres", PG, "app");
  await typeorm("mysql", MY, "root");
  await drizzlePg();
  await drizzleMy();
  console.log("all typeorm/drizzle tests passed");
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
