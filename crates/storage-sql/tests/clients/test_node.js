// Real-client compatibility tests for storine-sql (Node.js drivers and ORMs).
//
//   storine-sql --data /tmp/db --pg 127.0.0.1:55432 --mysql 127.0.0.1:53306 &
//   npm install pg mysql2 sequelize knex
//   node test_node.js 55432 53306

const assert = require("assert");

const PG_PORT = Number(process.argv[2] || 55432);
const MY_PORT = Number(process.argv[3] || 53306);

async function section(name, fn) {
  await fn();
  console.log(`OK  ${name}`);
}

async function main() {
  const { Client, Pool } = require("pg");

  await section("node-postgres: extended protocol, types, transactions, pool", async () => {
    const c = new Client({ host: "127.0.0.1", port: PG_PORT, user: "app", password: "x", database: "shop" });
    await c.connect();
    await c.query("DROP TABLE IF EXISTS np_items");
    await c.query(
      "CREATE TABLE np_items (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE, price NUMERIC(10,2), qty INT DEFAULT 0, tags TEXT[], data JSONB, at TIMESTAMPTZ DEFAULT now())"
    );
    const ins = await c.query("INSERT INTO np_items (name, price, tags, data) VALUES ($1, $2, $3, $4) RETURNING id, price", [
      "pen",
      "1.50",
      ["a", "b"],
      JSON.stringify({ k: [1, 2] }),
    ]);
    assert.deepStrictEqual(ins.rows[0], { id: 1, price: "1.50" });
    const r = await c.query("SELECT name, price, qty, tags, data, at FROM np_items WHERE id = $1", [1]);
    assert.strictEqual(r.rows[0].name, "pen");
    assert.strictEqual(r.rows[0].price, "1.50");
    assert.deepStrictEqual(r.rows[0].tags, ["a", "b"]);
    assert.deepStrictEqual(r.rows[0].data, { k: [1, 2] });
    assert.ok(r.rows[0].at instanceof Date);
    await assert.rejects(c.query("INSERT INTO np_items (name) VALUES ('pen')"), (e) => e.code === "23505");
    await c.query("BEGIN");
    await c.query("UPDATE np_items SET qty = qty + $1 WHERE name = $2", [5, "pen"]);
    await c.query("ROLLBACK");
    assert.strictEqual((await c.query("SELECT qty FROM np_items")).rows[0].qty, 0);
    await c.end();
    // Concurrent increments through a pool: exact.
    const pool = new Pool({ host: "127.0.0.1", port: PG_PORT, user: "app", database: "shop", max: 8 });
    await Promise.all(Array.from({ length: 200 }, () => pool.query("UPDATE np_items SET qty = qty + 1 WHERE id = $1", [1])));
    assert.strictEqual((await pool.query("SELECT qty FROM np_items WHERE id = 1")).rows[0].qty, 200);
    await pool.end();
  });

  const mysql = require("mysql2/promise");

  await section("mysql2: text and prepared statements, transactions", async () => {
    const c = await mysql.createConnection({ host: "127.0.0.1", port: MY_PORT, user: "root", database: "shop" });
    await c.query("DROP TABLE IF EXISTS m2_items");
    await c.query(
      "CREATE TABLE m2_items (id INT AUTO_INCREMENT PRIMARY KEY, sku VARCHAR(20) NOT NULL UNIQUE, price DECIMAL(10,2), qty INT DEFAULT 0, made DATETIME, active BOOLEAN DEFAULT true)"
    );
    const [res] = await c.execute("INSERT INTO m2_items (sku, price, made) VALUES (?, ?, ?)", ["A1", "9.99", new Date(Date.UTC(2024, 0, 2, 3, 4, 5))]);
    assert.strictEqual(res.insertId, 1);
    await c.query("INSERT INTO m2_items (sku, price) VALUES ('B2', 1.25), ('C3', 2)");
    const [rows] = await c.execute("SELECT sku, price, qty, active FROM m2_items WHERE price > ? ORDER BY id", [1]);
    assert.deepStrictEqual(rows.map((r) => [r.sku, r.price, r.qty, r.active]), [
      ["A1", "9.99", 0, 1],
      ["B2", "1.25", 0, 1],
      ["C3", "2.00", 0, 1],
    ]);
    const [txt] = await c.query("SELECT sku, made FROM m2_items WHERE id = 1");
    assert.ok(txt[0].made instanceof Date, String(txt[0].made));
    await c.beginTransaction();
    await c.execute("UPDATE m2_items SET qty = 50 WHERE sku = ?", ["A1"]);
    await c.rollback();
    const [[q]] = await c.query("SELECT qty FROM m2_items WHERE sku = 'A1'");
    assert.strictEqual(q.qty, 0);
    await assert.rejects(c.execute("INSERT INTO m2_items (sku) VALUES (?)", ["A1"]), (e) => e.errno === 1062);
    const [up] = await c.query("UPDATE m2_items SET qty = qty + 1");
    assert.strictEqual(up.affectedRows, 3);
    await c.end();
  });

  const { Sequelize, DataTypes, Op } = require("sequelize");

  for (const [label, url] of [
    ["Sequelize ORM (PostgreSQL)", `postgres://app:x@127.0.0.1:${PG_PORT}/shop`],
    ["Sequelize ORM (MySQL)", `mysql://root@127.0.0.1:${MY_PORT}/shop`],
  ]) {
    await section(label, async () => {
      const db = new Sequelize(url, { logging: false });
      const User = db.define("sq_user", {
        email: { type: DataTypes.STRING, unique: true, allowNull: false },
        balance: { type: DataTypes.DECIMAL(12, 2), defaultValue: 0 },
      });
      const Post = db.define("sq_post", { title: DataTypes.STRING, views: { type: DataTypes.INTEGER, defaultValue: 0 } });
      User.hasMany(Post, { onDelete: "CASCADE" });
      Post.belongsTo(User);
      await db.sync({ force: true });
      const u = await User.create({ email: "a@x", balance: "10.50" });
      await Post.bulkCreate([
        { title: "one", sqUserId: u.id },
        { title: "two", sqUserId: u.id, views: 3 },
      ]);
      const found = await User.findOne({ where: { email: "a@x" }, include: Post });
      assert.strictEqual(found.sq_posts.length, 2);
      assert.strictEqual(String(found.balance), "10.50");
      await Post.increment("views", { by: 2, where: { title: "two" } });
      assert.strictEqual((await Post.findOne({ where: { title: "two" } })).views, 5);
      assert.strictEqual(await Post.count({ where: { views: { [Op.gt]: 1 } } }), 1);
      await db.transaction(async (t) => {
        await User.update({ balance: "5.25" }, { where: { id: u.id }, transaction: t });
      });
      assert.strictEqual(String((await User.findByPk(u.id)).balance), "5.25");
      await assert.rejects(User.create({ email: "a@x" }), (e) => e.name === "SequelizeUniqueConstraintError");
      await u.destroy();
      assert.strictEqual(await Post.count(), 0);
      await db.drop();
      await db.close();
    });
  }

  await section("Knex query builder (PostgreSQL)", async () => {
    const knex = require("knex")({ client: "pg", connection: { host: "127.0.0.1", port: PG_PORT, user: "app", database: "shop" } });
    await knex.schema.dropTableIfExists("kx_t");
    await knex.schema.createTable("kx_t", (t) => {
      t.increments("id");
      t.string("name").notNullable();
      t.decimal("amount", 10, 2);
      t.timestamps(true, true);
    });
    await knex("kx_t").insert([{ name: "a", amount: "1.10" }, { name: "b", amount: "2.20" }]);
    const rows = await knex("kx_t").select("name").sum({ s: "amount" }).groupBy("name").orderBy("name");
    assert.deepStrictEqual(rows.map((r) => [r.name, r.s]), [["a", "1.10"], ["b", "2.20"]]);
    assert.ok(await knex.schema.hasTable("kx_t"));
    assert.ok(await knex.schema.hasColumn("kx_t", "amount"));
    await knex.destroy();
  });

  console.log("all node client tests passed");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
