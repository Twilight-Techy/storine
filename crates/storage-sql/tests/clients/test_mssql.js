// Node.js SQL Server clients against storine-sql's TDS endpoint: tedious (raw), Sequelize
// (mssql dialect) and TypeORM (mssql driver).
//
//   npm install tedious mssql sequelize typeorm reflect-metadata
//   node test_mssql.js [mssql_port]

require("reflect-metadata");
const assert = require("assert");

const PORT = Number(process.argv[2] || 51433);

async function section(name, fn) {
  await fn();
  console.log(`OK  ${name}`);
}

function tediousConnect(encrypt = false, extra = {}) {
  const { Connection } = require("tedious");
  return new Promise((resolve, reject) => {
    const c = new Connection({
      server: "127.0.0.1",
      authentication: { type: "default", options: { userName: "sa", password: "x" } },
      options: { port: PORT, database: "shop", encrypt, trustServerCertificate: true, rowCollectionOnRequestCompletion: true, ...extra },
    });
    c.on("connect", (err) => (err ? reject(err) : resolve(c)));
    c.connect();
  });
}

function exec(c, sql, params = []) {
  const { Request } = require("tedious");
  return new Promise((resolve, reject) => {
    const r = new Request(sql, (err, count, rows) => (err ? reject(err) : resolve({ count, rows: (rows || []).map((row) => row.map((col) => col.value)) })));
    for (const [name, type, value, opts] of params) r.addParameter(name, type, value, opts);
    c.execSql(r);
  });
}

function tx(c, fn) {
  return new Promise((resolve, reject) => {
    c.transaction(async (err, done) => {
      if (err) return reject(err);
      try {
        const v = await fn();
        done(null, (e) => (e ? reject(e) : resolve(v)));
      } catch (e) {
        done(e, () => reject(e));
      }
    });
  });
}

async function main() {
  const { TYPES } = require("tedious");

  await section("tedious: parameters, types, transactions, TLS", async () => {
    const c = await tediousConnect();
    await exec(c, "DROP TABLE IF EXISTS td_acct");
    await exec(c, "CREATE TABLE td_acct (id INT IDENTITY(1,1) PRIMARY KEY, owner NVARCHAR(40) NOT NULL UNIQUE, balance DECIMAL(12,2) NOT NULL, seen DATETIME2 NULL, flag BIT NOT NULL DEFAULT 0)");
    for (const [owner, bal] of [["ann", 100], ["bob", 0]]) {
      const r = await exec(c, "INSERT INTO td_acct (owner, balance, seen) OUTPUT INSERTED.id VALUES (@o, @b, @s)", [
        ["o", TYPES.NVarChar, owner],
        ["b", TYPES.Decimal, bal, { precision: 12, scale: 2 }],
        ["s", TYPES.DateTime2, new Date("2024-05-06T07:08:09.123Z")],
      ]);
      assert.ok(r.rows[0][0] > 0);
    }
    const r = await exec(c, "SELECT owner, balance, seen, flag FROM td_acct WHERE owner = @o", [["o", TYPES.NVarChar, "ann"]]);
    assert.deepStrictEqual(r.rows[0].slice(0, 2), ["ann", 100]);
    assert.strictEqual(r.rows[0][2].toISOString(), "2024-05-06T07:08:09.123Z");
    assert.strictEqual(r.rows[0][3], false);
    // Concurrent transfers in transactions.
    await Promise.all(
      Array.from({ length: 4 }, async () => {
        const w = await tediousConnect();
        for (let j = 0; j < 10; ) {
          try {
            await tx(w, async () => {
              await exec(w, "UPDATE td_acct SET balance = balance - 1 WHERE owner = 'ann'");
              await exec(w, "UPDATE td_acct SET balance = balance + 1 WHERE owner = 'bob'");
            });
            j++;
          } catch (e) {
            if (!/40001|could not serialize|deadlock|lock/i.test(e.message)) throw e;
          }
        }
        w.close();
      }),
    );
    const sum = await exec(c, "SELECT owner, balance FROM td_acct ORDER BY owner");
    assert.deepStrictEqual(sum.rows, [["ann", 60], ["bob", 40]]);
    await assert.rejects(exec(c, "INSERT INTO td_acct (owner, balance) VALUES ('ann', 1)"), (e) => e.number === 2627);
    c.close();
    // encrypt: true (TLS inside TDS 7.4), and TDS 8 strict mode against the pinned certificate.
    const modes = [[true, {}]];
    if (process.env.STORINE_TLS_CERT) {
      const ca = require("fs").readFileSync(process.env.STORINE_TLS_CERT);
      modes.push(["strict", { serverName: "localhost", trustServerCertificate: false, cryptoCredentialsDetails: { ca } }]);
    }
    const big = "x".repeat(100000);
    for (const [encrypt, extra] of modes) {
      const t = await tediousConnect(encrypt, extra);
      const b = await exec(t, "SELECT @s + 'y' AS v", [["s", TYPES.NVarChar, big]]);
      assert.strictEqual(b.rows[0][0], big + "y", String(encrypt));
      t.close();
    }
  });

  await section("Sequelize ORM (mssql dialect): sync, associations, paging, transactions", async () => {
    const { Sequelize, DataTypes, Op } = require("sequelize");
    const db = new Sequelize("shop", "sa", "x", {
      dialect: "mssql",
      host: "127.0.0.1",
      port: PORT,
      logging: false,
      dialectOptions: { options: { encrypt: false, trustServerCertificate: true } },
    });
    const User = db.define("sq_user", { email: { type: DataTypes.STRING, unique: true, allowNull: false }, credit: { type: DataTypes.DECIMAL(10, 2), defaultValue: 0 } }, { tableName: "sq_ms_users" });
    const Post = db.define("sq_post", { title: DataTypes.STRING, body: DataTypes.TEXT }, { tableName: "sq_ms_posts" });
    User.hasMany(Post, { onDelete: "CASCADE" });
    Post.belongsTo(User);
    await Post.drop().catch(() => {});
    await User.drop().catch(() => {});
    await db.sync();
    await db.sync({ alter: true });
    const u = await User.create({ email: "a@x", credit: "5.25" });
    await Post.bulkCreate([{ title: "t1", sqUserId: u.id }, { title: "t2", sqUserId: u.id, body: "b".repeat(5000) }]);
    const got = await User.findOne({ where: { email: "a@x" }, include: Post });
    assert.strictEqual(got.sq_posts.length, 2);
    assert.strictEqual(String(got.credit), "5.25");
    const page = await Post.findAll({ order: [["id", "ASC"]], limit: 1, offset: 1 });
    assert.deepStrictEqual(page.map((p) => p.title), ["t2"]);
    assert.strictEqual(await Post.count({ where: { title: { [Op.like]: "t%" } } }), 2);
    await assert.rejects(User.create({ email: "a@x" }), (e) => e.name === "SequelizeUniqueConstraintError");
    await db.transaction(async (t) => {
      await User.increment({ credit: 1 }, { where: { id: u.id }, transaction: t });
    });
    await assert.rejects(
      db.transaction(async (t) => {
        await User.update({ credit: 0 }, { where: { id: u.id }, transaction: t });
        throw new Error("abort");
      }),
    );
    assert.strictEqual(String((await User.findByPk(u.id)).credit), "6.25");
    await u.destroy();
    assert.strictEqual(await Post.count(), 0);
    await db.close();
  });

  await section("TypeORM (mssql driver): synchronize, repositories, query builder, transactions", async () => {
    const { DataSource, EntitySchema } = require("typeorm");
    const Wallet = new EntitySchema({
      name: "Wallet",
      tableName: "to_ms_wallets",
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
      tableName: "to_ms_moves",
      columns: {
        id: { type: Number, primary: true, generated: true },
        amount: { type: "decimal", precision: 12, scale: 2 },
        note: { type: String, nullable: true },
      },
      relations: { wallet: { type: "many-to-one", target: "Wallet", joinColumn: true, onDelete: "CASCADE" } },
    });
    const ds = new DataSource({
      type: "mssql",
      host: "127.0.0.1",
      port: PORT,
      username: "sa",
      password: "x",
      database: "shop",
      entities: [Wallet, Move],
      options: { encrypt: false, trustServerCertificate: true },
      logging: false,
    });
    await ds.initialize();
    await ds.query("DROP TABLE IF EXISTS to_ms_moves");
    await ds.query("DROP TABLE IF EXISTS to_ms_wallets");
    await ds.synchronize();
    await ds.synchronize(); // a second pass must find the schema up to date
    const wallets = ds.getRepository("Wallet");
    const moves = ds.getRepository("Move");
    const a = await wallets.save({ owner: "ada", balance: "10.00" });
    const b = await wallets.save({ owner: "bob" });
    await moves.save([{ amount: "1.50", wallet: a }, { amount: "2.50", wallet: a, note: "x" }]);
    const loaded = await wallets.findOne({ where: { owner: "ada" }, relations: { moves: true } });
    assert.strictEqual(loaded.moves.length, 2);
    assert.strictEqual(Number(loaded.balance), 10);
    const sum = await moves.createQueryBuilder("m").select("SUM(m.amount)", "s").innerJoin("m.wallet", "w").where("w.owner = :o", { o: "ada" }).getRawOne();
    assert.strictEqual(Number(sum.s), 4);
    await Promise.all(Array.from({ length: 20 }, () => wallets.increment({ id: a.id }, "balance", 1)));
    assert.strictEqual(Number((await wallets.findOneBy({ id: a.id })).balance), 30);
    await ds.transaction(async (m) => {
      await m.getRepository("Wallet").decrement({ id: a.id }, "balance", 5);
      await m.getRepository("Wallet").increment({ id: b.id }, "balance", 5);
    });
    await assert.rejects(
      ds.transaction(async (m) => {
        await m.getRepository("Wallet").update(a.id, { balance: "0" });
        throw new Error("abort");
      }),
    );
    assert.strictEqual(Number((await wallets.findOneBy({ id: a.id })).balance), 25);
    const page = await wallets.find({ order: { id: "ASC" }, skip: 1, take: 1 });
    assert.deepStrictEqual(page.map((w) => w.owner), ["bob"]);
    await ds.destroy();
  });

  console.log("all Node SQL Server client tests passed");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
