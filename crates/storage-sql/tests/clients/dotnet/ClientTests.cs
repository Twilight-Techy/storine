using System.ComponentModel.DataAnnotations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using MySqlConnector;
using Npgsql;
using Xunit;

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace Storine.SqlClients;

public class Account
{
    public int Id { get; set; }
    [MaxLength(100)] public string Email { get; set; } = "";
    public decimal Balance { get; set; }
    public DateTime Opened { get; set; }
    [ConcurrencyCheck] public int Version { get; set; }
    public List<Entry> Entries { get; set; } = new();
}

public class Entry
{
    public long Id { get; set; }
    public int AccountId { get; set; }
    public Account? Account { get; set; }
    public decimal Amount { get; set; }
    [MaxLength(200)] public string? Memo { get; set; }
}

public class Ledger(DbContextOptions<Ledger> options) : DbContext(options)
{
    public DbSet<Account> Accounts => Set<Account>();
    public DbSet<Entry> Entries => Set<Entry>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<Account>().ToTable("ef_accounts").HasIndex(a => a.Email).IsUnique();
        b.Entity<Account>().Property(a => a.Balance).HasPrecision(12, 2);
        b.Entity<Entry>().ToTable("ef_entries").Property(e => e.Amount).HasPrecision(12, 2);
    }
}

public class ClientTests
{
    static string Pg => Environment.GetEnvironmentVariable("STORINE_PG") ?? "127.0.0.1:55432";
    static string My => Environment.GetEnvironmentVariable("STORINE_MYSQL") ?? "127.0.0.1:53306";
    static string PgConn => $"Host={Pg.Split(':')[0]};Port={Pg.Split(':')[1]};Username=app;Password=x;Database=shop";
    static string MyConn => $"Server={My.Split(':')[0]};Port={My.Split(':')[1]};User ID=root;Database=shop;SslMode=None;AllowPublicKeyRetrieval=true";

    [Fact]
    public async Task NpgsqlTypesParametersAndTransactions()
    {
        await using var ds = NpgsqlDataSource.Create(PgConn);
        await using (var c = ds.CreateCommand("DROP TABLE IF EXISTS np_net")) await c.ExecuteNonQueryAsync();
        await using (var c = ds.CreateCommand("CREATE TABLE np_net (id SERIAL PRIMARY KEY, name TEXT UNIQUE, price NUMERIC(10,2), tags TEXT[], at TIMESTAMPTZ, ok BOOLEAN, uid UUID)"))
            await c.ExecuteNonQueryAsync();
        var uid = Guid.NewGuid();
        var at = new DateTime(2024, 5, 6, 7, 8, 9, DateTimeKind.Utc);
        await using (var c = ds.CreateCommand("INSERT INTO np_net (name, price, tags, at, ok, uid) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id"))
        {
            c.Parameters.Add(new() { Value = "pen" });
            c.Parameters.Add(new() { Value = 1.50m });
            c.Parameters.Add(new() { Value = new[] { "a", "b" } });
            c.Parameters.Add(new() { Value = at });
            c.Parameters.Add(new() { Value = true });
            c.Parameters.Add(new() { Value = uid });
            Assert.Equal(1, (int)(await c.ExecuteScalarAsync())!);
        }
        await using (var c = ds.CreateCommand("SELECT name, price, tags, at, ok, uid FROM np_net WHERE id = $1"))
        {
            c.Parameters.Add(new() { Value = 1 });
            await using var r = await c.ExecuteReaderAsync();
            Assert.True(await r.ReadAsync());
            Assert.Equal("pen", r.GetString(0));
            Assert.Equal(1.50m, r.GetDecimal(1));
            Assert.Equal(new[] { "a", "b" }, r.GetFieldValue<string[]>(2));
            Assert.Equal(at, r.GetDateTime(3).ToUniversalTime());
            Assert.True(r.GetBoolean(4));
            Assert.Equal(uid, r.GetGuid(5));
        }
        await using var conn = await ds.OpenConnectionAsync();
        await using (var tx = await conn.BeginTransactionAsync())
        {
            await using var c = new NpgsqlCommand("UPDATE np_net SET price = 99 WHERE id = 1", conn, tx);
            await c.ExecuteNonQueryAsync();
            await tx.RollbackAsync();
        }
        await using (var c = new NpgsqlCommand("SELECT price FROM np_net", conn)) Assert.Equal(1.50m, (decimal)(await c.ExecuteScalarAsync())!);
        var ex = await Assert.ThrowsAsync<PostgresException>(async () =>
        {
            await using var c = new NpgsqlCommand("INSERT INTO np_net (name) VALUES ('pen')", conn);
            await c.ExecuteNonQueryAsync();
        });
        Assert.Equal("23505", ex.SqlState);
    }

    [Fact]
    public async Task MySqlConnectorTypesAndTransactions()
    {
        await using var conn = new MySqlConnection(MyConn);
        await conn.OpenAsync();
        async Task Exec(string sql) { await using var c = new MySqlCommand(sql, conn); await c.ExecuteNonQueryAsync(); }
        await Exec("DROP TABLE IF EXISTS my_net");
        await Exec("CREATE TABLE my_net (id INT AUTO_INCREMENT PRIMARY KEY, sku VARCHAR(20) NOT NULL UNIQUE, price DECIMAL(10,2), made DATETIME(6), active TINYINT(1) DEFAULT 1)");
        var made = new DateTime(2024, 1, 2, 3, 4, 5);
        await using (var c = new MySqlCommand("INSERT INTO my_net (sku, price, made) VALUES (@s, @p, @m)", conn))
        {
            c.Parameters.AddWithValue("@s", "A1");
            c.Parameters.AddWithValue("@p", 9.99m);
            c.Parameters.AddWithValue("@m", made);
            await c.ExecuteNonQueryAsync();
            Assert.Equal(1, c.LastInsertedId);
        }
        await using (var c = new MySqlCommand("SELECT sku, price, made, active FROM my_net WHERE id = @id", conn))
        {
            c.Parameters.AddWithValue("@id", 1);
            await c.PrepareAsync(); // binary protocol
            await using var r = await c.ExecuteReaderAsync();
            Assert.True(await r.ReadAsync());
            Assert.Equal("A1", r.GetString(0));
            Assert.Equal(9.99m, r.GetDecimal(1));
            Assert.Equal(made, r.GetDateTime(2));
            Assert.True(r.GetBoolean(3));
        }
        await using (var tx = await conn.BeginTransactionAsync())
        {
            await using var c = new MySqlCommand("UPDATE my_net SET price = 1 WHERE id = 1", conn, tx);
            await c.ExecuteNonQueryAsync();
            await tx.RollbackAsync();
        }
        await using (var c = new MySqlCommand("SELECT price FROM my_net", conn)) Assert.Equal(9.99m, (decimal)(await c.ExecuteScalarAsync())!);
        var ex = await Assert.ThrowsAsync<MySqlException>(() => Exec("INSERT INTO my_net (sku) VALUES ('A1')"));
        Assert.Equal(MySqlErrorCode.DuplicateKeyEntry, ex.ErrorCode);
    }

    static async Task EfSuite(DbContextOptions<Ledger> options)
    {
        await using (var db = new Ledger(options))
        {
            // Both endpoints share one database, so drop just this suite's tables
            // (EnsureCreated skips a database that already has any table).
            await db.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS ef_entries");
            await db.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS ef_accounts");
            await db.GetService<IRelationalDatabaseCreator>().CreateTablesAsync();
            var a = new Account { Email = "a@x", Balance = 100m, Opened = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc) };
            a.Entries.Add(new Entry { Amount = 100m, Memo = "open" });
            db.Accounts.Add(a);
            db.Accounts.Add(new Account { Email = "b@x", Opened = DateTime.UtcNow });
            await db.SaveChangesAsync();
        }
        await using (var db = new Ledger(options))
        {
            var a = await db.Accounts.Include(x => x.Entries).SingleAsync(x => x.Email == "a@x");
            Assert.Equal(100m, a.Balance);
            Assert.Single(a.Entries);
            var totals = await db.Accounts.OrderBy(x => x.Email).Select(x => new { x.Email, Sum = x.Entries.Sum(e => e.Amount), N = x.Entries.Count() }).ToListAsync();
            Assert.Equal(2, totals.Count);
            Assert.Equal(100m, totals[0].Sum);
            Assert.Equal(0, totals[1].N);
            Assert.Equal(1, await db.Accounts.CountAsync(x => x.Email.StartsWith("b")));
        }
        // Concurrent transfers inside transactions: balances stay exact.
        var workers = Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
        {
            for (var i = 0; i < 10; i++)
            {
                await using var db = new Ledger(options);
                var strategy = db.Database.CreateExecutionStrategy();
                while (true)
                {
                    try
                    {
                        await using var tx = await db.Database.BeginTransactionAsync();
                        await db.Accounts.Where(x => x.Email == "a@x").ExecuteUpdateAsync(s => s.SetProperty(x => x.Balance, x => x.Balance - 1));
                        await db.Accounts.Where(x => x.Email == "b@x").ExecuteUpdateAsync(s => s.SetProperty(x => x.Balance, x => x.Balance + 1));
                        await tx.CommitAsync();
                        break;
                    }
                    catch (Exception e) when (IsRetryable(e))
                    {
                        db.ChangeTracker.Clear();
                    }
                }
            }
        })).ToArray();
        await Task.WhenAll(workers);
        await using (var db = new Ledger(options))
        {
            Assert.Equal(60m, (await db.Accounts.SingleAsync(x => x.Email == "a@x")).Balance);
            Assert.Equal(40m, (await db.Accounts.SingleAsync(x => x.Email == "b@x")).Balance);
        }
        // Optimistic concurrency token: a stale write is rejected.
        await using (var db1 = new Ledger(options))
        await using (var db2 = new Ledger(options))
        {
            var x1 = await db1.Accounts.SingleAsync(x => x.Email == "a@x");
            var x2 = await db2.Accounts.SingleAsync(x => x.Email == "a@x");
            x1.Version++;
            x1.Balance = 1;
            await db1.SaveChangesAsync();
            x2.Version++;
            x2.Balance = 2;
            await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => db2.SaveChangesAsync());
        }
        await using (var db = new Ledger(options))
        {
            db.Accounts.Add(new Account { Email = "a@x", Opened = DateTime.UtcNow });
            await Assert.ThrowsAsync<DbUpdateException>(() => db.SaveChangesAsync());
        }
    }

    static bool IsRetryable(Exception e) => e switch
    {
        PostgresException p => p.SqlState is "40001" or "40P01",
        MySqlException m => m.ErrorCode is MySqlErrorCode.LockDeadlock or MySqlErrorCode.LockWaitTimeout,
        Microsoft.Data.SqlClient.SqlException s => s.Number is 1205 or 1222,
        _ => e.InnerException is { } inner && IsRetryable(inner),
    };

    [Fact]
    public Task EfCorePostgres() =>
        EfSuite(new DbContextOptionsBuilder<Ledger>().UseNpgsql(PgConn).Options);

    [Fact]
    public Task EfCoreMySql() =>
        EfSuite(new DbContextOptionsBuilder<Ledger>().UseMySql(MyConn, new MySqlServerVersion(new Version(8, 4, 3))).Options);

    static string Ms => Environment.GetEnvironmentVariable("STORINE_MSSQL") ?? "127.0.0.1:51433";

    [Fact]
    public Task EfCoreSqlServer() =>
        EfSuite(new DbContextOptionsBuilder<Ledger>()
            .UseSqlServer($"Server=tcp:{Ms.Replace(':', ',')};Database=shop;User ID=sa;Password=x;Encrypt=False")
            .Options);

    [Fact]
    public async Task TlsBothProtocols()
    {
        await using (var c = new NpgsqlConnection(PgConn + ";SSL Mode=Require"))
        {
            await c.OpenAsync();
            await using var cmd = new NpgsqlCommand("SELECT repeat('n', 70000)", c);
            Assert.Equal(70000, ((string)(await cmd.ExecuteScalarAsync())!).Length);
        }
        await using (var c = new MySqlConnection(MyConn.Replace("SslMode=None", "SslMode=Required")))
        {
            await c.OpenAsync();
            await using var cmd = new MySqlCommand("SELECT 6 * 7", c);
            Assert.Equal(42L, Convert.ToInt64(await cmd.ExecuteScalarAsync()));
        }
    }
}
