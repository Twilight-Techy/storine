using System.Data;
using Microsoft.Data.SqlClient;
using Xunit;

// Microsoft.Data.SqlClient against the SQL Server (TDS) endpoint.
public class MssqlTests
{
    static string Ms => Environment.GetEnvironmentVariable("STORINE_MSSQL") ?? "127.0.0.1:51433";
    static string Conn(string encryption = "Encrypt=Optional") =>
        $"Server=tcp:{Ms.Replace(':', ',')};Database=shop;User ID=sa;Password=x;{encryption}";

    [Fact]
    public async Task SqlClientCrudTransactionsAndErrors()
    {
        await using var c = new SqlConnection(Conn());
        await c.OpenAsync();
        foreach (var q in new[] {
            "DROP TABLE IF EXISTS cs_acct",
            "CREATE TABLE cs_acct (id INT IDENTITY(1,1) PRIMARY KEY, owner NVARCHAR(40) NOT NULL UNIQUE, balance DECIMAL(12,2) NOT NULL, opened DATETIME2 NULL)",
        })
        {
            await using var cmd = new SqlCommand(q, c);
            await cmd.ExecuteNonQueryAsync();
        }
        foreach (var (owner, bal) in new[] { ("ann", 100.00m), ("bob", 0.00m) })
        {
            await using var ins = new SqlCommand("INSERT INTO cs_acct (owner, balance, opened) OUTPUT INSERTED.id VALUES (@o, @b, @t)", c);
            ins.Parameters.Add("@o", SqlDbType.NVarChar, 40).Value = owner;
            ins.Parameters.Add(new SqlParameter("@b", SqlDbType.Decimal) { Precision = 12, Scale = 2, Value = bal });
            ins.Parameters.Add("@t", SqlDbType.DateTime2).Value = new DateTime(2024, 5, 6, 7, 8, 9, 123);
            Assert.True(Convert.ToInt32(await ins.ExecuteScalarAsync()) > 0);
        }
        // Concurrent transfers in transactions: exact totals.
        await Task.WhenAll(Enumerable.Range(0, 4).Select(async _ =>
        {
            await using var cc = new SqlConnection(Conn());
            await cc.OpenAsync();
            for (var j = 0; j < 10; j++)
            {
                await using var tx = (SqlTransaction)await cc.BeginTransactionAsync();
                try
                {
                    foreach (var (sql, who) in new[] { ("UPDATE cs_acct SET balance = balance - @x WHERE owner = @o", "ann"), ("UPDATE cs_acct SET balance = balance + @x WHERE owner = @o", "bob") })
                    {
                        await using var u = new SqlCommand(sql, cc, tx);
                        u.Parameters.AddWithValue("@x", 1.00m);
                        u.Parameters.AddWithValue("@o", who);
                        Assert.Equal(1, await u.ExecuteNonQueryAsync());
                    }
                    await tx.CommitAsync();
                }
                catch (SqlException)
                {
                    await tx.RollbackAsync();
                    j--;
                }
            }
        }));
        await using (var q = new SqlCommand("SELECT owner, balance, opened FROM cs_acct ORDER BY owner", c))
        await using (var r = await q.ExecuteReaderAsync())
        {
            Assert.True(await r.ReadAsync());
            Assert.Equal(60.00m, r.GetDecimal(1));
            Assert.Equal(new DateTime(2024, 5, 6, 7, 8, 9, 123), r.GetDateTime(2));
            Assert.True(await r.ReadAsync());
            Assert.Equal(40.00m, r.GetDecimal(1));
        }
        // Rollback undoes; a duplicate key is error 2627.
        await using (var tx = (SqlTransaction)await c.BeginTransactionAsync())
        {
            await using var z = new SqlCommand("UPDATE cs_acct SET balance = 0", c, tx);
            await z.ExecuteNonQueryAsync();
            await tx.RollbackAsync();
        }
        await using (var s = new SqlCommand("SELECT SUM(balance) FROM cs_acct", c))
            Assert.Equal(100.00m, (decimal)(await s.ExecuteScalarAsync())!);
        var dup = await Assert.ThrowsAsync<SqlException>(async () =>
        {
            await using var d = new SqlCommand("INSERT INTO cs_acct (owner, balance) VALUES ('ann', 1)", c);
            await d.ExecuteNonQueryAsync();
        });
        Assert.Equal(2627, dup.Number);
    }

    // A JWT signed with the test issuer's key (tests/clients/oidc/test-key.pem), as an OpenID
    // Connect provider would issue it.
    static string MintToken(string keyFile, string extraClaims = "")
    {
        static string B64(byte[] b) => Convert.ToBase64String(b).TrimEnd('=').Replace('+', '-').Replace('/', '_');
        var exp = DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 300;
        var head = B64(System.Text.Encoding.UTF8.GetBytes("{\"alg\":\"RS256\",\"kid\":\"storine-test\",\"typ\":\"JWT\"}"));
        var body = B64(System.Text.Encoding.UTF8.GetBytes(
            $"{{\"iss\":\"https://id.storine.test/realms/test\",\"aud\":\"storine\",\"exp\":{exp},\"sub\":\"u-1\"{extraClaims}}}"));
        using var rsa = System.Security.Cryptography.RSA.Create();
        rsa.ImportFromPem(File.ReadAllText(keyFile));
        var sig = rsa.SignData(System.Text.Encoding.ASCII.GetBytes($"{head}.{body}"),
            System.Security.Cryptography.HashAlgorithmName.SHA256, System.Security.Cryptography.RSASignaturePadding.Pkcs1);
        return $"{head}.{body}.{B64(sig)}";
    }

    static SqlConnection TokenConn(string server, string token) =>
        new($"Server=tcp:{server.Replace(':', ',')};Database=shop;Encrypt=Optional;TrustServerCertificate=true;Connect Timeout=10")
        {
            AccessToken = token,
        };

    // OpenID Connect access tokens (SqlConnection.AccessToken). The main server has no issuer
    // configured and says so; STORINE_MSSQL_AUTH names one started with --oidc-* (run_all.sh).
    [Fact]
    public async Task AccessTokenLogins()
    {
        await using (var off = TokenConn(Ms, "a.b.c"))
        {
            var e = await Assert.ThrowsAsync<SqlException>(() => off.OpenAsync());
            Assert.Contains("not enabled", e.Message);
        }
        var auth = Environment.GetEnvironmentVariable("STORINE_MSSQL_AUTH");
        var key = Environment.GetEnvironmentVariable("STORINE_OIDC_KEY");
        if (auth == null || key == null) return;
        await using (var c = TokenConn(auth, MintToken(key, ",\"preferred_username\":\"ann\"")))
        {
            await c.OpenAsync();
            await using var q = new SqlCommand("SELECT SUSER_NAME()", c);
            Assert.Equal("ann", await q.ExecuteScalarAsync());
        }
        SqlConnection.ClearAllPools();
        await using (var c = TokenConn(auth, MintToken(key, ",\"aud\":\"elsewhere\"")))
        {
            var e = await Assert.ThrowsAsync<SqlException>(() => c.OpenAsync());
            Assert.Contains("audience", e.Message);
        }
    }

    // Kerberos: Integrated Security through the platform's GSSAPI (MIT libgssapi_krb5 on
    // Linux), using the ticket cache from kinit. STORINE_MSSQL_KRB5 names a server started with
    // --kerberos-keytab for MSSQLSvc/localhost:<port> (kerberos/realm.sh, run_all.sh).
    [Fact]
    public async Task KerberosLogin()
    {
        var addr = Environment.GetEnvironmentVariable("STORINE_MSSQL_KRB5");
        if (addr == null) return;
        var port = addr.Split(':')[1];
        await using var c = new SqlConnection(
            $"Server=tcp:localhost,{port};Database=shop;Integrated Security=true;Encrypt=Optional;TrustServerCertificate=true;Connect Timeout=10");
        await c.OpenAsync();
        await using var q = new SqlCommand("SELECT SUSER_NAME()", c);
        Assert.Equal("ann", await q.ExecuteScalarAsync());
    }

    [Fact]
    public async Task SqlClientTls()
    {
        var modes = new List<string> { "Encrypt=Optional", "Encrypt=False", "Encrypt=Mandatory;TrustServerCertificate=true" };
        // TDS 8 strict mode verifies the certificate; on Linux SqlClient trusts it through
        // OpenSSL's store (run with SSL_CERT_FILE naming a bundle that includes server.crt; it
        // ignores ServerCertificate).
        var cert = Environment.GetEnvironmentVariable("STORINE_TLS_CERT");
        if (cert != null && Environment.GetEnvironmentVariable("SSL_CERT_FILE") is { } bundle
            && File.ReadAllText(bundle).Contains(File.ReadAllText(cert).Trim()))
            modes.Add("Encrypt=Strict;HostNameInCertificate=localhost");
        var big = new string('x', 100_000);
        foreach (var mode in modes)
        {
            try
            {
                await using var c = new SqlConnection(Conn(mode));
                await c.OpenAsync();
                await using var q = new SqlCommand("SELECT @s + 'y'", c);
                q.Parameters.Add("@s", SqlDbType.NVarChar, -1).Value = big;
                Assert.Equal(big + "y", (string)(await q.ExecuteScalarAsync())!);
            }
            catch (Exception e)
            {
                throw new Exception(mode, e);
            }
        }
    }
}
