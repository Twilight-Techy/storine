using System;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Storine.Tests;

/// <summary>TLS on the native port, with certificates from <c>storage-server gen-ca</c> / <c>gen-cert</c>.</summary>
public class TlsTests
{
    private static void Run(params string[] args)
    {
        var psi = new ProcessStartInfo(Server.Binary) { RedirectStandardOutput = true };
        foreach (var a in args) psi.ArgumentList.Add(a);
        using var p = Process.Start(psi)!;
        p.StandardOutput.ReadToEnd();
        p.WaitForExit();
        Assert.Equal(0, p.ExitCode);
    }

    [Fact]
    public async Task TlsRoundTripAndUntrustedServer()
    {
        if (!Server.Built) return;
        string d = Server.TempDir();
        Run("gen-ca", Path.Combine(d, "ca"));
        Run("gen-cert", Path.Combine(d, "ca"), Path.Combine(d, "node"), "127.0.0.1,localhost");
        Run("gen-ca", Path.Combine(d, "other"));
        int port = Server.FreePort();
        var psi = new ProcessStartInfo(Server.Binary) { RedirectStandardOutput = true, RedirectStandardError = true };
        psi.ArgumentList.Add(Path.Combine(d, "data"));
        psi.ArgumentList.Add($"127.0.0.1:{port}");
        psi.Environment["STORAGE_TLS_CERT"] = Path.Combine(d, "node", "node.crt");
        psi.Environment["STORAGE_TLS_KEY"] = Path.Combine(d, "node", "node.key");
        var server = Process.Start(psi)!;
        server.OutputDataReceived += (_, _) => { };
        server.ErrorDataReceived += (_, _) => { };
        server.BeginOutputReadLine();
        server.BeginErrorReadLine();
        try
        {
            var deadline = DateTime.UtcNow.AddSeconds(10);
            while (true)
            {
                try
                {
                    using var c = new TcpClient();
                    c.Connect("127.0.0.1", port);
                    break;
                }
                catch (SocketException) when (DateTime.UtcNow < deadline)
                {
                    Thread.Sleep(50);
                }
            }
            string big = new string('x', 100_000);
            await using (var db = StorineClient.Connect($"storines://127.0.0.1:{port}",
                             new StorineOptions { TlsCa = Path.Combine(d, "ca", "ca.crt") }))
            {
                await db.Collection("t").PutAsync("k", new Doc { ["v"] = big });
                Assert.Equal(big, (await db.Collection("t").GetAsync("k"))!["v"]);
            }
            await using (var named = StorineClient.Connect(
                             $"storines://127.0.0.1:{port}?tls_ca={Path.Combine(d, "ca", "ca.crt")}&tls_server_name=localhost"))
            {
                Assert.NotNull(await named.Collection("t").GetAsync("k"));
            }
            await using (var other = StorineClient.Connect($"storines://127.0.0.1:{port}",
                             new StorineOptions { TlsCa = Path.Combine(d, "other", "ca.crt"), Timeout = TimeSpan.FromSeconds(1) }))
            {
                await Assert.ThrowsAnyAsync<Exception>(() => other.Collection("t").GetAsync("k"));
            }
        }
        finally
        {
            Server.Stop(server);
            Directory.Delete(d, true);
        }
    }
}
