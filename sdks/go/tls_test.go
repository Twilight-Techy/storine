package storine

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TLS on the native port, with certificates from `storage-server gen-ca` / `gen-cert`.
func TestTLSRoundTripAndUntrustedServer(t *testing.T) {
	bin := serverBinary(t)
	d := t.TempDir()
	for _, args := range [][]string{
		{"gen-ca", filepath.Join(d, "ca")},
		{"gen-cert", filepath.Join(d, "ca"), filepath.Join(d, "node"), "127.0.0.1,localhost"},
		{"gen-ca", filepath.Join(d, "other")},
	} {
		if out, err := exec.Command(bin, args...).CombinedOutput(); err != nil {
			t.Fatalf("%v: %v %s", args, err, out)
		}
	}
	port := freePort(t)
	cmd := exec.Command(bin, filepath.Join(d, "data"), fmt.Sprintf("127.0.0.1:%d", port))
	cmd.Env = append(cmd.Environ(), "STORAGE_TLS_CERT="+filepath.Join(d, "node", "node.crt"), "STORAGE_TLS_KEY="+filepath.Join(d, "node", "node.key"))
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stop(cmd) })
	deadline := time.Now().Add(10 * time.Second)
	for {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server did not start")
		}
		time.Sleep(50 * time.Millisecond)
	}

	db, err := Connect(fmt.Sprintf("storines://127.0.0.1:%d", port), Options{TLSCAFile: filepath.Join(d, "ca", "ca.crt")})
	if err != nil {
		t.Fatal(err)
	}
	big := strings.Repeat("x", 100_000)
	if _, err := db.Collection("t").Put(context.Background(), "k", Doc{"v": big}); err != nil {
		t.Fatal(err)
	}
	got, err := db.Collection("t").Get(context.Background(), "k")
	if err != nil || got == nil || got.Fields["v"] != big {
		t.Fatalf("read back over TLS: %v %v", got, err)
	}
	db.Close()

	named, err := Connect(fmt.Sprintf("storines://127.0.0.1:%d?tls_ca=%s&tls_server_name=localhost", port, filepath.Join(d, "ca", "ca.crt")))
	if err != nil {
		t.Fatal(err)
	}
	if doc, err := named.Collection("t").Get(context.Background(), "k"); err != nil || doc == nil {
		t.Fatalf("named server: %v %v", doc, err)
	}
	named.Close()

	other, err := Connect(fmt.Sprintf("storines://127.0.0.1:%d", port), Options{TLSCAFile: filepath.Join(d, "other", "ca.crt"), Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Collection("t").Get(context.Background(), "k"); err == nil {
		t.Fatal("a server signed by an untrusted CA was accepted")
	}
	other.Close()
}
