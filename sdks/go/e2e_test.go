package storine

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// End-to-end tests against a real storage-server (single node and a 3-node cluster). The binary
// is $STORINE_SERVER or <repo>/target/release/storage-server (cargo build --release -p storage-server).

func serverBinary(t *testing.T) string {
	p := os.Getenv("STORINE_SERVER")
	if p == "" {
		p, _ = filepath.Abs(filepath.Join("..", "..", "target", "release", "storage-server"))
	}
	if _, err := os.Stat(p); err != nil {
		t.Skip("storage-server binary not built")
	}
	return p
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func startServer(t *testing.T, bin, dir string, port int, extra ...string) *exec.Cmd {
	cmd := exec.Command(bin, append([]string{dir, fmt.Sprintf("127.0.0.1:%d", port)}, extra...)...)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
		if err == nil {
			c.Close()
			return cmd
		}
		if time.Now().After(deadline) {
			cmd.Process.Kill()
			t.Fatalf("server on port %d did not start", port)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func stop(cmd *exec.Cmd) {
	if cmd.ProcessState == nil {
		cmd.Process.Kill()
		cmd.Wait()
	}
}

func singleNode(t *testing.T) *Client {
	bin := serverBinary(t)
	port := freePort(t)
	cmd := startServer(t, bin, t.TempDir(), port)
	t.Cleanup(func() { stop(cmd) })
	db, err := Connect(fmt.Sprintf("storine://127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestE2ESingleNode(t *testing.T) {
	db := singleNode(t)
	ctx := context.Background()

	t.Run("crud, types and versions", func(t *testing.T) {
		c := db.Collection("people")
		uid, _ := ParseUUID("123e4567-e89b-12d3-a456-426614174000")
		doc := Doc{"name": "Ann", "age": int64(37), "score": 9.5, "price": MustDecimal("19.99"),
			"joined": time.Date(2024, 5, 1, 12, 30, 0, 0, time.UTC), "id": uid, "raw": []byte{0, 0xff},
			"tags": []any{"a", "b"}, "address": Doc{"city": "Lagos"}, "nothing": nil, "ok": true}
		v1, err := c.Insert(ctx, "ann", doc)
		if err != nil {
			t.Fatal(err)
		}
		got, err := c.Get(ctx, "ann")
		if err != nil || got.Version != v1 || !reflect.DeepEqual(comparable(got.Fields), comparable(doc)) {
			t.Fatalf("get: %+v %v", got, err)
		}
		if _, err := c.Insert(ctx, "ann", doc); !errors.Is(err, ErrConflict) {
			t.Fatalf("second insert: %v", err)
		}
		v2, err := c.Update(ctx, "ann", Set("address.city", "Abuja"), Unset("nothing"), Append("tags", "c"))
		if err != nil || v2 <= v1 {
			t.Fatal(v2, err)
		}
		got, _ = c.Get(ctx, "ann")
		if got.Fields["address"].(Doc)["city"] != "Abuja" || !reflect.DeepEqual(got.Fields["tags"], []any{"a", "b", "c"}) {
			t.Fatalf("%+v", got.Fields)
		}
		if _, ok := got.Fields["nothing"]; ok {
			t.Fatal("unset field still present")
		}
		_, err = c.ReplaceIfVersion(ctx, "ann", v1, Doc{"name": "x"})
		var e *Error
		if !errors.As(err, &e) || e.Kind != ErrConflict || e.CurrentVersion != v2 {
			t.Fatalf("stale replace: %v", err)
		}
		if _, err := c.ReplaceIfVersion(ctx, "ann", v2, Doc{"name": "Ann B"}); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Update(ctx, "nobody", Set("a", 1)); !errors.Is(err, ErrNotFound) {
			t.Fatalf("update missing: %v", err)
		}
		if ok, err := c.Delete(ctx, "ann"); !ok || err != nil {
			t.Fatal(ok, err)
		}
		if ok, err := c.Delete(ctx, "ann"); ok || err != nil {
			t.Fatal(ok, err)
		}
		if d, err := c.Get(ctx, "ann"); d != nil || err != nil {
			t.Fatal(d, err)
		}
	})

	t.Run("money is exact and never lost", func(t *testing.T) {
		acct := db.Collection("accounts")
		if _, err := acct.Put(ctx, "a", Doc{"balance": MustDecimal("0.00")}); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		errs := make(chan error, 8)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					if _, err := acct.Update(ctx, "a", Add("balance", MustDecimal("0.01"))); err != nil {
						errs <- err
						return
					}
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
		balance := func() string {
			d, _ := acct.Get(ctx, "a")
			return d.Fields["balance"].(Decimal).String()
		}
		if b := balance(); b != "8.00" {
			t.Fatal(b)
		}
		if _, err := acct.Update(ctx, "a", Add("balance", MustDecimal("-8.01")).Min(0)); !errors.Is(err, ErrInvalid) {
			t.Fatalf("overdraft: %v", err)
		}
		if b := balance(); b != "8.00" {
			t.Fatal(b)
		}
	})

	t.Run("transaction is atomic", func(t *testing.T) {
		bank := db.Collection("bank")
		bank.Put(ctx, "alice", Doc{"balance": MustDecimal("50")})
		bank.Put(ctx, "bob", Doc{"balance": MustDecimal("0")})
		transfer := func(amount string) error {
			_, err := db.Transaction(ctx, func(tx *Tx) error {
				tx.Update("bank", "alice", Add("balance", MustDecimal("-"+amount)).Min(0))
				tx.Update("bank", "bob", Add("balance", MustDecimal(amount)))
				return nil
			})
			return err
		}
		if err := transfer("20"); err != nil {
			t.Fatal(err)
		}
		if err := transfer("31"); !errors.Is(err, ErrInvalid) {
			t.Fatalf("overdraft transfer: %v", err)
		}
		a, _ := bank.Get(ctx, "alice")
		b, _ := bank.Get(ctx, "bob")
		if a.Fields["balance"].(Decimal).String() != "30" || b.Fields["balance"].(Decimal).String() != "20" {
			t.Fatal(a.Fields, b.Fields)
		}
		if _, err := db.Transaction(ctx, func(tx *Tx) error {
			tx.Put("bank", "carol", Doc{})
			return errors.New("changed my mind")
		}); err == nil || err.Error() != "changed my mind" {
			t.Fatal(err)
		}
		if d, _ := bank.Get(ctx, "carol"); d != nil {
			t.Fatal("aborted transaction wrote")
		}
		_, err := db.Transaction(ctx, func(tx *Tx) error {
			tx.RequireVersion("bank", "alice", a.Version+100)
			tx.Put("bank", "carol", Doc{})
			return nil
		})
		var e *Error
		if !errors.As(err, &e) || e.Kind != ErrConflict || e.WriteIndex != 0 {
			t.Fatalf("require version: %v", err)
		}
	})

	t.Run("queries, indexes and pagination", func(t *testing.T) {
		items := db.Collection("items")
		for i := 0; i < 120; i++ {
			kind := "odd"
			if i%2 == 0 {
				kind = "even"
			}
			price, _ := NewDecimal(big.NewInt(int64(i)*25), 2)
			if _, err := items.Put(ctx, fmt.Sprintf("i%03d", i), Doc{"n": i, "kind": kind, "price": price}); err != nil {
				t.Fatal(err)
			}
		}
		q := Query{Where: And(Between("n", 10, 19), Eq("kind", "odd")), OrderBy: []string{"-n"}}
		keys := func() []string {
			docs, err := items.Find(ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			var out []string
			for _, d := range docs {
				out = append(out, d.Key)
			}
			return out
		}
		want := []string{"i019", "i017", "i015", "i013", "i011"}
		if got := keys(); !reflect.DeepEqual(got, want) {
			t.Fatal(got)
		}
		for _, f := range []string{"n", "kind"} {
			if err := items.CreateIndex(ctx, f); err != nil {
				t.Fatal(err)
			}
		}
		if got := keys(); !reflect.DeepEqual(got, want) {
			t.Fatal(got)
		}
		info, err := items.Info(ctx)
		if err != nil || !strings.Contains(strings.Join(append(info.Indexes, info.Building...), ","), "n") {
			t.Fatal(info, err)
		}
		if n, _ := items.Count(ctx, Gt("price", MustDecimal("25"))); n != 19 {
			t.Fatal(n)
		}
		if n, _ := items.Count(ctx, Match(Doc{"kind": "even"})); n != 60 {
			t.Fatal(n)
		}
		seen := 0
		if err := items.Scan(ctx, nil, func(*Document) error { seen++; return nil }); err != nil || seen != 120 {
			t.Fatal(seen, err)
		}
		if d, _ := items.First(ctx, Query{OrderBy: []string{"-price"}}); d.Key != "i119" {
			t.Fatal(d.Key)
		}
		names, _ := db.Collections(ctx)
		if !strings.Contains(strings.Join(names, ","), "items") {
			t.Fatal(names)
		}
	})

	t.Run("vector search", func(t *testing.T) {
		c := db.Collection("go_docs")
		if err := c.CreateVectorIndex(ctx, "emb", 3); err != nil {
			t.Fatal(err)
		}
		if vi, err := c.VectorIndexes(ctx); err != nil || !reflect.DeepEqual(vi, []VectorIndex{{Field: "emb", Dims: 3, Ready: true}}) {
			t.Fatal(vi, err)
		}
		for _, x := range []struct {
			key  string
			emb  []any
			kind string
		}{{"a", []any{1.0, 0.0, 0.0}, "x"}, {"b", []any{0.0, 1.0, 0.0}, "y"}, {"c", []any{0.7, 0.7, 0.0}, "x"}} {
			if _, err := c.Put(ctx, x.key, Doc{"emb": x.emb, "kind": x.kind}); err != nil {
				t.Fatal(err)
			}
		}
		keys := func(docs []*Document) []string {
			out := []string{}
			for _, d := range docs {
				out = append(out, d.Key)
			}
			return out
		}
		hits, err := c.Nearest(ctx, "emb", []float32{1, 0.1, 0}, NearestOptions{K: 2})
		if err != nil || !reflect.DeepEqual(keys(hits), []string{"a", "c"}) || !(hits[0].Score() > hits[1].Score()) {
			t.Fatal(keys(hits), err)
		}
		hits, err = c.Nearest(ctx, "emb", []float32{1, 0.1, 0}, NearestOptions{K: 5, Where: Eq("kind", "x"), Exact: true})
		if err != nil || !reflect.DeepEqual(keys(hits), []string{"a", "c"}) {
			t.Fatal(keys(hits), err)
		}
		if _, err := c.Put(ctx, "bad", Doc{"emb": []any{1, 2}}); !errors.Is(err, ErrInvalid) { // wrong dimensions
			t.Fatalf("wrong-dims put: %v", err)
		}
		if _, err := c.Nearest(ctx, "emb", []float32{1, 2}, NearestOptions{K: 1}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("wrong-dims query: %v", err)
		}
		if err := c.DropVectorIndex(ctx, "emb"); err != nil {
			t.Fatal(err)
		}
		if vi, err := c.VectorIndexes(ctx); err != nil || len(vi) != 0 {
			t.Fatal(vi, err)
		}
	})

	t.Run("ttl and errors", func(t *testing.T) {
		s := db.Collection("sessions")
		if _, err := s.PutWithTTL(ctx, "short", Doc{"u": 1}, 200*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		if d, _ := s.Get(ctx, "short"); d == nil {
			t.Fatal("missing before expiry")
		}
		time.Sleep(400 * time.Millisecond)
		if d, _ := s.Get(ctx, "short"); d != nil {
			t.Fatal("present after expiry")
		}
		if _, err := s.Put(ctx, "bad", Doc{"a.b": 1}); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
		if _, err := db.Collection("bad/name").Put(ctx, "k", Doc{}); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := s.Get(cancelled, "x"); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
}

func TestE2EClusterFailover(t *testing.T) {
	bin := serverBinary(t)
	base := t.TempDir()
	ports := []int{freePort(t), freePort(t), freePort(t)}
	var peers, seeds []string
	for i, p := range ports {
		peers = append(peers, fmt.Sprintf("%d=127.0.0.1:%d", i+1, p))
		seeds = append(seeds, fmt.Sprintf("127.0.0.1:%d", p))
	}
	var procs []*exec.Cmd
	for i, p := range ports {
		cmd := startServer(t, bin, filepath.Join(base, fmt.Sprintf("n%d", i+1)), p,
			"--node-id", fmt.Sprint(i+1), "--peers", strings.Join(peers, ","))
		procs = append(procs, cmd)
		t.Cleanup(func() { stop(cmd) })
	}
	// Any seed works: the client follows redirects to the leader.
	db, err := Connect("storine://"+strings.Join(seeds, ","), Options{Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	acct := db.Collection("acct")
	deadline := time.Now().Add(15 * time.Second)
	for {
		_, err := acct.Put(ctx, "a", Doc{"bal": MustDecimal("100")})
		if err == nil {
			break
		}
		if !errors.Is(err, ErrUnavailable) || time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	deposit := func(n int) {
		for i := 0; i < n; i++ {
			if _, err := acct.Update(ctx, "a", Add("bal", MustDecimal("1"))); err != nil {
				t.Fatal(err)
			}
		}
	}
	balance := func() string {
		d, err := acct.Get(ctx, "a")
		if err != nil {
			t.Fatal(err)
		}
		return d.Fields["bal"].(Decimal).String()
	}
	deposit(10)
	if b := balance(); b != "110" {
		t.Fatal(b)
	}
	info, err := db.ClusterInfo(ctx)
	if err != nil || !info.IsLeader {
		t.Fatal(info, err)
	}
	stop(procs[info.NodeID-1])
	// The client fails over to the new leader; every deposit applies exactly once.
	deposit(5)
	if b := balance(); b != "115" {
		t.Fatal(b)
	}
}
