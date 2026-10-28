package sqlclients

import (
	"database/sql"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	"gorm.io/driver/sqlserver"
)

var msAddr = addr("STORINE_MSSQL", "127.0.0.1:51433")

// go-mssqldb (pure Go TDS): parameters, transactions, concurrent increments, errors.
func TestMSSQL(t *testing.T) {
	host, port, _ := strings.Cut(msAddr, ":")
	db, err := sql.Open("sqlserver", "sqlserver://sa:x@"+host+":"+port+"?database=shop&encrypt=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, q := range []string{
		"DROP TABLE IF EXISTS go_ms",
		"CREATE TABLE go_ms (id INT IDENTITY(1,1) PRIMARY KEY, name NVARCHAR(50) NOT NULL UNIQUE, amt DECIMAL(12,2) NOT NULL DEFAULT 0, at DATETIME2 NULL)",
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	at := time.Date(2024, 3, 4, 5, 6, 7, 0, time.UTC)
	if _, err := db.Exec("INSERT INTO go_ms (name, amt, at) VALUES (@p1, @p2, @p3)", "ann", "10.50", at); err != nil {
		t.Fatal(err)
	}
	var (
		name string
		amt  string
		got  time.Time
	)
	if err := db.QueryRow("SELECT name, amt, at FROM go_ms WHERE name = @name", sql.Named("name", "ann")).Scan(&name, &amt, &got); err != nil {
		t.Fatal(err)
	}
	if name != "ann" || amt != "10.50" || !got.Equal(at) {
		t.Fatalf("got %q %q %v", name, amt, got)
	}
	// Concurrent atomic increments: exact.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				if _, err := db.Exec("UPDATE go_ms SET amt = amt + 1 WHERE name = @p1", "ann"); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	if err := db.QueryRow("SELECT amt FROM go_ms WHERE name = 'ann'").Scan(&amt); err != nil || amt != "210.50" {
		t.Fatalf("increments: %v %s", err, amt)
	}
	// Transactions.
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("UPDATE go_ms SET amt = 0 WHERE name = @p1", "ann"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT amt FROM go_ms WHERE name = 'ann'").Scan(&amt); err != nil || amt != "210.50" {
		t.Fatalf("rollback: %v %s", err, amt)
	}
	// Unique violation surfaces as SQL Server error 2627.
	_, err = db.Exec("INSERT INTO go_ms (name) VALUES (@p1)", "ann")
	if err == nil || !strings.Contains(err.Error(), "Violation of UNIQUE KEY constraint") {
		t.Fatalf("duplicate accepted: %v", err)
	}
	var n int64
	var nullable sql.NullTime
	if err := db.QueryRow("SELECT COUNT(*), MAX(at) FROM go_ms WHERE at IS NULL").Scan(&n, &nullable); err != nil || n != 0 || nullable.Valid {
		t.Fatalf("nulls: %v %d %v", err, n, nullable)
	}
}

// Every encryption mode: login-only (encrypt=false), full TLS inside TDS 7.4 (encrypt=true),
// TDS 8 strict, and certificate verification when STORINE_TLS_CERT names the server's.
func TestMSSQLTLS(t *testing.T) {
	host, port, _ := strings.Cut(msAddr, ":")
	modes := []string{"false&TrustServerCertificate=true", "true&TrustServerCertificate=true"}
	if cert := os.Getenv("STORINE_TLS_CERT"); cert != "" {
		// Strict mode always verifies the certificate. go-mssqldb wants a .pem name.
		pem, err := os.ReadFile(cert)
		if err != nil {
			t.Fatal(err)
		}
		cert = t.TempDir() + "/server.pem"
		if err := os.WriteFile(cert, pem, 0o600); err != nil {
			t.Fatal(err)
		}
		pin := "&certificate=" + url.QueryEscape(cert) + "&hostNameInCertificate=localhost"
		modes = append(modes, "true"+pin, "strict"+pin)
	}
	for _, mode := range modes {
		db, err := sql.Open("sqlserver", "sqlserver://sa:x@"+host+":"+port+"?database=shop&encrypt="+mode)
		if err != nil {
			t.Fatal(err)
		}
		var s string
		big := strings.Repeat("x", 100_000)
		if err := db.QueryRow("SELECT @p1 + 'y'", big).Scan(&s); err != nil || s != big+"y" {
			db.Close()
			t.Fatalf("encrypt=%s: %v (%d bytes)", mode, err, len(s))
		}
		db.Close()
	}
}

func TestGormSQLServer(t *testing.T) {
	host, port, _ := strings.Cut(msAddr, ":")
	gormSuite(t, sqlserver.Open("sqlserver://sa:x@"+host+":"+port+"?database=shop&encrypt=disable"))
}
