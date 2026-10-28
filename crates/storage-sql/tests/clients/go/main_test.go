// Real-client compatibility tests for storine-sql (Go drivers and GORM).
//
//	storine-sql --data /tmp/db --pg 127.0.0.1:55432 --mysql 127.0.0.1:53306 &
//	STORINE_PG=127.0.0.1:55432 STORINE_MYSQL=127.0.0.1:53306 go test -v .
package sqlclients

import (
	"context"
	"crypto/tls"
	"database/sql"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	_ "github.com/lib/pq"
	"github.com/shopspring/decimal"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

func addr(env, def string) string {
	if v := os.Getenv(env); v != "" {
		return v
	}
	return def
}

var (
	pgAddr = addr("STORINE_PG", "127.0.0.1:55432")
	myAddr = addr("STORINE_MYSQL", "127.0.0.1:53306")
)

func TestPgx(t *testing.T) {
	ctx := context.Background()
	c, err := pgx.Connect(ctx, "postgres://app:x@"+pgAddr+"/shop?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	must := func(_ any, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(c.Exec(ctx, "DROP TABLE IF EXISTS gx_t"))
	must(c.Exec(ctx, "CREATE TABLE gx_t (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL, amount NUMERIC(12,2), at TIMESTAMPTZ, ok BOOLEAN, tags TEXT[])"))
	at := time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)
	var id int64
	if err := c.QueryRow(ctx, "INSERT INTO gx_t (name, amount, at, ok, tags) VALUES ($1, $2, $3, $4, $5) RETURNING id", "a", "12.50", at, true, []string{"x", "y"}).Scan(&id); err != nil {
		t.Fatal(err)
	}
	var (
		name   string
		amount string
		got    time.Time
		ok     bool
		tags   []string
	)
	if err := c.QueryRow(ctx, "SELECT name, amount::text, at, ok, tags FROM gx_t WHERE id = $1", id).Scan(&name, &amount, &got, &ok, &tags); err != nil {
		t.Fatal(err)
	}
	if name != "a" || amount != "12.50" || !got.Equal(at) || !ok || len(tags) != 2 {
		t.Fatalf("%v %v %v %v %v", name, amount, got, ok, tags)
	}
	tx, err := c.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	must(tx.Exec(ctx, "UPDATE gx_t SET amount = amount + $1 WHERE id = $2", "1.25", id))
	must(nil, tx.Commit(ctx))
	var total decimal.Decimal
	var s string
	if err := c.QueryRow(ctx, "SELECT sum(amount)::text FROM gx_t").Scan(&s); err != nil {
		t.Fatal(err)
	}
	total, _ = decimal.NewFromString(s)
	if !total.Equal(decimal.RequireFromString("13.75")) {
		t.Fatal(total)
	}
}

func TestLibPqConcurrentNoLostUpdates(t *testing.T) {
	db, err := sql.Open("postgres", "postgres://app:x@"+pgAddr+"/shop?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(8)
	if _, err := db.Exec("DROP TABLE IF EXISTS pq_acct"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE pq_acct (id INT PRIMARY KEY, balance NUMERIC(12,2) NOT NULL CHECK (balance >= 0))"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO pq_acct VALUES (1, 100), (2, 0)"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				// Transfer 0.50 from 1 to 2 atomically.
				tx, err := db.Begin()
				if err != nil {
					errs <- err
					return
				}
				if _, err := tx.Exec("UPDATE pq_acct SET balance = balance - 0.50 WHERE id = 1"); err != nil {
					tx.Rollback()
					errs <- err
					return
				}
				if _, err := tx.Exec("UPDATE pq_acct SET balance = balance + 0.50 WHERE id = 2"); err != nil {
					tx.Rollback()
					errs <- err
					return
				}
				if err := tx.Commit(); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	var a, b string
	db.QueryRow("SELECT balance::text FROM pq_acct WHERE id = 1").Scan(&a)
	db.QueryRow("SELECT balance::text FROM pq_acct WHERE id = 2").Scan(&b)
	if a != "0.00" || b != "100.00" {
		t.Fatalf("balances %s %s", a, b)
	}
	// Overdraft is refused by the CHECK constraint, atomically.
	if _, err := db.Exec("UPDATE pq_acct SET balance = balance - 0.01 WHERE id = 1"); err == nil {
		t.Fatal("overdraft accepted")
	}
}

func TestMySQLDriver(t *testing.T) {
	db, err := sql.Open("mysql", "root@tcp("+myAddr+")/shop?parseTime=true")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, q := range []string{
		"DROP TABLE IF EXISTS gm_t",
		"CREATE TABLE gm_t (id INT AUTO_INCREMENT PRIMARY KEY, sku VARCHAR(10) UNIQUE, price DECIMAL(8,2), made DATETIME(6))",
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(q, err)
		}
	}
	made := time.Date(2024, 2, 3, 4, 5, 6, 789000000, time.UTC)
	r, err := db.Exec("INSERT INTO gm_t (sku, price, made) VALUES (?, ?, ?)", "A", "3.50", made)
	if err != nil {
		t.Fatal(err)
	}
	if id, _ := r.LastInsertId(); id != 1 {
		t.Fatal(id)
	}
	var sku, price string
	var got time.Time
	if err := db.QueryRow("SELECT sku, price, made FROM gm_t WHERE id = ?", 1).Scan(&sku, &price, &got); err != nil {
		t.Fatal(err)
	}
	if sku != "A" || price != "3.50" || !got.Equal(made) {
		t.Fatal(sku, price, got)
	}
	if _, err := db.Exec("INSERT INTO gm_t (sku) VALUES (?)", "A"); err == nil {
		t.Fatal("duplicate accepted")
	}
}

type Product struct {
	ID       uint   `gorm:"primaryKey"`
	Code     string `gorm:"uniqueIndex;size:32"`
	Price    decimal.Decimal `gorm:"type:decimal(12,2)"`
	Stock    int
	Reviews  []Review
	Created  time.Time
}

type Review struct {
	ID        uint `gorm:"primaryKey"`
	ProductID uint
	Stars     int
	Body      string
}

func gormSuite(t *testing.T, dial gorm.Dialector) {
	db, err := gorm.Open(dial, &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Migrator().DropTable(&Review{}, &Product{})
	if err := db.AutoMigrate(&Product{}, &Review{}); err != nil {
		t.Fatal(err)
	}
	p := Product{Code: "P1", Price: decimal.RequireFromString("9.99"), Stock: 10, Created: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		Reviews: []Review{{Stars: 5, Body: "great"}, {Stars: 3, Body: "ok"}}}
	if err := db.Create(&p).Error; err != nil {
		t.Fatal(err)
	}
	var got Product
	if err := db.Preload("Reviews").First(&got, "code = ?", "P1").Error; err != nil {
		t.Fatal(err)
	}
	if len(got.Reviews) != 2 || !got.Price.Equal(p.Price) {
		t.Fatalf("%+v", got)
	}
	if err := db.Model(&Product{}).Where("id = ?", p.ID).Update("stock", gorm.Expr("stock - ?", 3)).Error; err != nil {
		t.Fatal(err)
	}
	db.First(&got, p.ID)
	if got.Stock != 7 {
		t.Fatal(got.Stock)
	}
	var avg float64
	db.Model(&Review{}).Select("AVG(stars)").Scan(&avg)
	if avg != 4 {
		t.Fatal(avg)
	}
	// Upsert.
	up := Product{Code: "P1", Price: decimal.RequireFromString("1.00"), Created: time.Now().UTC()}
	conflict := clause.OnConflict{Columns: []clause.Column{{Name: "code"}}, DoUpdates: clause.AssignmentColumns([]string{"price"})}
	if db.Dialector.Name() == "sqlserver" {
		// GORM's SQL Server driver upserts by primary key (a MERGE).
		up.ID = p.ID
		conflict.Columns = []clause.Column{{Name: "id"}}
	}
	if err := db.Clauses(conflict).Create(&up).Error; err != nil {
		t.Fatal(err)
	}
	db.First(&got, "code = ?", "P1")
	if !got.Price.Equal(decimal.RequireFromString("1.00")) {
		t.Fatal(got.Price)
	}
	// Transactions roll back on error.
	_ = db.Transaction(func(tx *gorm.DB) error {
		tx.Model(&Product{}).Where("code = ?", "P1").Update("stock", 0)
		return gorm.ErrInvalidData
	})
	db.First(&got, "code = ?", "P1")
	if got.Stock != 7 {
		t.Fatal("rollback failed", got.Stock)
	}
	if err := db.Migrator().DropTable(&Review{}, &Product{}); err != nil {
		t.Fatal(err)
	}
}

func TestGormPostgres(t *testing.T) {
	gormSuite(t, postgres.Open("host=127.0.0.1 port="+pgAddr[len("127.0.0.1:"):]+" user=app dbname=shop sslmode=disable"))
}

func TestGormMySQL(t *testing.T) {
	gormSuite(t, mysql.Open("root@tcp("+myAddr+")/shop?parseTime=true"))
}

// TLS with Go's own crypto/tls on both protocols (sslmode=require / tls=skip-verify, and
// pgx with the server certificate pinned when STORINE_TLS_CERT names it).
func TestTLS(t *testing.T) {
	ctx := context.Background()
	urls := []string{"postgres://app:x@" + pgAddr + "/shop?sslmode=require"}
	if cert := os.Getenv("STORINE_TLS_CERT"); cert != "" {
		urls = append(urls, "postgres://app:x@localhost:"+pgAddr[len("127.0.0.1:"):]+"/shop?sslmode=verify-full&sslrootcert="+cert)
	}
	for _, u := range urls {
		c, err := pgx.Connect(ctx, u)
		if err != nil {
			t.Fatalf("%s: %v", u, err)
		}
		var s string
		if err := c.QueryRow(ctx, "SELECT repeat('x', 100000)").Scan(&s); err != nil || len(s) != 100000 {
			t.Fatalf("large result over TLS: %v (%d bytes)", err, len(s))
		}
		c.Close(ctx)
	}
	// TLS 1.2 (crypto/tls capped): ECDHE x25519, ChaCha20-Poly1305, Ed25519 signature.
	cfg, err := pgx.ParseConfig("postgres://app:x@" + pgAddr + "/shop?sslmode=require")
	if err != nil {
		t.Fatal(err)
	}
	cfg.TLSConfig.MaxVersion = tls.VersionTLS12
	c12, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("TLS 1.2: %v", err)
	}
	if state := c12.PgConn().Conn().(*tls.Conn).ConnectionState(); state.Version != tls.VersionTLS12 {
		t.Fatalf("negotiated %x, want TLS 1.2", state.Version)
	}
	var big string
	if err := c12.QueryRow(ctx, "SELECT repeat('y', 100000)").Scan(&big); err != nil || len(big) != 100000 {
		t.Fatalf("large result over TLS 1.2: %v (%d bytes)", err, len(big))
	}
	c12.Close(ctx)
	db, err := sql.Open("mysql", "root@tcp("+myAddr+")/shop?tls=skip-verify")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow("SELECT 6 * 7").Scan(&n); err != nil || n != 42 {
		t.Fatalf("mysql over TLS: %v %d", err, n)
	}
}
