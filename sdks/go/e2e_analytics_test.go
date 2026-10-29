package storine

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"testing"
)

// End-to-end analytics (TASK-129) against a real storage-server: expectations are computed here
// from the inserted documents and must match the server exactly, with and without columnar.

type sale struct {
	key    string
	region string
	qty    int64
	price  int64 // cents: the stored value is Decimal(price, 2)
	day    int64
}

func analyticsData() []sale {
	regions := []string{"eu", "us", "apac"}
	out := make([]sale, 0, 3000)
	for i := 0; i < 3000; i++ {
		out = append(out, sale{
			key:    fmt.Sprintf("s%05d", i),
			region: regions[i%3],
			qty:    int64(i%17 + 1),
			price:  int64((i*7919)%10000 + 100),
			day:    int64(i % 60),
		})
	}
	return out
}

func cents(m int64) Decimal { d, _ := NewDecimal(big.NewInt(m), 2); return d }

// avgDecimal is the server's exact mean: 6 more fractional digits, rounded half-even.
func avgDecimal(sum *big.Int, scale uint8, count int64) Decimal {
	scaled := new(big.Int).Mul(sum, big.NewInt(1_000_000))
	c := big.NewInt(count)
	q, r := new(big.Int).QuoRem(scaled, c, new(big.Int))
	twice := new(big.Int).Mul(new(big.Int).Abs(r), big.NewInt(2))
	if cmp := twice.Cmp(c); cmp > 0 || (cmp == 0 && q.Bit(0) == 1) {
		q.Add(q, big.NewInt(int64(scaled.Sign())))
	}
	d, _ := NewDecimal(q, scale+6)
	return d
}

// numeric renders numbers by value (Decimal 12.50 == 12.5, int64 3 == Decimal 3) for exact comparison.
func numeric(v any) any {
	switch x := v.(type) {
	case Decimal:
		r := new(big.Rat).SetFrac(x.Mantissa(), new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(x.Scale())), nil))
		return "n:" + r.RatString()
	case int64:
		return "n:" + big.NewInt(x).String()
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = numeric(e)
		}
		return out
	case [][]any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = numeric(e)
		}
		return out
	}
	return v
}

func assertRows(t *testing.T, what string, got *AggregateResult, want [][]any) {
	t.Helper()
	if !reflect.DeepEqual(numeric(got.Rows), numeric(want)) {
		t.Fatalf("%s:\n got %v\nwant %v", what, got.Rows, want)
	}
}

func TestE2EAnalytics(t *testing.T) {
	db := singleNode(t)
	ctx := context.Background()
	sales := db.Collection("sales")
	data := analyticsData()
	for start := 0; start < len(data); start += 200 {
		_, err := db.Transaction(ctx, func(tx *Tx) error {
			for _, s := range data[start:min(start+200, len(data))] {
				tx.Put("sales", s.key, Doc{"region": s.region, "qty": s.qty, "price": cents(s.price), "day": s.day})
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Expected grouped result, computed from the data.
	type group struct {
		n, qty, minP, maxP int64
		revenue            *big.Int
		days               map[int64]bool
	}
	groups := map[string]*group{}
	for _, s := range data {
		g := groups[s.region]
		if g == nil {
			g = &group{minP: s.price, maxP: s.price, revenue: new(big.Int), days: map[int64]bool{}}
			groups[s.region] = g
		}
		g.n++
		g.qty += s.qty
		g.minP, g.maxP = min(g.minP, s.price), max(g.maxP, s.price)
		g.revenue.Add(g.revenue, big.NewInt(s.price*s.qty))
		g.days[s.day] = true
	}
	var grouped [][]any
	for region, g := range groups {
		rev, _ := NewDecimal(g.revenue, 2)
		grouped = append(grouped, []any{region, g.n, rev, avgDecimal(big.NewInt(g.qty), 0, g.n), cents(g.minP), cents(g.maxP), int64(len(g.days))})
	}
	sort.Slice(grouped, func(i, j int) bool { // -revenue, then the group key
		if c := grouped[i][2].(Decimal).Cmp(grouped[j][2].(Decimal)); c != 0 {
			return c > 0
		}
		return grouped[i][0].(string) < grouped[j][0].(string)
	})
	grouped = grouped[:2]
	groupedQuery := AggregateQuery{
		Aggs: []NamedAgg{
			{"orders", Count()},
			{"revenue", Sum(Field("price").Times(Field("qty")))},
			{"avg_qty", Avg("qty")},
			{"min_price", Min("price")},
			{"max_price", Max("price")},
			{"days", CountDistinct("day")},
		},
		GroupBy: []any{"region"},
		OrderBy: []string{"-revenue"},
		Limit:   2,
	}

	// Weekly buckets of day (width 7, offset 3): sum(qty), count.
	type bucketAcc struct{ qty, n int64 }
	buckets := map[int64]*bucketAcc{}
	for _, s := range data {
		d := s.day - 3
		q := d / 7
		if d%7 != 0 && d < 0 {
			q--
		}
		b := 3 + q*7
		if buckets[b] == nil {
			buckets[b] = &bucketAcc{}
		}
		buckets[b].qty += s.qty
		buckets[b].n++
	}
	var bucketRows [][]any
	for b, a := range buckets {
		bucketRows = append(bucketRows, []any{b, a.qty, a.n})
	}
	sort.Slice(bucketRows, func(i, j int) bool { return bucketRows[i][0].(int64) < bucketRows[j][0].(int64) })
	bucketQuery := AggregateQuery{
		Aggs:    []NamedAgg{{"units", Sum("qty")}, {"n", Count()}},
		GroupBy: []any{Bucket("day", 7, 3)},
		OrderBy: []string{"day"},
	}

	// Filtered totals: region us and day >= 30.
	var fn, fqty int64
	fsum := new(big.Int)
	for _, s := range data {
		if s.region == "us" && s.day >= 30 {
			fn++
			fqty += s.qty
			fsum.Add(fsum, big.NewInt(s.price))
		}
	}
	fsumDec, _ := NewDecimal(fsum, 2)
	filtered := [][]any{{fn, fqty, fsumDec, avgDecimal(fsum, 2, fn)}}
	filteredQuery := AggregateQuery{
		Aggs:  []NamedAgg{{"n", Count()}, {"units", Sum("qty")}, {"total", Sum("price")}, {"avg_price", Avg("price")}},
		Where: And(Eq("region", "us"), Ge("day", 30)),
	}

	runAll := func(t *testing.T, mode string) {
		res, err := sales.Aggregate(ctx, groupedQuery)
		if err != nil {
			t.Fatal(err)
		}
		if res.Matched != 3000 || !reflect.DeepEqual(res.Columns, []string{"region", "orders", "revenue", "avg_qty", "min_price", "max_price", "days"}) {
			t.Fatalf("%s: matched %d, columns %v", mode, res.Matched, res.Columns)
		}
		assertRows(t, mode+" grouped", res, grouped)
		if res.Get(0, "region") != grouped[0][0] {
			t.Fatalf("Get: %v", res.Get(0, "region"))
		}
		res, err = sales.Aggregate(ctx, bucketQuery)
		if err != nil {
			t.Fatal(err)
		}
		assertRows(t, mode+" buckets", res, bucketRows)
		res, err = sales.Aggregate(ctx, filteredQuery)
		if err != nil {
			t.Fatal(err)
		}
		if res.Matched != uint64(fn) {
			t.Fatalf("%s: filtered matched %d want %d", mode, res.Matched, fn)
		}
		assertRows(t, mode+" filtered", res, filtered)
	}

	t.Run("row store", func(t *testing.T) { runAll(t, "row store") })

	t.Run("unknown order column is refused client-side", func(t *testing.T) {
		q := groupedQuery
		q.OrderBy = []string{"-nope"}
		if _, err := sales.Aggregate(ctx, q); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	})

	if err := sales.EnableColumnar(ctx); err != nil {
		t.Fatal(err)
	}
	t.Run("columnar", func(t *testing.T) { runAll(t, "columnar") })

	t.Run("scan columns", func(t *testing.T) {
		var want []sale
		for _, s := range data {
			if s.region == "eu" {
				want = append(want, s)
			}
		}
		i := 0
		err := sales.ScanColumns(ctx, []string{"qty", "price"}, Eq("region", "eu"), 500, func(r *ColumnRow) error {
			if i >= len(want) {
				return fmt.Errorf("extra row %s", r.Key)
			}
			w := want[i]
			if r.Key != w.key || r.Fields["qty"] != w.qty || !r.Fields["price"].(Decimal).Equal(cents(w.price)) ||
				len(r.Values) != 2 || r.Values[0] != w.qty {
				return fmt.Errorf("row %d: got %s %v, want %+v", i, r.Key, r.Values, w)
			}
			i++
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if i != len(want) || len(want) != 1000 {
			t.Fatalf("scanned %d of %d", i, len(want))
		}
		stop := errors.New("stop")
		n := 0
		if err := sales.ScanColumns(ctx, []string{"qty"}, nil, 0, func(*ColumnRow) error { n++; return stop }); err != stop || n != 1 {
			t.Fatal(err, n)
		}
	})

	if err := sales.DisableColumnar(ctx); err != nil {
		t.Fatal(err)
	}
	t.Run("columnar disabled", func(t *testing.T) { runAll(t, "after disable") })
}
