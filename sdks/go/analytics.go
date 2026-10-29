package storine

import (
	"context"
	"strings"
)

const (
	opDocAggregate   = 0x29
	opDocScanColumns = 0x2A

	maxAnalyticItems = 64
	maxScanPageSize  = 100_000
)

// Expr is an argument expression of an aggregate: Field("price").Times(Field("qty")).
// Build them with Field and Lit and combine with Plus, Minus, Times and Neg.
type Expr struct {
	kind  byte // 1 field, 2 const, 3 add, 4 sub, 5 mul, 6 neg
	path  string
	value any
	a, b  *Expr
}

// Field is the value of the field at path (dotted for nested maps).
func Field(path string) Expr { return Expr{kind: 1, path: path} }

// Lit is a constant: Lit(1), Lit(MustDecimal("2.5")).
func Lit(value any) Expr { return Expr{kind: 2, value: value} }

func binExpr(kind byte, a, b Expr) Expr { return Expr{kind: kind, a: &a, b: &b} }

// Plus is e + o (mixed kinds follow the server's exact add rules).
func (e Expr) Plus(o Expr) Expr { return binExpr(3, e, o) }

// Minus is e - o.
func (e Expr) Minus(o Expr) Expr { return binExpr(4, e, o) }

// Times is e * o.
func (e Expr) Times(o Expr) Expr { return binExpr(5, e, o) }

// Neg is -e.
func (e Expr) Neg() Expr { return Expr{kind: 6, a: &e} }

func encodeExpr(e *Expr, w *writer, depth int) error {
	if depth > maxDepth {
		return invalidf("expression nesting is too deep (max 32)")
	}
	switch e.kind {
	case 1:
		w.u8(1)
		w.str(e.path)
	case 2:
		w.u8(2)
		return encodeValue(e.value, w, 0)
	case 3, 4, 5:
		w.u8(e.kind)
		if err := encodeExpr(e.a, w, depth+1); err != nil {
			return err
		}
		return encodeExpr(e.b, w, depth+1)
	case 6:
		w.u8(6)
		return encodeExpr(e.a, w, depth+1)
	default:
		return invalidf("unknown expression (build them with Field, Lit, Plus, ...)")
	}
	return nil
}

// Agg is one aggregate function: Count(), Sum("qty"), Avg(Field("price").Times(Field("qty"))), ...
// Arguments are a field path (string, never a text constant: use Lit("...")) or an Expr.
type Agg struct {
	fn  byte // 1 count, 2 sum, 3 avg, 4 min, 5 max, 6 count_distinct, 7 approx_count_distinct
	arg *Expr
	err error
}

func aggOf(fn byte, arg any) Agg {
	switch x := arg.(type) {
	case string:
		e := Field(x)
		return Agg{fn: fn, arg: &e}
	case Expr:
		return Agg{fn: fn, arg: &x}
	case *Expr:
		if x != nil {
			return Agg{fn: fn, arg: x}
		}
	}
	return Agg{fn: fn, err: invalidf("aggregate arguments are a field path (string) or an Expr, not %T", arg)}
}

// Count counts the matching rows (of each group).
func Count() Agg { return Agg{fn: 1} }

// CountOf counts the non-null values of arg.
func CountOf(arg any) Agg { return aggOf(1, arg) }

// Sum adds arg exactly (integers and decimals never round; floats are correctly rounded),
// skipping non-numeric values (null when there are none).
func Sum(arg any) Agg { return aggOf(2, arg) }

// Avg is the mean of arg: a Decimal (6 more fractional digits) for exact inputs, else a float64.
func Avg(arg any) Agg { return aggOf(3, arg) }

// Min is the smallest value of arg in the total sort order.
func Min(arg any) Agg { return aggOf(4, arg) }

// Max is the largest value of arg in the total sort order.
func Max(arg any) Agg { return aggOf(5, arg) }

// CountDistinct is the exact number of distinct values of arg.
func CountDistinct(arg any) Agg { return aggOf(6, arg) }

// ApproxCountDistinct estimates the number of distinct values of arg (HyperLogLog, ~0.8 % error).
func ApproxCountDistinct(arg any) Agg { return aggOf(7, arg) }

// NamedAgg names an aggregate's output column: NamedAgg{"orders", Count()}.
type NamedAgg struct {
	Name string
	Agg  Agg
}

// GroupKey is a bucketed group key built with Bucket; plain field paths (strings) group by value.
type GroupKey struct {
	path          string
	width, offset int64
}

// Bucket groups by offset + floor((v - offset) / width) * width of the field at path; its
// column is named path.
func Bucket(path string, width, offset int64) GroupKey {
	return GroupKey{path: path, width: width, offset: offset}
}

// AggregateQuery is what Aggregate computes. Result columns are the group keys (named by their
// path) followed by the aggregates (named by NamedAgg.Name), in order.
type AggregateQuery struct {
	Aggs    []NamedAgg
	Where   *Filter  // nil: every document
	GroupBy []any    // field paths (string) and/or Bucket(...); none: one row of totals
	OrderBy []string // output column names: "revenue" ascending, "-revenue" descending
	Limit   int      // 0: every group
	// MemoryBudget for group tables before spilling to disk (0: server default, 256 MiB).
	MemoryBudget uint64
	// SpillBudget caps the bytes spilled (0: server default, 16 GiB).
	SpillBudget uint64
}

// columns returns the output column names: group keys first, then aggregates.
func (q *AggregateQuery) columns() ([]string, error) {
	var cols []string
	for _, k := range q.GroupBy {
		switch x := k.(type) {
		case string:
			cols = append(cols, x)
		case GroupKey:
			cols = append(cols, x.path)
		default:
			return nil, invalidf("group keys are field paths (string) or Bucket(...), not %T", k)
		}
	}
	for _, a := range q.Aggs {
		if a.Name == "" {
			return nil, invalidf("every aggregate needs a name")
		}
		cols = append(cols, a.Name)
	}
	seen := map[string]bool{}
	for _, c := range cols {
		if seen[c] {
			return nil, invalidf("duplicate output column %q", c)
		}
		seen[c] = true
	}
	return cols, nil
}

func encodeAggregate(collection string, q AggregateQuery) ([]byte, []string, error) {
	if len(q.GroupBy) > maxAnalyticItems || len(q.Aggs) > maxAnalyticItems || len(q.OrderBy) > maxAnalyticItems {
		return nil, nil, invalidf("at most 64 group keys, aggregates and order columns")
	}
	if q.Limit < 0 {
		return nil, nil, invalidf("Limit must not be negative")
	}
	cols, err := q.columns()
	if err != nil {
		return nil, nil, err
	}
	w := &writer{}
	w.str(collection)
	if q.Where == nil {
		w.u8(0)
	} else {
		w.u8(1)
		if err := encodeFilter(q.Where, w, 0); err != nil {
			return nil, nil, err
		}
	}
	w.u32(uint32(len(q.GroupBy)))
	for _, k := range q.GroupBy {
		switch x := k.(type) {
		case string:
			w.u8(1)
			w.str(x)
		case GroupKey:
			if x.width <= 0 {
				return nil, nil, invalidf("bucket width must be positive")
			}
			w.u8(2)
			w.str(x.path)
			w.u64(uint64(x.width))
			w.u64(uint64(x.offset))
		}
	}
	w.u32(uint32(len(q.Aggs)))
	for _, a := range q.Aggs {
		if a.Agg.err != nil {
			return nil, nil, a.Agg.err
		}
		if a.Agg.fn < 1 || a.Agg.fn > 7 {
			return nil, nil, invalidf("unknown aggregate %q (build them with Count, Sum, ...)", a.Name)
		}
		w.u8(a.Agg.fn)
		if a.Agg.arg == nil {
			if a.Agg.fn != 1 {
				return nil, nil, invalidf("aggregate %q needs an argument", a.Name)
			}
			w.u8(0)
			continue
		}
		w.u8(1)
		if err := encodeExpr(a.Agg.arg, w, 0); err != nil {
			return nil, nil, err
		}
	}
	w.u32(uint32(len(q.OrderBy)))
	for _, o := range parseOrderBy(q.OrderBy) {
		idx := -1
		for i, c := range cols {
			if c == o.path {
				idx = i
				break
			}
		}
		if idx < 0 {
			return nil, nil, invalidf("cannot order by %q: not an output column (one of %s)", o.path, strings.Join(cols, ", "))
		}
		w.u32(uint32(idx))
		if o.desc {
			w.u8(1)
		} else {
			w.u8(0)
		}
	}
	w.u32(uint32(q.Limit))
	w.u64(q.MemoryBudget)
	w.u64(q.SpillBudget)
	return w.b, cols, nil
}

// AggregateResult is the result of Aggregate: one row per group, sorted by the order columns
// then the group keys.
type AggregateResult struct {
	Columns         []string // group keys, then aggregates
	Rows            [][]any  // each row has one value per column
	Matched         uint64   // documents that matched the filter
	PrunedRowGroups uint64   // column row groups skipped by zone maps without reading them
	SpilledBytes    uint64   // > 0 when the query outgrew its memory budget and spilled
}

// Column returns the index of the named column, or -1.
func (r *AggregateResult) Column(name string) int {
	for i, c := range r.Columns {
		if c == name {
			return i
		}
	}
	return -1
}

// Get returns the value of column name in row i (nil if there is no such column).
func (r *AggregateResult) Get(i int, name string) any {
	if c := r.Column(name); c >= 0 && c < len(r.Rows[i]) {
		return r.Rows[i][c]
	}
	return nil
}

// Maps returns each row as a column name -> value map.
func (r *AggregateResult) Maps() []Doc {
	out := make([]Doc, len(r.Rows))
	for i, row := range r.Rows {
		m := make(Doc, len(row))
		for j, v := range row {
			if j < len(r.Columns) {
				m[r.Columns[j]] = v
			}
		}
		out[i] = m
	}
	return out
}

func decodeAggregateResult(body []byte) (*AggregateResult, error) {
	r := &reader{b: body}
	res := &AggregateResult{}
	var err error
	for _, p := range []*uint64{&res.Matched, &res.PrunedRowGroups, &res.SpilledBytes} {
		if *p, err = r.u64(); err != nil {
			return nil, err
		}
	}
	cols, err := r.u32()
	if err != nil {
		return nil, err
	}
	n, err := r.count(int(max(cols, 1)))
	if err != nil {
		return nil, err
	}
	res.Rows = make([][]any, 0, n)
	for i := 0; i < n; i++ {
		row := make([]any, cols)
		for j := range row {
			if row[j], err = r.value(0); err != nil {
				return nil, err
			}
		}
		res.Rows = append(res.Rows, row)
	}
	if r.remaining() != 0 {
		return nil, invalidf("trailing bytes after aggregate result")
	}
	return res, nil
}

// Aggregate filters, groups and aggregates the collection on the server, exactly and as of one
// snapshot (vectorized over column segments when columnar is enabled, else over the row store).
//
//	res, err := sales.Aggregate(ctx, storine.AggregateQuery{
//		Aggs: []storine.NamedAgg{
//			{"orders", storine.Count()},
//			{"revenue", storine.Sum(storine.Field("price").Times(storine.Field("qty")))},
//		},
//		Where:   storine.Ge("day", 20),
//		GroupBy: []any{"region"},
//		OrderBy: []string{"-revenue"},
//		Limit:   10,
//	})
//
// Sum and Avg skip non-numeric values (null when none are numeric). Inside expressions a string
// is always a field path; use Lit("...") for a text constant. ErrInvalid for a bad query (an
// unknown order column is caught before sending) or numeric overflow; ErrOverloaded only once
// retries with backoff are exhausted.
func (c *Collection) Aggregate(ctx context.Context, q AggregateQuery) (*AggregateResult, error) {
	body, cols, err := encodeAggregate(c.name, q)
	if err != nil {
		return nil, err
	}
	_, rbody, err := c.client.call(ctx, opDocAggregate, body)
	if err != nil {
		return nil, err
	}
	res, err := decodeAggregateResult(rbody)
	if err != nil {
		return nil, err
	}
	if len(res.Rows) > 0 && len(res.Rows[0]) != len(cols) {
		return nil, newError(ErrServer, 0, "aggregate result has an unexpected number of columns")
	}
	res.Columns = cols
	return res, nil
}

// EnableColumnar also keeps the collection as column segments (built in the background) so
// Aggregate and ScanColumns run vectorized over them. Results are the same either way.
func (c *Collection) EnableColumnar(ctx context.Context) error {
	_, _, err := c.admin(ctx, opDocAdmin, 10, nil)
	return err
}

// DisableColumnar deletes the column segments; analytics then read the row store.
func (c *Collection) DisableColumnar(ctx context.Context) error {
	_, _, err := c.admin(ctx, opDocAdmin, 11, nil)
	return err
}

func encodeScanColumns(collection string, where *Filter, fields []string, after *string, limit int) ([]byte, error) {
	if len(fields) > maxAnalyticItems {
		return nil, invalidf("a column scan reads at most 64 fields")
	}
	if limit < 0 || limit > maxScanPageSize {
		return nil, invalidf("page size must be between 0 and %d", maxScanPageSize)
	}
	w := &writer{}
	w.str(collection)
	if where == nil {
		w.u8(0)
	} else {
		w.u8(1)
		if err := encodeFilter(where, w, 0); err != nil {
			return nil, err
		}
	}
	w.u32(uint32(len(fields)))
	for _, f := range fields {
		w.str(f)
	}
	if after == nil {
		w.u8(0)
	} else {
		w.u8(1)
		w.str(*after)
	}
	w.u32(uint32(limit))
	return w.b, nil
}

type scanRow struct {
	key    string
	values []any
}

type scanPage struct {
	total   uint64
	nextKey *string
	fields  int
	rows    []scanRow
}

func decodeScanPage(body []byte) (*scanPage, error) {
	r := &reader{b: body}
	total, err := r.u64()
	if err != nil {
		return nil, err
	}
	p := &scanPage{total: total}
	has, err := r.u8()
	if err != nil {
		return nil, err
	}
	if has != 0 {
		k, err := r.str()
		if err != nil {
			return nil, err
		}
		p.nextKey = &k
	}
	nf, err := r.u32()
	if err != nil {
		return nil, err
	}
	p.fields = int(nf)
	n, err := r.count(4 + p.fields)
	if err != nil {
		return nil, err
	}
	for i := 0; i < n; i++ {
		key, err := r.str()
		if err != nil {
			return nil, err
		}
		vals := make([]any, nf)
		for j := range vals {
			if vals[j], err = r.value(0); err != nil {
				return nil, err
			}
		}
		p.rows = append(p.rows, scanRow{key, vals})
	}
	if r.remaining() != 0 {
		return nil, invalidf("trailing bytes after scan page")
	}
	return p, nil
}

// ColumnRow is one document read by ScanColumns.
type ColumnRow struct {
	Key    string
	Values []any // one per requested field, in order (nil for a missing field)
	Fields Doc   // requested field path -> value
}

// ScanColumns calls fn with the requested fields (dotted paths; at most 64) of every document
// matching where (nil: all), in key order, fetching pageSize rows per request (0: 1,000; at most
// 100,000) without transferring whole documents. Returning an error from fn stops the scan and
// returns that error.
func (c *Collection) ScanColumns(ctx context.Context, fields []string, where *Filter, pageSize int, fn func(*ColumnRow) error) error {
	var after *string
	for {
		body, err := encodeScanColumns(c.name, where, fields, after, pageSize)
		if err != nil {
			return err
		}
		_, rbody, err := c.client.call(ctx, opDocScanColumns, body)
		if err != nil {
			return err
		}
		p, err := decodeScanPage(rbody)
		if err != nil {
			return err
		}
		if p.fields != len(fields) {
			return newError(ErrServer, 0, "scan page has an unexpected number of fields")
		}
		for _, row := range p.rows {
			m := make(Doc, len(fields))
			for i, f := range fields {
				m[f] = row.values[i]
			}
			if err := fn(&ColumnRow{Key: row.key, Values: row.values, Fields: m}); err != nil {
				return err
			}
		}
		if p.nextKey == nil {
			return nil
		}
		after = p.nextKey
	}
}
