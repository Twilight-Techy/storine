package storine

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// Conformance: the SDK reproduces the server's encodings byte for byte (protocol/vectors).

type vecValue struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
	Bits  string          `json:"bits"`
}

type vectors struct {
	Values []struct {
		Name  string   `json:"name"`
		Value vecValue `json:"value"`
		Hex   string   `json:"hex"`
	} `json:"values"`
	Documents []struct {
		Fields vecValue `json:"fields"`
		Hex    string   `json:"hex"`
	} `json:"documents"`
	Requests []struct {
		Name    string `json:"name"`
		BodyHex string `json:"body_hex"`
	} `json:"requests"`
	Responses []struct {
		Name    string `json:"name"`
		BodyHex string `json:"body_hex"`
	} `json:"responses"`
}

func loadVectors(t *testing.T) *vectors {
	raw, err := os.ReadFile(filepath.Join("..", "..", "protocol", "vectors", "vectors.json"))
	if err != nil {
		t.Fatal(err)
	}
	var v vectors
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	return &v
}

func native(t *testing.T, d vecValue) any {
	var s string
	_ = json.Unmarshal(d.Value, &s)
	switch d.Type {
	case "null":
		return nil
	case "bool":
		var b bool
		_ = json.Unmarshal(d.Value, &b)
		return b
	case "int64":
		n, _ := strconv.ParseInt(s, 10, 64)
		return n
	case "uint64":
		n, _ := strconv.ParseUint(s, 10, 64)
		return n
	case "float64":
		bits, _ := strconv.ParseUint(d.Bits, 16, 64)
		return math.Float64frombits(bits)
	case "int128":
		n, _ := new(big.Int).SetString(s, 10)
		return n
	case "timestamp":
		n, _ := strconv.ParseInt(s, 10, 64)
		return time.Unix(0, n).UTC()
	case "uuid":
		u, err := ParseUUID(s)
		if err != nil {
			t.Fatal(err)
		}
		return u
	case "id":
		var id ID
		b, _ := hex.DecodeString(s)
		copy(id[:], b)
		return id
	case "text":
		return s
	case "bytes":
		b, _ := hex.DecodeString(s)
		return b
	case "decimal":
		return MustDecimal(s)
	case "list":
		var items []vecValue
		_ = json.Unmarshal(d.Value, &items)
		out := []any{}
		for _, it := range items {
			out = append(out, native(t, it))
		}
		return out
	case "map":
		var pairs [][2]json.RawMessage
		_ = json.Unmarshal(d.Value, &pairs)
		out := Doc{}
		for _, p := range pairs {
			var k string
			var v vecValue
			_ = json.Unmarshal(p[0], &k)
			_ = json.Unmarshal(p[1], &v)
			out[k] = native(t, v)
		}
		return out
	}
	t.Fatalf("unknown vector type %s", d.Type)
	return nil
}

// comparable renders values so decoded and native forms compare (big ints, decimals, times).
func comparable(v any) any {
	switch x := v.(type) {
	case *big.Int:
		return "int:" + x.String()
	case Decimal:
		return "dec:" + x.String()
	case time.Time:
		return fmt.Sprintf("ts:%d", x.UnixNano())
	case float64:
		return fmt.Sprintf("f:%x", math.Float64bits(x))
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = comparable(e)
		}
		return out
	case map[string]any:
		out := map[string]any{}
		for k, e := range x {
			out[k] = comparable(e)
		}
		return out
	}
	return v
}

func TestValueVectors(t *testing.T) {
	for _, vec := range loadVectors(t).Values {
		t.Run(vec.Name, func(t *testing.T) {
			v := native(t, vec.Value)
			b, err := EncodeValue(v)
			if err != nil {
				t.Fatal(err)
			}
			if got := hex.EncodeToString(b); got != vec.Hex {
				t.Fatalf("encode: got %s want %s", got, vec.Hex)
			}
			raw, _ := hex.DecodeString(vec.Hex)
			back, err := DecodeValue(raw)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(comparable(back), comparable(v)) {
				t.Fatalf("decode: got %#v want %#v", back, v)
			}
		})
	}
}

func TestDocumentVector(t *testing.T) {
	d := loadVectors(t).Documents[0]
	w := &writer{}
	if err := encodeFields(native(t, d.Fields).(Doc), w, 1); err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(w.b); got != d.Hex {
		t.Fatalf("got %s want %s", got, d.Hex)
	}
}

func must(b []byte, err error) []byte {
	if err != nil {
		panic(err)
	}
	return b
}

func TestRequestVectors(t *testing.T) {
	d := Doc{"balance": MustDecimal("100.00"), "owner": "alice", "active": true}
	rid, _ := hex.DecodeString("0102030405060708090a0b0c0d0e0f10")
	ab := make([]byte, 16)
	for i := range ab {
		ab[i] = 0xab
	}
	ws := func(rid []byte, w ...write) []byte { return must(encodeWrites(w, rid)) }
	q := func(c string, f *Filter, order []string, limit, offset uint32, after *string) []byte {
		return must(encodeQuery(c, f, parseOrderBy(order), limit, offset, after))
	}
	after := "k0099"
	admin := func(sub byte, name string, field *string) []byte {
		w := &writer{}
		w.u8(sub)
		w.str(name)
		if field != nil {
			w.str(*field)
		}
		return w.b
	}
	price := "price"
	built := map[string][]byte{
		"get accounts/alice": encodeGet("accounts", "alice"),
		"write: put":         ws(nil, write{collection: "accounts", key: "alice", kind: 1, fields: d}),
		"write: insert (must not exist) with request id": ws(ab, write{collection: "accounts", key: "alice", kind: 1, pre: 1, fields: d}),
		"write: replace if version 7":                    ws(nil, write{collection: "accounts", key: "alice", kind: 1, pre: 3, version: 7, fields: d}),
		"write: put with ttl 1500 ms":                    ws(nil, must2(putWrite("sessions", "s1", Doc{"u": 1}, 1500*time.Millisecond, 0, 0))),
		"write: delete":                                  ws(nil, write{collection: "accounts", key: "alice", kind: 3}),
		"write: delete if exists":                        ws(nil, write{collection: "accounts", key: "alice", kind: 3, pre: 2}),
		"write: transfer 25.00 (two patches, no overdraft) with request id": ws(rid,
			write{collection: "accounts", key: "alice", kind: 4, pre: 2, ops: []FieldOp{Add("balance", MustDecimal("-25.00")).Min(0)}},
			write{collection: "accounts", key: "bob", kind: 4, pre: 2, ops: []FieldOp{Add("balance", MustDecimal("25.00"))}}),
		"write: patch set / unset / append / add with min and max": ws(nil, write{collection: "c", key: "k", kind: 4, pre: 2, ops: []FieldOp{
			Set("profile.name", "Ann"), Unset("tmp"), Append("log", 3), Add("n", 1).Min(0).Max(10)}}),
		"write: check version 3":                      ws(nil, write{collection: "c", key: "k", kind: 5, pre: 3, version: 3}),
		"query: all (default limit)":                  q("items", nil, nil, 0, 0, nil),
		"query: eq text":                              q("items", Eq("kind", "book"), nil, 10, 0, nil),
		"query: and(gte, lt) sorted desc, offset 20":  q("items", And(Ge("price", MustDecimal("10")), Lt("price", MustDecimal("20.50"))), []string{"-price"}, 10, 20, nil),
		"query: between":                              q("items", Between("n", 1, 5), nil, 0, 0, nil),
		"query: in":                                   q("items", In("n", 1, 2), nil, 0, 0, nil),
		"query: prefix / contains / exists / missing": q("items", Or(Prefix("name", "An"), Contains("tags", "x"), Exists("email"), Missing("deleted_at")), nil, 0, 0, nil),
		"query: not / ne / le / gt / key prefix":      q("items", And(Not(Ne("a", 1)), Le("b", 2.5), Gt("c", 0), KeyPrefix("user:")), nil, 0, 0, nil),
		"query: after key cursor":                     q("items", nil, nil, 100, 0, &after),
		"query: multi-key sort":                       q("items", nil, []string{"a", "-b"}, 5, 0, nil),
		"admin: create index items.price":             admin(1, "items", &price),
		"info: collection items":                      admin(6, "items", nil),
		"info: list collections":                      {3},
	}
	seen := 0
	for _, vec := range loadVectors(t).Requests {
		b, ok := built[vec.Name]
		if !ok {
			t.Errorf("no builder for vector %q", vec.Name)
			continue
		}
		seen++
		if got := hex.EncodeToString(b); got != vec.BodyHex {
			t.Errorf("%s:\n got %s\nwant %s", vec.Name, got, vec.BodyHex)
		}
	}
	if seen != len(built) {
		t.Fatalf("matched %d of %d builders", seen, len(built))
	}
}

func must2(w write, err error) write {
	if err != nil {
		panic(err)
	}
	return w
}

func TestResponseVectors(t *testing.T) {
	r := map[string][]byte{}
	for _, v := range loadVectors(t).Responses {
		r[v.Name], _ = hex.DecodeString(v.BodyHex)
	}
	versions, err := decodeWriteResponse(r["write ok: [v12, deleted]"])
	if err != nil || !reflect.DeepEqual(versions, []uint64{12, 0}) {
		t.Fatalf("write response: %v %v", versions, err)
	}
	p, err := decodePage(r["page: total 42, next k1, one doc"])
	if err != nil || p.total != 42 || *p.nextKey != "k1" || len(p.docs) != 1 ||
		p.docs[0].Key != "k1" || p.docs[0].Version != 3 || p.docs[0].Fields["n"] != int64(1) {
		t.Fatalf("page: %+v %v", p, err)
	}
}

func TestHostileBytesNeverPanic(t *testing.T) {
	vals := loadVectors(t).Values
	good, _ := hex.DecodeString(vals[len(vals)-1].Hex)
	for cut := 0; cut < len(good); cut++ {
		if _, err := DecodeValue(good[:cut]); !errors.Is(err, ErrInvalid) {
			t.Fatalf("cut %d: %v", cut, err)
		}
	}
	if _, err := DecodeValue([]byte{0x0C, 0xFF, 0xFF, 0xFF, 0xFF}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestValueMappingRules(t *testing.T) {
	huge := new(big.Int).Lsh(big.NewInt(1), 130)
	if _, err := EncodeValue(huge); !errors.Is(err, ErrInvalid) {
		t.Fatal("2^130 must be rejected")
	}
	if _, err := EncodeValue(struct{}{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("structs must be rejected")
	}
	for _, s := range []string{"NaN", "1e", "", "abc", "1.2.3"} {
		if _, err := ParseDecimal(s); err == nil {
			t.Fatalf("%q must not parse", s)
		}
	}
	if d := MustDecimal("1E+3"); d.String() != "1000" {
		t.Fatal(d)
	}
	for _, s := range []string{"1234567890123456789012345678.0123456789", "-0.001", "0.00", "19.99"} {
		b, _ := EncodeValue(MustDecimal(s))
		back, _ := DecodeValue(b)
		if back.(Decimal).String() != s {
			t.Fatalf("%s round-tripped to %s", s, back)
		}
	}
	if !MustDecimal("1.50").Equal(MustDecimal("1.5")) {
		t.Fatal("1.50 == 1.5")
	}
	// Plain Go collections of any element type encode as lists / maps.
	b, err := EncodeValue(map[string][]int{"a": {1, 2}})
	if err != nil {
		t.Fatal(err)
	}
	back, _ := DecodeValue(b)
	if !reflect.DeepEqual(back, map[string]any{"a": []any{int64(1), int64(2)}}) {
		t.Fatalf("%#v", back)
	}
}
