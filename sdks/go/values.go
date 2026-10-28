package storine

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

// Doc is a document's fields: a plain map of native Go values.
type Doc = map[string]any

const (
	tagNull byte = iota
	tagBool
	tagInt64
	tagUint64
	tagFloat64
	tagInt128
	tagTimestamp
	tagUUID
	tagID
	tagText
	tagBytes
	tagDecimal
	tagList
	tagMap
)

const (
	maxDepth = 32
	maxScale = 38
)

var (
	i64Min  = big.NewInt(math.MinInt64)
	i64Max  = big.NewInt(math.MaxInt64)
	i128Min = new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 127))
	i128Max = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 127), big.NewInt(1))
	two128  = new(big.Int).Lsh(big.NewInt(1), 128)
)

// Decimal is an exact decimal number (money): Mantissa / 10^Scale, up to 38 digits.
// It is never converted to or from a binary float implicitly.
type Decimal struct {
	mantissa *big.Int
	scale    uint8
}

var decimalRe = regexp.MustCompile(`^\s*([+-]?)(\d*)(?:\.(\d*))?(?:[eE]([+-]?\d+))?\s*$`)

// ParseDecimal parses "19.99", "-0.001", "1E+3". The scale is the number of fractional digits written.
func ParseDecimal(s string) (Decimal, error) {
	m := decimalRe.FindStringSubmatch(s)
	if m == nil || (m[2] == "" && m[3] == "") {
		return Decimal{}, invalidf("not a decimal number: %q", s)
	}
	digits, ok := new(big.Int).SetString(defaultStr(m[2]+m[3], "0"), 10)
	if !ok {
		return Decimal{}, invalidf("not a decimal number: %q", s)
	}
	exp := 0
	if m[4] != "" {
		if _, err := fmt.Sscan(m[4], &exp); err != nil {
			return Decimal{}, invalidf("bad exponent in %q", s)
		}
	}
	scale := len(m[3]) - exp
	if digits.Sign() == 0 {
		return Decimal{mantissa: new(big.Int), scale: uint8(min(len(m[3]), maxScale))}, nil
	}
	if scale < -40 || scale > 200 {
		return Decimal{}, invalidf("decimal out of range: %q", s)
	}
	ten := big.NewInt(10)
	for scale < 0 {
		digits.Mul(digits, ten)
		scale++
	}
	for scale > maxScale && new(big.Int).Mod(digits, ten).Sign() == 0 {
		digits.Quo(digits, ten)
		scale--
	}
	if scale > maxScale {
		return Decimal{}, invalidf("decimals have at most 38 fractional digits: %q", s)
	}
	if m[1] == "-" {
		digits.Neg(digits)
	}
	return NewDecimal(digits, uint8(scale))
}

// MustDecimal is ParseDecimal that panics on invalid input (for literals in code).
func MustDecimal(s string) Decimal {
	d, err := ParseDecimal(s)
	if err != nil {
		panic(err)
	}
	return d
}

// NewDecimal returns mantissa / 10^scale.
func NewDecimal(mantissa *big.Int, scale uint8) (Decimal, error) {
	if scale > maxScale {
		return Decimal{}, invalidf("decimal scale must be at most 38")
	}
	if mantissa.Cmp(i128Min) < 0 || mantissa.Cmp(i128Max) > 0 {
		return Decimal{}, invalidf("decimal does not fit in 38 digits")
	}
	return Decimal{mantissa: new(big.Int).Set(mantissa), scale: scale}, nil
}

// Mantissa returns a copy of the unscaled value.
func (d Decimal) Mantissa() *big.Int {
	if d.mantissa == nil {
		return new(big.Int)
	}
	return new(big.Int).Set(d.mantissa)
}

// Scale is the number of fractional digits.
func (d Decimal) Scale() uint8 { return d.scale }

// String returns plain notation with exactly Scale fractional digits.
func (d Decimal) String() string {
	m := d.Mantissa()
	neg := m.Sign() < 0
	s := new(big.Int).Abs(m).String()
	if d.scale > 0 {
		if len(s) <= int(d.scale) {
			s = strings.Repeat("0", int(d.scale)-len(s)+1) + s
		}
		s = s[:len(s)-int(d.scale)] + "." + s[len(s)-int(d.scale):]
	}
	if neg {
		return "-" + s
	}
	return s
}

// Cmp compares numerically: -1, 0 or 1 (1.50 equals 1.5).
func (d Decimal) Cmp(o Decimal) int {
	s := max(d.scale, o.scale)
	a := new(big.Int).Mul(d.Mantissa(), new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(s-d.scale)), nil))
	b := new(big.Int).Mul(o.Mantissa(), new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(s-o.scale)), nil))
	return a.Cmp(b)
}

// Equal reports numeric equality.
func (d Decimal) Equal(o Decimal) bool { return d.Cmp(o) == 0 }

// UUID is a 16-byte UUID in RFC 4122 byte order.
type UUID [16]byte

// ParseUUID parses "123e4567-e89b-12d3-a456-426614174000".
func ParseUUID(s string) (UUID, error) {
	h := strings.ReplaceAll(s, "-", "")
	var u UUID
	if len(h) != 32 {
		return u, invalidf("not a UUID: %q", s)
	}
	for i := 0; i < 16; i++ {
		var b byte
		if _, err := fmt.Sscanf(h[2*i:2*i+2], "%02x", &b); err != nil {
			return u, invalidf("not a UUID: %q", s)
		}
		u[i] = b
	}
	return u, nil
}

func (u UUID) String() string {
	h := fmt.Sprintf("%x", u[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// ID is an engine id value (24 raw bytes).
type ID [24]byte

type writer struct{ b []byte }

func (w *writer) u8(v byte)    { w.b = append(w.b, v) }
func (w *writer) u32(v uint32) { w.b = binary.LittleEndian.AppendUint32(w.b, v) }
func (w *writer) u64(v uint64) { w.b = binary.LittleEndian.AppendUint64(w.b, v) }
func (w *writer) str(s string) {
	w.u32(uint32(len(s)))
	w.b = append(w.b, s...)
}

func (w *writer) i128(v *big.Int) {
	u := new(big.Int).Set(v)
	if u.Sign() < 0 {
		u.Add(u, two128)
	}
	var buf [16]byte
	u.FillBytes(buf[:])
	for i := 15; i >= 0; i-- { // big-endian -> little-endian
		w.b = append(w.b, buf[i])
	}
}

func encodeValue(v any, w *writer, depth int) error {
	if depth > maxDepth {
		return invalidf("value nesting is too deep (max 32)")
	}
	switch x := v.(type) {
	case nil:
		w.u8(tagNull)
	case bool:
		w.u8(tagBool)
		if x {
			w.u8(1)
		} else {
			w.u8(0)
		}
	case int:
		w.u8(tagInt64)
		w.u64(uint64(int64(x)))
	case int8:
		return encodeValue(int64(x), w, depth)
	case int16:
		return encodeValue(int64(x), w, depth)
	case int32:
		return encodeValue(int64(x), w, depth)
	case int64:
		w.u8(tagInt64)
		w.u64(uint64(x))
	case uint8:
		return encodeValue(int64(x), w, depth)
	case uint16:
		return encodeValue(int64(x), w, depth)
	case uint32:
		return encodeValue(int64(x), w, depth)
	case uint:
		w.u8(tagUint64)
		w.u64(uint64(x))
	case uint64:
		w.u8(tagUint64)
		w.u64(x)
	case float32:
		return encodeValue(float64(x), w, depth)
	case float64:
		w.u8(tagFloat64)
		w.u64(math.Float64bits(x))
	case *big.Int:
		if x == nil {
			w.u8(tagNull)
		} else if x.Cmp(i64Min) >= 0 && x.Cmp(i64Max) <= 0 {
			w.u8(tagInt64)
			w.u64(uint64(x.Int64()))
		} else if x.Cmp(i128Min) >= 0 && x.Cmp(i128Max) <= 0 {
			w.u8(tagInt128)
			w.i128(x)
		} else {
			return invalidf("integer does not fit in 128 bits")
		}
	case Decimal:
		w.u8(tagDecimal)
		w.i128(x.Mantissa())
		w.u8(x.scale)
	case *Decimal:
		if x == nil {
			w.u8(tagNull)
		} else {
			return encodeValue(*x, w, depth)
		}
	case string:
		w.u8(tagText)
		w.str(x)
	case []byte:
		w.u8(tagBytes)
		w.u32(uint32(len(x)))
		w.b = append(w.b, x...)
	case time.Time:
		w.u8(tagTimestamp)
		w.u64(uint64(x.UnixNano()))
	case UUID:
		w.u8(tagUUID)
		w.b = append(w.b, x[:]...)
	case ID:
		w.u8(tagID)
		w.b = append(w.b, x[:]...)
	case map[string]any:
		w.u8(tagMap)
		return encodeFields(x, w, depth+1)
	case []any:
		w.u8(tagList)
		w.u32(uint32(len(x)))
		for _, item := range x {
			if err := encodeValue(item, w, depth+1); err != nil {
				return err
			}
		}
	default:
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Slice, reflect.Array:
			w.u8(tagList)
			w.u32(uint32(rv.Len()))
			for i := 0; i < rv.Len(); i++ {
				if err := encodeValue(rv.Index(i).Interface(), w, depth+1); err != nil {
					return err
				}
			}
		case reflect.Map:
			if rv.Type().Key().Kind() != reflect.String {
				return invalidf("map keys must be strings")
			}
			m := make(map[string]any, rv.Len())
			for _, k := range rv.MapKeys() {
				m[k.String()] = rv.MapIndex(k).Interface()
			}
			w.u8(tagMap)
			return encodeFields(m, w, depth+1)
		case reflect.Pointer:
			if rv.IsNil() {
				w.u8(tagNull)
				return nil
			}
			return encodeValue(rv.Elem().Interface(), w, depth)
		default:
			return invalidf("cannot store a value of type %T", v)
		}
	}
	return nil
}

// encodeFields appends `u32 count | (str key, value)*` with keys in ascending byte order.
func encodeFields(fields map[string]any, w *writer, depth int) error {
	keys := sortedKeys(fields)
	w.u32(uint32(len(keys)))
	for _, k := range keys {
		w.str(k)
		if err := encodeValue(fields[k], w, depth); err != nil {
			return err
		}
	}
	return nil
}

type reader struct {
	b   []byte
	pos int
}

var errTruncated = invalidf("truncated response")

func (r *reader) remaining() int { return len(r.b) - r.pos }

func (r *reader) take(n int) ([]byte, error) {
	if n < 0 || r.pos+n > len(r.b) {
		return nil, errTruncated
	}
	s := r.b[r.pos : r.pos+n]
	r.pos += n
	return s, nil
}

func (r *reader) u8() (byte, error) {
	b, err := r.take(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func (r *reader) u32() (uint32, error) {
	b, err := r.take(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}

func (r *reader) u64() (uint64, error) {
	b, err := r.take(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

func (r *reader) str() (string, error) {
	n, err := r.u32()
	if err != nil {
		return "", err
	}
	b, err := r.take(int(n))
	if err != nil {
		return "", err
	}
	if !utf8.Valid(b) {
		return "", invalidf("invalid UTF-8 in response")
	}
	return string(b), nil
}

func (r *reader) count(minSize int) (int, error) {
	n, err := r.u32()
	if err != nil {
		return 0, err
	}
	if int64(n)*int64(max(minSize, 1)) > int64(r.remaining()) {
		return 0, errTruncated
	}
	return int(n), nil
}

func (r *reader) i128() (*big.Int, error) {
	b, err := r.take(16)
	if err != nil {
		return nil, err
	}
	var be [16]byte
	for i := 0; i < 16; i++ {
		be[i] = b[15-i]
	}
	v := new(big.Int).SetBytes(be[:])
	if b[15]&0x80 != 0 {
		v.Sub(v, two128)
	}
	return v, nil
}

func (r *reader) value(depth int) (any, error) {
	tag, err := r.u8()
	if err != nil {
		return nil, err
	}
	switch tag {
	case tagNull:
		return nil, nil
	case tagBool:
		b, err := r.u8()
		if err != nil {
			return nil, err
		}
		if b > 1 {
			return nil, invalidf("bool byte must be 0 or 1")
		}
		return b == 1, nil
	case tagInt64:
		v, err := r.u64()
		return int64(v), err
	case tagUint64:
		return r.u64()
	case tagFloat64:
		v, err := r.u64()
		return math.Float64frombits(v), err
	case tagInt128:
		return r.i128()
	case tagTimestamp:
		v, err := r.u64()
		return time.Unix(0, int64(v)).UTC(), err
	case tagUUID:
		b, err := r.take(16)
		var u UUID
		copy(u[:], b)
		return u, err
	case tagID:
		b, err := r.take(24)
		var id ID
		copy(id[:], b)
		return id, err
	case tagText:
		return r.str()
	case tagBytes:
		n, err := r.u32()
		if err != nil {
			return nil, err
		}
		b, err := r.take(int(n))
		return append([]byte(nil), b...), err
	case tagDecimal:
		m, err := r.i128()
		if err != nil {
			return nil, err
		}
		scale, err := r.u8()
		if err != nil {
			return nil, err
		}
		return NewDecimal(m, scale)
	case tagList:
		if depth >= maxDepth {
			return nil, invalidf("value nesting is too deep")
		}
		n, err := r.count(1)
		if err != nil {
			return nil, err
		}
		out := make([]any, 0, n)
		for i := 0; i < n; i++ {
			v, err := r.value(depth + 1)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	case tagMap:
		if depth >= maxDepth {
			return nil, invalidf("value nesting is too deep")
		}
		return r.fields(depth + 1)
	}
	return nil, invalidf("unknown value tag %d", tag)
}

func (r *reader) fields(depth int) (map[string]any, error) {
	n, err := r.count(5)
	if err != nil {
		return nil, err
	}
	out := make(map[string]any, n)
	for i := 0; i < n; i++ {
		k, err := r.str()
		if err != nil {
			return nil, err
		}
		if _, dup := out[k]; dup {
			return nil, invalidf("duplicate map key")
		}
		v, err := r.value(depth)
		if err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, nil
}

// EncodeValue returns the wire encoding of one value (exposed for tests and tooling).
func EncodeValue(v any) ([]byte, error) {
	w := &writer{}
	err := encodeValue(v, w, 0)
	return w.b, err
}

// DecodeValue decodes exactly one value.
func DecodeValue(b []byte) (any, error) {
	r := &reader{b: b}
	v, err := r.value(0)
	if err != nil {
		return nil, err
	}
	if r.remaining() != 0 {
		return nil, invalidf("trailing bytes after value")
	}
	return v, nil
}

func defaultStr(s, d string) string {
	if s == "" {
		return d
	}
	return s
}

// sortedKeys returns the keys in ascending byte order (Go strings compare by bytes).
func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
