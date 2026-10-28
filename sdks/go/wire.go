package storine

import (
	"encoding/binary"
	"strings"
)

const (
	opAuth          = 0x0C
	opDocGet        = 0x20
	opDocWrite      = 0x21
	opDocQuery      = 0x22
	opDocAdmin      = 0x23
	opDocInfo       = 0x25
	opSetOptions    = 0x26
	opClusterInfo   = 0x27
	stOK            = 0
	stNotFound      = 1
	stError         = 2
	stOverloaded    = 3
	stConflict      = 4
	stUnauthorized  = 5
	stRateLimited   = 6
	stNotLeader     = 7
	stInvalid       = 8
	stCommitUnknown = 9
)

// FieldOp is one atomic change applied server-side by Update: Set, Unset, Add or Append.
type FieldOp struct {
	kind           byte
	path           string
	value          any
	min, max       any
	hasMin, hasMax bool
}

// Set sets the field at path (dotted for nested maps) to value.
func Set(path string, value any) FieldOp { return FieldOp{kind: 1, path: path, value: value} }

// Unset removes the field at path.
func Unset(path string) FieldOp { return FieldOp{kind: 2, path: path} }

// Add adds amount (negative to subtract) exactly and atomically.
// Bound the result with .Min / .Max: Add("balance", MustDecimal("-25.00")).Min(0) never overdraws.
func Add(path string, amount any) FieldOp { return FieldOp{kind: 3, path: path, value: amount} }

// Append appends value to the list at path (a missing field becomes a one-element list).
func Append(path string, value any) FieldOp { return FieldOp{kind: 4, path: path, value: value} }

// Min refuses (ErrInvalid) an Add whose result would be below bound.
func (op FieldOp) Min(bound any) FieldOp { op.min, op.hasMin = bound, true; return op }

// Max refuses (ErrInvalid) an Add whose result would be above bound.
func (op FieldOp) Max(bound any) FieldOp { op.max, op.hasMax = bound, true; return op }

func encodeOp(op FieldOp, w *writer) error {
	w.u8(op.kind)
	w.str(op.path)
	switch op.kind {
	case 1, 4:
		return encodeValue(op.value, w, 0)
	case 3:
		if err := encodeValue(op.value, w, 0); err != nil {
			return err
		}
		var flags byte
		if op.hasMin {
			flags |= 1
		}
		if op.hasMax {
			flags |= 2
		}
		w.u8(flags)
		if op.hasMin {
			if err := encodeValue(op.min, w, 0); err != nil {
				return err
			}
		}
		if op.hasMax {
			return encodeValue(op.max, w, 0)
		}
	case 2:
	default:
		return invalidf("unknown field operation (build them with Set, Unset, Add, Append)")
	}
	return nil
}

// Filter is a query condition: Eq, Gt, Between, And, ... A nil Filter matches every document.
type Filter struct {
	kind     byte
	path     string
	values   []any
	children []*Filter
}

func cmp(kind byte, path string, v any) *Filter {
	return &Filter{kind: kind, path: path, values: []any{v}}
}

// Eq: field equals value (numbers compare numerically; nil matches missing fields).
func Eq(path string, value any) *Filter { return cmp(1, path, value) }

// Ne: field differs from value.
func Ne(path string, value any) *Filter { return cmp(2, path, value) }

// Lt: field < value.
func Lt(path string, value any) *Filter { return cmp(3, path, value) }

// Le: field <= value.
func Le(path string, value any) *Filter { return cmp(4, path, value) }

// Gt: field > value.
func Gt(path string, value any) *Filter { return cmp(5, path, value) }

// Ge: field >= value.
func Ge(path string, value any) *Filter { return cmp(6, path, value) }

// Between: low <= field <= high.
func Between(path string, low, high any) *Filter {
	return &Filter{kind: 7, path: path, values: []any{low, high}}
}

// In: field equals any of values.
func In(path string, values ...any) *Filter { return &Filter{kind: 8, path: path, values: values} }

// Prefix: text field starts with text.
func Prefix(path, text string) *Filter { return cmp(9, path, text) }

// Contains: text field contains value, or list field has an element equal to value.
func Contains(path string, value any) *Filter { return cmp(10, path, value) }

// Exists: field is present and not null.
func Exists(path string) *Filter { return &Filter{kind: 11, path: path} }

// Missing: field is absent or null.
func Missing(path string) *Filter { return &Filter{kind: 12, path: path} }

// KeyPrefix: document key starts with text.
func KeyPrefix(text string) *Filter { return &Filter{kind: 13, values: []any{text}} }

// And: every filter holds.
func And(filters ...*Filter) *Filter { return &Filter{kind: 14, children: filters} }

// Or: at least one filter holds.
func Or(filters ...*Filter) *Filter { return &Filter{kind: 15, children: filters} }

// Not: the filter does not hold.
func Not(f *Filter) *Filter { return &Filter{kind: 16, children: []*Filter{f}} }

// Match: every field equals the given value (Match(Doc{"kind": "book"})).
func Match(fields Doc) *Filter {
	keys := sortedKeys(fields)
	parts := make([]*Filter, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, Eq(k, fields[k]))
	}
	if len(parts) == 1 {
		return parts[0]
	}
	return And(parts...)
}

func encodeFilter(f *Filter, w *writer, depth int) error {
	if depth > maxDepth {
		return invalidf("filter nesting is too deep (max 32)")
	}
	if f == nil {
		return invalidf("nil filter inside And / Or / Not")
	}
	w.u8(f.kind)
	switch f.kind {
	case 1, 2, 3, 4, 5, 6, 10:
		w.str(f.path)
		return encodeValue(f.values[0], w, 0)
	case 7:
		w.str(f.path)
		if err := encodeValue(f.values[0], w, 0); err != nil {
			return err
		}
		return encodeValue(f.values[1], w, 0)
	case 8:
		w.str(f.path)
		w.u32(uint32(len(f.values)))
		for _, v := range f.values {
			if err := encodeValue(v, w, 0); err != nil {
				return err
			}
		}
	case 9:
		w.str(f.path)
		w.str(f.values[0].(string))
	case 11, 12:
		w.str(f.path)
	case 13:
		w.str(f.values[0].(string))
	case 14, 15:
		w.u32(uint32(len(f.children)))
		for _, c := range f.children {
			if err := encodeFilter(c, w, depth+1); err != nil {
				return err
			}
		}
	case 16:
		return encodeFilter(f.children[0], w, depth+1)
	default:
		return invalidf("unknown filter (build them with Eq, Gt, And, ...)")
	}
	return nil
}

// write is one write of an atomic request.
type write struct {
	collection, key string
	kind            byte // 1 put, 2 put+ttl, 3 delete, 4 patch, 5 check
	pre             byte // 0 none, 1 absent, 2 exists, 3 version
	version         uint64
	fields          Doc
	ttlMs           uint64
	ops             []FieldOp
}

func encodeGet(collection, key string) []byte {
	w := &writer{}
	w.str(collection)
	w.str(key)
	return w.b
}

// MaxByteRange is the most bytes one ReadBytes call returns (16 MiB).
const MaxByteRange = 16 << 20

// encodeGetRange is OP_DOC_GET with a byte-range shape: bytes offset..offset+length of one field.
func encodeGetRange(collection, key, field string, offset uint64, length int) ([]byte, error) {
	if length < 0 || length > MaxByteRange {
		return nil, invalidf("length must be between 0 and %d", MaxByteRange)
	}
	w := &writer{b: encodeGet(collection, key)}
	w.u8(1)
	w.str(field)
	w.u64(offset)
	w.u32(uint32(length))
	return w.b, nil
}

// encodeGetFields is OP_DOC_GET with a projection shape: only the given (dotted) paths.
func encodeGetFields(collection, key string, paths []string) ([]byte, error) {
	if len(paths) > 256 {
		return nil, invalidf("at most 256 fields can be projected")
	}
	w := &writer{b: encodeGet(collection, key)}
	w.u8(2)
	w.u32(uint32(len(paths)))
	for _, p := range paths {
		w.str(p)
	}
	return w.b, nil
}

// decodeByteRange decodes `u64 total | u32 n | n bytes`.
func decodeByteRange(body []byte) (total uint64, data []byte, err error) {
	r := &reader{b: body}
	if total, err = r.u64(); err != nil {
		return 0, nil, err
	}
	n, err := r.u32()
	if err != nil {
		return 0, nil, err
	}
	if data, err = r.take(int(n)); err != nil {
		return 0, nil, err
	}
	if r.remaining() != 0 {
		return 0, nil, invalidf("trailing bytes after byte range")
	}
	return total, data, nil
}

func encodeWrites(writes []write, requestID []byte) ([]byte, error) {
	w := &writer{}
	if requestID == nil {
		w.u8(0)
	} else {
		w.u8(1)
		w.b = append(w.b, requestID...)
	}
	w.u32(uint32(len(writes)))
	for _, x := range writes {
		w.str(x.collection)
		w.str(x.key)
		w.u8(x.pre)
		if x.pre == 3 {
			w.u64(x.version)
		}
		w.u8(x.kind)
		switch x.kind {
		case 1:
			if err := encodeFields(x.fields, w, 1); err != nil {
				return nil, err
			}
		case 2:
			w.u64(x.ttlMs)
			if err := encodeFields(x.fields, w, 1); err != nil {
				return nil, err
			}
		case 4:
			w.u32(uint32(len(x.ops)))
			for _, op := range x.ops {
				if err := encodeOp(op, w); err != nil {
					return nil, err
				}
			}
		}
	}
	return w.b, nil
}

type orderKey struct {
	path string
	desc bool
}

// parseOrderBy turns "price" / "-price" (descending) into sort keys.
func parseOrderBy(fields []string) []orderKey {
	out := make([]orderKey, 0, len(fields))
	for _, f := range fields {
		if strings.HasPrefix(f, "-") {
			out = append(out, orderKey{f[1:], true})
		} else {
			out = append(out, orderKey{f, false})
		}
	}
	return out
}

// nearestQuery is the nearest-neighbour part of a query: the k documents whose field is most
// similar to vector.
type nearestQuery struct {
	field  string
	vector []float32
	k      uint32
	exact  bool
}

func encodeQuery(collection string, f *Filter, order []orderKey, limit, offset uint32, afterKey *string, nearest *nearestQuery) ([]byte, error) {
	w := &writer{}
	w.str(collection)
	if f == nil {
		w.u8(0)
	} else {
		w.u8(1)
		if err := encodeFilter(f, w, 0); err != nil {
			return nil, err
		}
	}
	w.u32(uint32(len(order)))
	for _, o := range order {
		w.str(o.path)
		if o.desc {
			w.u8(1)
		} else {
			w.u8(0)
		}
	}
	w.u32(limit)
	w.u32(offset)
	if afterKey == nil {
		w.u8(0)
	} else {
		w.u8(1)
		w.str(*afterKey)
	}
	if nearest != nil {
		// Optional trailing block: u8 1 | str field | u32 k | u8 exact | u32 dims | dims x f32.
		w.u8(1)
		w.str(nearest.field)
		w.u32(nearest.k)
		if nearest.exact {
			w.u8(1)
		} else {
			w.u8(0)
		}
		w.u32(uint32(len(nearest.vector)))
		for _, x := range nearest.vector {
			w.f32(x)
		}
	}
	return w.b, nil
}

// decodeWriteResponse returns the new version of each write (0 = deleted / absent).
func decodeWriteResponse(body []byte) ([]uint64, error) {
	r := &reader{b: body}
	if _, err := r.u8(); err != nil { // duplicate flag
		return nil, err
	}
	n, err := r.count(9)
	if err != nil {
		return nil, err
	}
	versions := make([]uint64, n)
	for i := range versions {
		exists, err := r.u8()
		if err != nil {
			return nil, err
		}
		v, err := r.u64()
		if err != nil {
			return nil, err
		}
		if exists != 0 {
			versions[i] = v
		}
	}
	return versions, nil
}

type page struct {
	total   uint32
	nextKey *string
	docs    []*Document
}

func decodePage(body []byte) (*page, error) {
	r := &reader{b: body}
	total, err := r.u32()
	if err != nil {
		return nil, err
	}
	p := &page{total: total}
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
	n, err := r.count(16)
	if err != nil {
		return nil, err
	}
	for i := 0; i < n; i++ {
		key, err := r.str()
		if err != nil {
			return nil, err
		}
		version, err := r.u64()
		if err != nil {
			return nil, err
		}
		fields, err := r.fields(1)
		if err != nil {
			return nil, err
		}
		p.docs = append(p.docs, &Document{Key: key, Version: version, Fields: fields})
	}
	return p, nil
}

func decodeConflict(body []byte) *Error {
	e := newError(ErrConflict, stConflict, string(body))
	if len(body) >= 13 {
		if idx := binary.LittleEndian.Uint32(body); idx != 0xFFFFFFFF {
			e.WriteIndex = int(idx)
		}
		if body[4] != 0 {
			e.CurrentVersion = binary.LittleEndian.Uint64(body[5:])
		}
		e.Message = string(body[13:])
	}
	return e
}

// CollectionInfo describes a collection's indexes and metadata.
type CollectionInfo struct {
	Name     string
	Indexes  []string // ready to use
	Building []string // being built from existing documents
	Metadata Doc
}

func (r *reader) strList() ([]string, error) {
	n, err := r.count(4)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		s, err := r.str()
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

func decodeInfo(r *reader) (*CollectionInfo, error) {
	name, err := r.str()
	if err != nil {
		return nil, err
	}
	info := &CollectionInfo{Name: name}
	if info.Indexes, err = r.strList(); err != nil {
		return nil, err
	}
	if info.Building, err = r.strList(); err != nil {
		return nil, err
	}
	if info.Metadata, err = r.fields(1); err != nil {
		return nil, err
	}
	return info, nil
}

// VectorIndex describes one vector index of a collection.
type VectorIndex struct {
	Field string
	Dims  int
	Ready bool // false while being built from existing documents
}

func decodeVectorIndexes(body []byte) ([]VectorIndex, error) {
	r := &reader{b: body}
	n, err := r.count(9)
	if err != nil {
		return nil, err
	}
	out := make([]VectorIndex, 0, n)
	for i := 0; i < n; i++ {
		field, err := r.str()
		if err != nil {
			return nil, err
		}
		dims, err := r.u32()
		if err != nil {
			return nil, err
		}
		ready, err := r.u8()
		if err != nil {
			return nil, err
		}
		out = append(out, VectorIndex{Field: field, Dims: int(dims), Ready: ready == 1})
	}
	return out, nil
}

// ClusterInfo describes the server currently used and its view of the cluster.
type ClusterInfo struct {
	NodeID      uint64
	IsLeader    bool
	LeaderID    uint64 // 0 when no leader is known
	LeaderAddr  string // "" when no leader is known
	Term        uint64
	CommitIndex uint64
	LastApplied uint64
	Members     map[uint64]string
}

func decodeClusterInfo(body []byte) (*ClusterInfo, error) {
	r := &reader{b: body}
	ci := &ClusterInfo{Members: map[uint64]string{}}
	var err error
	if ci.NodeID, err = r.u64(); err != nil {
		return nil, err
	}
	lb, err := r.u8()
	if err != nil {
		return nil, err
	}
	ci.IsLeader = lb != 0
	if ci.LeaderID, err = r.u64(); err != nil {
		return nil, err
	}
	if ci.LeaderAddr, err = r.str(); err != nil {
		return nil, err
	}
	for _, p := range []*uint64{&ci.Term, &ci.CommitIndex, &ci.LastApplied} {
		if *p, err = r.u64(); err != nil {
			return nil, err
		}
	}
	n, err := r.count(12)
	if err != nil {
		return nil, err
	}
	for i := 0; i < n; i++ {
		id, err := r.u64()
		if err != nil {
			return nil, err
		}
		addr, err := r.str()
		if err != nil {
			return nil, err
		}
		ci.Members[id] = addr
	}
	return ci, nil
}
