package storine

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	mrand "math/rand"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const maxBody = 256 * 1024 * 1024

// Document is a stored document: its fields plus its key and version.
type Document struct {
	Key     string
	Version uint64
	Fields  Doc
}

// Score is the similarity of a Nearest result (also Fields["$score"]); 0 for other queries.
func (d *Document) Score() float64 {
	f, _ := d.Fields["$score"].(float64)
	return f
}

// Options configure Connect. Every field is optional.
type Options struct {
	// Token authenticates (also accepted in the URL: storine://token@host:port).
	Token string
	// Timeout of one request (default 5s). Retries continue for up to 3x this.
	Timeout time.Duration
	// FollowerReads allows possibly stale reads from cluster followers (default: leader reads).
	FollowerReads bool
	// PoolSize is the number of idle connections kept to the current server (default 16).
	PoolSize int
}

type conn struct {
	address string
	c       net.Conn
}

// Client is a connection pool to a Storine server or cluster. It is safe for concurrent use.
// Writes are retried automatically with the same request id (applied at most once) across
// timeouts, reconnects and leader changes.
type Client struct {
	seeds  []string
	opts   Options
	mu     sync.Mutex
	target string
	pool   []*conn
}

// Connect returns a client for url: "storine://[token@]host:port[,host:port...]" or just
// "host:port". Query parameters timeout_ms=5000 and follower_reads=1 are accepted too.
// Connections are opened lazily, on the first request.
func Connect(rawURL string, options ...Options) (*Client, error) {
	var o Options
	if len(options) > 1 {
		return nil, invalidf("Connect takes at most one Options")
	}
	if len(options) == 1 {
		o = options[0]
	}
	if !strings.Contains(rawURL, "://") {
		rawURL = "storine://" + rawURL
	}
	scheme, rest, _ := strings.Cut(rawURL, "://")
	if scheme != "storine" {
		return nil, invalidf("connection strings start with storine://")
	}
	hosts, query, _ := strings.Cut(rest, "?")
	if at := strings.LastIndex(hosts, "@"); at >= 0 {
		if o.Token == "" {
			tok, err := url.PathUnescape(hosts[:at])
			if err != nil {
				return nil, invalidf("bad token in connection string")
			}
			o.Token = tok
		}
		hosts = hosts[at+1:]
	}
	hosts = strings.TrimSuffix(hosts, "/")
	q, err := url.ParseQuery(query)
	if err != nil {
		return nil, invalidf("bad connection string options: %v", err)
	}
	if v := q.Get("timeout_ms"); v != "" {
		ms, err := strconv.Atoi(v)
		if err != nil || ms <= 0 {
			return nil, invalidf("timeout_ms must be a positive integer")
		}
		o.Timeout = time.Duration(ms) * time.Millisecond
	}
	if v := q.Get("follower_reads"); v != "" {
		o.FollowerReads = v == "1" || v == "true" || v == "yes"
	}
	if o.Timeout <= 0 {
		o.Timeout = 5 * time.Second
	}
	if o.PoolSize <= 0 {
		o.PoolSize = 16
	}
	var seeds []string
	for _, h := range strings.Split(hosts, ",") {
		if h = strings.TrimSpace(h); h != "" {
			seeds = append(seeds, normalizeAddress(h))
		}
	}
	if len(seeds) == 0 {
		return nil, invalidf("no server address given")
	}
	return &Client{seeds: seeds, opts: o, target: seeds[0]}, nil
}

func normalizeAddress(a string) string {
	if _, _, err := net.SplitHostPort(a); err == nil {
		return a
	}
	return net.JoinHostPort(strings.Trim(a, "[]"), "7878")
}

// Close closes every pooled connection.
func (c *Client) Close() error {
	c.mu.Lock()
	pool := c.pool
	c.pool = nil
	c.mu.Unlock()
	for _, x := range pool {
		x.c.Close()
	}
	return nil
}

// ── plumbing ──

func (c *Client) dial(ctx context.Context, address string) (*conn, error) {
	d := net.Dialer{Timeout: c.opts.Timeout}
	nc, err := d.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	x := &conn{address: address, c: nc}
	if c.opts.Token != "" {
		st, _, body, err := x.call(ctx, c.opts.Timeout, opAuth, []byte(c.opts.Token))
		if err != nil {
			nc.Close()
			return nil, err
		}
		if st != stOK {
			nc.Close()
			return nil, newError(ErrUnauthorized, st, "authentication failed: "+string(body))
		}
	}
	if c.opts.FollowerReads {
		if _, _, _, err := x.call(ctx, c.opts.Timeout, opSetOptions, []byte{1}); err != nil {
			nc.Close()
			return nil, err
		}
	}
	return x, nil
}

func (x *conn) call(ctx context.Context, timeout time.Duration, op byte, body []byte) (int, uint64, []byte, error) {
	deadline := time.Now().Add(timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	x.c.SetDeadline(deadline)
	frame := make([]byte, 29, 29+len(body))
	frame[0] = op
	binary.LittleEndian.PutUint32(frame[25:], uint32(len(body)))
	if _, err := x.c.Write(append(frame, body...)); err != nil {
		return 0, 0, nil, err
	}
	var hdr [13]byte
	if _, err := io.ReadFull(x.c, hdr[:]); err != nil {
		return 0, 0, nil, err
	}
	n := binary.LittleEndian.Uint32(hdr[9:])
	if n > maxBody {
		return 0, 0, nil, errors.New("response too large")
	}
	rbody := make([]byte, n)
	if _, err := io.ReadFull(x.c, rbody); err != nil {
		return 0, 0, nil, err
	}
	return int(hdr[0]), binary.LittleEndian.Uint64(hdr[1:]), rbody, nil
}

func (c *Client) acquire(ctx context.Context) (*conn, error) {
	c.mu.Lock()
	target := c.target
	for len(c.pool) > 0 {
		x := c.pool[len(c.pool)-1]
		c.pool = c.pool[:len(c.pool)-1]
		if x.address == target {
			c.mu.Unlock()
			return x, nil
		}
		x.c.Close()
	}
	c.mu.Unlock()
	x, err := c.dial(ctx, target)
	var e *Error
	if errors.As(err, &e) {
		return nil, err
	}
	if err != nil {
		return nil, &dialError{address: target, err: err}
	}
	return x, nil
}

type dialError struct {
	address string
	err     error
}

func (e *dialError) Error() string { return e.err.Error() }

func (c *Client) release(x *conn) {
	c.mu.Lock()
	if x.address == c.target && len(c.pool) < c.opts.PoolSize {
		c.pool = append(c.pool, x)
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()
	x.c.Close()
}

// redirect switches to address, or (when empty) to a random other seed not known to have failed.
func (c *Client) redirect(address string, failed map[string]bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if address != "" {
		c.target = normalizeAddress(address)
		return
	}
	var others []string
	for _, s := range c.seeds {
		if s != c.target && !failed[s] {
			others = append(others, s)
		}
	}
	if len(others) == 0 {
		for _, s := range c.seeds {
			if s != c.target {
				others = append(others, s)
			}
		}
	}
	if len(others) == 0 {
		others = c.seeds
	}
	c.target = others[mrand.Intn(len(others))]
}

// request sends a request, following leader redirects and retrying transient failures for up to
// three times the timeout (enough to ride out a leader election). Every request is idempotent:
// reads by nature and writes because they carry a request id.
func (c *Client) request(ctx context.Context, op byte, body []byte) (int, uint64, []byte, error) {
	deadline := time.Now().Add(max(3*c.opts.Timeout, time.Second))
	delay := 20 * time.Millisecond
	var last error
	failed := map[string]bool{}
	backoff := func() error {
		t := time.NewTimer(delay)
		defer t.Stop()
		delay = min(delay*2, 500*time.Millisecond)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			return nil
		}
	}
	for attempt := 0; attempt < 1000 && (attempt < 8 || time.Now().Before(deadline)); attempt++ {
		if err := ctx.Err(); err != nil {
			return 0, 0, nil, err
		}
		x, err := c.acquire(ctx)
		if err != nil {
			var ue *Error
			if errors.As(err, &ue) {
				return 0, 0, nil, err // authentication failed: retrying cannot help
			}
			last = err
			if de, ok := err.(*dialError); ok {
				failed[de.address] = true
			}
			c.redirect("", failed)
			if err := backoff(); err != nil {
				return 0, 0, nil, err
			}
			continue
		}
		status, version, rbody, err := x.call(ctx, c.opts.Timeout, op, body)
		if err != nil {
			x.c.Close()
			if ctx.Err() != nil {
				return 0, 0, nil, ctx.Err()
			}
			last = err
			failed[x.address] = true
			c.redirect("", failed)
			if err := backoff(); err != nil {
				return 0, 0, nil, err
			}
			continue
		}
		switch status {
		case stNotLeader:
			x.c.Close()
			hint := string(rbody)
			if hint != "" && failed[normalizeAddress(hint)] {
				hint = "" // stale hint: the old leader is down
			}
			c.redirect(hint, failed)
			last = newError(ErrUnavailable, status, "no cluster leader is available yet")
			if hint == "" {
				if err := backoff(); err != nil {
					return 0, 0, nil, err
				}
			}
			continue
		case stCommitUnknown, stOverloaded:
			x.c.Close()
			if status == stOverloaded {
				last = newError(ErrOverloaded, status, defaultStr(string(rbody), "server overloaded"))
			} else {
				last = newError(ErrUnavailable, status, "write outcome unknown and retries exhausted")
			}
			if err := backoff(); err != nil {
				return 0, 0, nil, err
			}
			continue
		case stUnauthorized, stRateLimited:
			x.c.Close() // the server closed the connection
		default:
			c.release(x)
		}
		return status, version, rbody, nil
	}
	if e, ok := last.(*Error); ok {
		return 0, 0, nil, e
	}
	return 0, 0, nil, newError(ErrUnavailable, 0, fmt.Sprintf("could not reach any server: %v", last))
}

func statusError(status int, body []byte) error {
	text := string(body)
	switch status {
	case stConflict:
		return decodeConflict(body)
	case stNotFound:
		return newError(ErrNotFound, status, defaultStr(text, "document not found"))
	case stInvalid:
		return newError(ErrInvalid, status, text)
	case stUnauthorized:
		return newError(ErrUnauthorized, status, defaultStr(text, "not authorized"))
	case stRateLimited:
		return newError(ErrRateLimited, status, defaultStr(text, "rate limited"))
	case stOverloaded:
		return newError(ErrOverloaded, status, defaultStr(text, "server overloaded"))
	case stCommitUnknown:
		return newError(ErrUnavailable, status, "write outcome unknown; retry the same call")
	}
	return newError(ErrServer, status, defaultStr(text, fmt.Sprintf("server error (status %d)", status)))
}

func (c *Client) call(ctx context.Context, op byte, body []byte) (uint64, []byte, error) {
	status, version, rbody, err := c.request(ctx, op, body)
	if err != nil {
		return 0, nil, err
	}
	if status != stOK {
		return 0, nil, statusError(status, rbody)
	}
	return version, rbody, nil
}

// commit applies writes atomically (exactly once, even across retries); returns the new versions
// (0 for a deleted or checked-absent document).
func (c *Client) commit(ctx context.Context, writes []write) ([]uint64, error) {
	requestID := make([]byte, 16)
	if _, err := rand.Read(requestID); err != nil {
		return nil, err
	}
	body, err := encodeWrites(writes, requestID)
	if err != nil {
		return nil, err
	}
	status, _, rbody, err := c.request(ctx, opDocWrite, body)
	if err != nil {
		return nil, err
	}
	if status == stNotFound {
		// Body: u32 index of the write that patched a missing document, then the message.
		e := newError(ErrNotFound, status, "document not found")
		if len(rbody) >= 4 {
			e.WriteIndex = int(binary.LittleEndian.Uint32(rbody))
			e.Message = defaultStr(string(rbody[4:]), e.Message)
		}
		return nil, e
	}
	if status != stOK {
		return nil, statusError(status, rbody)
	}
	return decodeWriteResponse(rbody)
}

// ── public API ──

// Collection returns a handle on the collection name (created on first write).
func (c *Client) Collection(name string) *Collection { return &Collection{client: c, name: name} }

// Collections lists every declared collection.
func (c *Client) Collections(ctx context.Context) ([]string, error) {
	_, body, err := c.call(ctx, opDocInfo, []byte{3})
	if err != nil {
		return nil, err
	}
	r := &reader{b: body}
	n, err := r.count(17)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, n)
	for i := 0; i < n; i++ {
		info, err := decodeInfo(r)
		if err != nil {
			return nil, err
		}
		names = append(names, info.Name)
	}
	return names, nil
}

// ClusterInfo returns node id, leader, term and commit indexes of the server currently used.
func (c *Client) ClusterInfo(ctx context.Context) (*ClusterInfo, error) {
	_, body, err := c.call(ctx, opClusterInfo, nil)
	if err != nil {
		return nil, err
	}
	return decodeClusterInfo(body)
}

// Transaction runs fn to collect writes, then commits them atomically: all or none.
// It returns the new version of each write in order (0 for deletes and checks).
// If fn returns an error nothing is written.
//
//	_, err := db.Transaction(ctx, func(tx *storine.Tx) error {
//		tx.Update("accounts", "alice", storine.Add("balance", storine.MustDecimal("-10")).Min(0))
//		tx.Update("accounts", "bob", storine.Add("balance", storine.MustDecimal("10")))
//		return nil
//	})
func (c *Client) Transaction(ctx context.Context, fn func(tx *Tx) error) ([]uint64, error) {
	tx := &Tx{}
	if err := fn(tx); err != nil {
		return nil, err
	}
	if tx.err != nil {
		return nil, tx.err
	}
	if len(tx.writes) == 0 {
		return []uint64{}, nil
	}
	return c.commit(ctx, tx.writes)
}

func putWrite(collection, key string, fields Doc, ttl time.Duration, pre byte, version uint64) (write, error) {
	if fields == nil {
		fields = Doc{}
	}
	w := write{collection: collection, key: key, kind: 1, pre: pre, version: version, fields: fields}
	if ttl < 0 {
		return w, invalidf("ttl must be positive")
	}
	if ttl > 0 {
		w.kind = 2
		w.ttlMs = uint64(max(ttl.Milliseconds(), 1))
	}
	return w, nil
}

func patchWrite(collection, key string, pre byte, version uint64, ops []FieldOp) (write, error) {
	if len(ops) == 0 {
		return write{}, invalidf("Update needs at least one change (Set, Unset, Add, Append)")
	}
	return write{collection: collection, key: key, kind: 4, pre: pre, version: version, ops: ops}, nil
}

// Collection is a named set of documents. Every method is one atomic, exactly-once operation.
type Collection struct {
	client *Client
	name   string
}

// Name of the collection.
func (c *Collection) Name() string { return c.name }

func (c *Collection) one(ctx context.Context, w write, err error) (uint64, error) {
	if err != nil {
		return 0, err
	}
	v, err := c.client.commit(ctx, []write{w})
	if err != nil {
		return 0, err
	}
	return v[0], nil
}

// Get returns the document key, or nil (and no error) if it does not exist.
func (c *Collection) Get(ctx context.Context, key string) (*Document, error) {
	status, version, body, err := c.client.request(ctx, opDocGet, encodeGet(c.name, key))
	if err != nil {
		return nil, err
	}
	if status == stNotFound {
		return nil, nil
	}
	if status != stOK {
		return nil, statusError(status, body)
	}
	fields, err := (&reader{b: body}).fields(1)
	if err != nil {
		return nil, err
	}
	return &Document{Key: key, Version: version, Fields: fields}, nil
}

// Put creates or replaces the document; returns its new version.
func (c *Collection) Put(ctx context.Context, key string, fields Doc) (uint64, error) {
	w, err := putWrite(c.name, key, fields, 0, 0, 0)
	return c.one(ctx, w, err)
}

// PutWithTTL is Put for a document that expires after ttl.
func (c *Collection) PutWithTTL(ctx context.Context, key string, fields Doc, ttl time.Duration) (uint64, error) {
	if ttl <= 0 {
		return 0, invalidf("ttl must be positive")
	}
	w, err := putWrite(c.name, key, fields, ttl, 0, 0)
	return c.one(ctx, w, err)
}

// Insert creates the document; ErrConflict if it already exists.
func (c *Collection) Insert(ctx context.Context, key string, fields Doc) (uint64, error) {
	w, err := putWrite(c.name, key, fields, 0, 1, 0)
	return c.one(ctx, w, err)
}

// Replace replaces an existing document; ErrConflict if it does not exist.
func (c *Collection) Replace(ctx context.Context, key string, fields Doc) (uint64, error) {
	w, err := putWrite(c.name, key, fields, 0, 2, 0)
	return c.one(ctx, w, err)
}

// ReplaceIfVersion replaces the document only if it is still at version; ErrConflict otherwise.
func (c *Collection) ReplaceIfVersion(ctx context.Context, key string, version uint64, fields Doc) (uint64, error) {
	w, err := putWrite(c.name, key, fields, 0, 3, version)
	return c.one(ctx, w, err)
}

// Update applies field changes atomically on the server; returns the new version.
//
//	accounts.Update(ctx, "alice", storine.Add("balance", storine.MustDecimal("-25.00")).Min(0))
//
// ErrNotFound if the document does not exist; ErrInvalid if a bound would be violated.
func (c *Collection) Update(ctx context.Context, key string, changes ...FieldOp) (uint64, error) {
	w, err := patchWrite(c.name, key, 0, 0, changes)
	return c.one(ctx, w, err)
}

// UpdateIfVersion is Update applied only if the document is still at version; ErrConflict otherwise.
func (c *Collection) UpdateIfVersion(ctx context.Context, key string, version uint64, changes ...FieldOp) (uint64, error) {
	w, err := patchWrite(c.name, key, 3, version, changes)
	return c.one(ctx, w, err)
}

// Delete deletes the document; returns false if it did not exist.
func (c *Collection) Delete(ctx context.Context, key string) (bool, error) {
	_, err := c.one(ctx, write{collection: c.name, key: key, kind: 3, pre: 2}, nil)
	var e *Error
	if errors.As(err, &e) && e.Kind == ErrConflict && e.CurrentVersion == 0 {
		return false, nil
	}
	return err == nil, err
}

// DeleteIfVersion deletes the document only if it is still at version; ErrConflict otherwise.
func (c *Collection) DeleteIfVersion(ctx context.Context, key string, version uint64) error {
	_, err := c.one(ctx, write{collection: c.name, key: key, kind: 3, pre: 3, version: version}, nil)
	return err
}

// Query selects documents. The zero Query matches everything (up to 1,000 documents).
type Query struct {
	Where   *Filter  // nil: every document
	OrderBy []string // "price" ascending, "-price" descending; default key order
	Limit   int      // 0: server default (1,000)
	Offset  int
}

func (c *Collection) query(ctx context.Context, q Query, after *string) (*page, error) {
	return c.queryNearest(ctx, q, after, nil)
}

func (c *Collection) queryNearest(ctx context.Context, q Query, after *string, nearest *nearestQuery) (*page, error) {
	if q.Limit < 0 || q.Offset < 0 {
		return nil, invalidf("Limit and Offset must not be negative")
	}
	body, err := encodeQuery(c.name, q.Where, parseOrderBy(q.OrderBy), uint32(q.Limit), uint32(q.Offset), after, nearest)
	if err != nil {
		return nil, err
	}
	_, rbody, err := c.client.call(ctx, opDocQuery, body)
	if err != nil {
		return nil, err
	}
	return decodePage(rbody)
}

// Find returns the documents matching q.
func (c *Collection) Find(ctx context.Context, q Query) ([]*Document, error) {
	p, err := c.query(ctx, q, nil)
	if err != nil {
		return nil, err
	}
	return p.docs, nil
}

// First returns the first document matching q, or nil.
func (c *Collection) First(ctx context.Context, q Query) (*Document, error) {
	q.Limit = 1
	docs, err := c.Find(ctx, q)
	if err != nil || len(docs) == 0 {
		return nil, err
	}
	return docs[0], nil
}

// Count returns the number of documents matching where (nil: all).
func (c *Collection) Count(ctx context.Context, where *Filter) (int, error) {
	p, err := c.query(ctx, Query{Where: where, Limit: 1}, nil)
	if err != nil {
		return 0, err
	}
	return int(p.total), nil
}

// Scan calls fn for every document matching where (nil: all), in key order, for any collection
// size. Returning an error from fn stops the scan and returns that error.
func (c *Collection) Scan(ctx context.Context, where *Filter, fn func(*Document) error) error {
	var after *string
	for {
		p, err := c.query(ctx, Query{Where: where, Limit: 1000}, after)
		if err != nil {
			return err
		}
		for _, d := range p.docs {
			if err := fn(d); err != nil {
				return err
			}
		}
		if p.nextKey == nil {
			return nil
		}
		after = p.nextKey
	}
}

// NearestOptions configure Nearest. Every field is optional.
type NearestOptions struct {
	K     int     // number of documents to return; 0: 10
	Where *Filter // nil: every document
	Exact bool    // score every candidate instead of using the vector index
}

// Nearest returns the K documents whose field (a list of numbers) is most similar to vector
// (cosine), best first, among those matching Where. Each document's similarity is in
// doc.Score() (also Fields["$score"]). Uses the field's vector index when it has one (approximate
// candidates, exact scores); Exact scores every candidate. ErrInvalid if the dimensions differ.
func (c *Collection) Nearest(ctx context.Context, field string, vector []float32, o NearestOptions) ([]*Document, error) {
	if o.K < 0 {
		return nil, invalidf("K must not be negative")
	}
	if o.K == 0 {
		o.K = 10
	}
	n := &nearestQuery{field: field, vector: vector, k: uint32(o.K), exact: o.Exact}
	p, err := c.queryNearest(ctx, Query{Where: o.Where, Limit: o.K}, nil, n)
	if err != nil {
		return nil, err
	}
	return p.docs, nil
}

func (c *Collection) admin(ctx context.Context, op byte, sub byte, field *string) (uint64, []byte, error) {
	w := &writer{}
	w.u8(sub)
	w.str(c.name)
	if field != nil {
		w.str(*field)
	}
	return c.client.call(ctx, op, w.b)
}

// CreateVectorIndex indexes a top-level field holding dims numbers for Nearest (at most 4 per
// collection); existing documents included. Writes of other values to that field are then refused.
func (c *Collection) CreateVectorIndex(ctx context.Context, field string, dims int) error {
	if dims <= 0 {
		return invalidf("dims must be positive")
	}
	w := &writer{}
	w.u8(7)
	w.str(c.name)
	w.str(field)
	w.u32(uint32(dims))
	_, _, err := c.client.call(ctx, opDocAdmin, w.b)
	return err
}

// DropVectorIndex removes the vector index on field.
func (c *Collection) DropVectorIndex(ctx context.Context, field string) error {
	_, _, err := c.admin(ctx, opDocAdmin, 8, &field)
	return err
}

// VectorIndexes returns the collection's vector indexes.
func (c *Collection) VectorIndexes(ctx context.Context) ([]VectorIndex, error) {
	_, body, err := c.admin(ctx, opDocInfo, 9, nil)
	if err != nil {
		return nil, err
	}
	return decodeVectorIndexes(body)
}

// CreateIndex indexes a top-level field (at most 16 per collection); existing documents included.
func (c *Collection) CreateIndex(ctx context.Context, field string) error {
	_, _, err := c.admin(ctx, opDocAdmin, 1, &field)
	return err
}

// DropIndex removes the index on field.
func (c *Collection) DropIndex(ctx context.Context, field string) error {
	_, _, err := c.admin(ctx, opDocAdmin, 2, &field)
	return err
}

// Drop deletes every document and the collection's metadata; returns the number deleted.
func (c *Collection) Drop(ctx context.Context) (uint64, error) {
	n, _, err := c.admin(ctx, opDocAdmin, 4, nil)
	return n, err
}

// Info returns the collection's indexes and metadata.
func (c *Collection) Info(ctx context.Context) (*CollectionInfo, error) {
	_, body, err := c.admin(ctx, opDocInfo, 6, nil)
	if err != nil {
		return nil, err
	}
	return decodeInfo(&reader{b: body})
}

// Tx collects the writes of a Transaction. Its methods only buffer; nothing is sent until fn returns.
type Tx struct {
	writes []write
	err    error
}

func (t *Tx) add(w write, err error) *Tx {
	if err != nil && t.err == nil {
		t.err = err
	}
	t.writes = append(t.writes, w)
	return t
}

// Put creates or replaces a document.
func (t *Tx) Put(collection, key string, fields Doc) *Tx {
	return t.add(putWrite(collection, key, fields, 0, 0, 0))
}

// Insert creates a document; the transaction fails with ErrConflict if it exists.
func (t *Tx) Insert(collection, key string, fields Doc) *Tx {
	return t.add(putWrite(collection, key, fields, 0, 1, 0))
}

// Replace replaces an existing document.
func (t *Tx) Replace(collection, key string, fields Doc) *Tx {
	return t.add(putWrite(collection, key, fields, 0, 2, 0))
}

// ReplaceIfVersion replaces a document still at version.
func (t *Tx) ReplaceIfVersion(collection, key string, version uint64, fields Doc) *Tx {
	return t.add(putWrite(collection, key, fields, 0, 3, version))
}

// Update applies field changes to an existing document.
func (t *Tx) Update(collection, key string, changes ...FieldOp) *Tx {
	return t.add(patchWrite(collection, key, 0, 0, changes))
}

// UpdateIfVersion applies field changes to a document still at version.
func (t *Tx) UpdateIfVersion(collection, key string, version uint64, changes ...FieldOp) *Tx {
	return t.add(patchWrite(collection, key, 3, version, changes))
}

// Delete deletes a document if it exists.
func (t *Tx) Delete(collection, key string) *Tx {
	return t.add(write{collection: collection, key: key, kind: 3}, nil)
}

// DeleteIfVersion deletes a document still at version.
func (t *Tx) DeleteIfVersion(collection, key string, version uint64) *Tx {
	return t.add(write{collection: collection, key: key, kind: 3, pre: 3, version: version}, nil)
}

// RequireVersion commits only if the document is at version (it is not written).
func (t *Tx) RequireVersion(collection, key string, version uint64) *Tx {
	return t.add(write{collection: collection, key: key, kind: 5, pre: 3, version: version}, nil)
}

// RequireExists commits only if the document exists (exists=true) or does not (exists=false).
func (t *Tx) RequireExists(collection, key string, exists bool) *Tx {
	pre := byte(1)
	if exists {
		pre = 2
	}
	return t.add(write{collection: collection, key: key, kind: 5, pre: pre}, nil)
}
