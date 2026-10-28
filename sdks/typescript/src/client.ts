/** Storine client: connections, retries, redirects and the collection API. */

import { randomBytes } from "crypto";
import * as net from "net";

import {
  ConflictError, InvalidError, NotFoundError, OverloadedError, RateLimitedError, ServerError, StorineError,
  UnauthorizedError, UnavailableError,
} from "./errors";
import { Fields, Reader, Value } from "./values";
import * as w from "./wire";

const ZERO_KEY = new Uint8Array(24);
const MAX_BODY = 256 * 1024 * 1024;

/** A document: its fields plus `key` and `version`. */
export interface Document<T extends Fields = Fields> {
  key: string;
  version: bigint;
  fields: T;
  /** Similarity of a `nearest` result (also `fields["$score"]`); undefined for other queries. */
  score?: number;
}

/** Options of `connect`. */
export interface ConnectOptions {
  /** Bearer token (also accepted in the URL: `storine://token@host:port`). */
  token?: string;
  /** Per-request timeout in milliseconds (default 5000). Retries continue for 3x this. */
  timeoutMs?: number;
  /** Allow possibly stale reads from cluster followers (default false: leader reads). */
  followerReads?: boolean;
  /** Connections kept open to the current server (default 8). */
  poolSize?: number;
}

type Reply = { status: number; version: bigint; body: Uint8Array };

class Connection {
  private socket: net.Socket;
  private buffer = Buffer.alloc(0);
  private waiter?: { resolve: (r: Reply) => void; reject: (e: Error) => void; timer: NodeJS.Timeout };
  private closed = false;
  private chain: Promise<unknown> = Promise.resolve();

  private constructor(readonly address: string, private timeoutMs: number) {
    const [host, port] = splitAddress(address);
    this.socket = net.createConnection({ host, port });
    this.socket.setNoDelay(true);
    this.socket.on("data", (chunk: Buffer) => this.onData(chunk));
    const fail = (e: Error) => {
      this.closed = true;
      if (this.waiter) clearTimeout(this.waiter.timer);
      this.waiter?.reject(e);
      this.waiter = undefined;
    };
    this.socket.on("error", fail);
    this.socket.on("close", () => fail(new Error("server closed the connection")));
  }

  static async open(address: string, timeoutMs: number, token?: string, followerReads?: boolean): Promise<Connection> {
    const c = new Connection(address, timeoutMs);
    await new Promise<void>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error(`connect timeout to ${address}`)), timeoutMs);
      c.socket.once("connect", () => {
        clearTimeout(t);
        resolve();
      });
      c.socket.once("error", (e) => {
        clearTimeout(t);
        reject(e);
      });
    });
    if (token) {
      const r = await c.call(w.OP.AUTH, new TextEncoder().encode(token));
      if (r.status !== w.ST.OK) {
        c.close();
        throw new UnauthorizedError("authentication failed: " + new TextDecoder().decode(r.body), r.status);
      }
    }
    if (followerReads) await c.call(w.OP.SET_OPTIONS, Uint8Array.of(1));
    return c;
  }

  get isClosed(): boolean {
    return this.closed;
  }

  /** Sends one request; requests on one connection are serialized. */
  call(op: number, body: Uint8Array): Promise<Reply> {
    const run = () =>
      new Promise<Reply>((resolve, reject) => {
        if (this.closed) return reject(new Error("connection closed"));
        const hdr = Buffer.alloc(29);
        hdr[0] = op;
        Buffer.from(ZERO_KEY).copy(hdr, 1);
        hdr.writeUInt32LE(body.length, 25);
        const timer = setTimeout(() => {
          this.waiter = undefined;
          this.close();
          reject(new Error("request timed out"));
        }, this.timeoutMs);
        this.waiter = { resolve, reject, timer };
        this.socket.write(Buffer.concat([hdr, Buffer.from(body.buffer, body.byteOffset, body.byteLength)]));
      });
    const p = this.chain.then(run, run);
    this.chain = p.catch(() => undefined);
    return p;
  }

  private onData(chunk: Buffer): void {
    this.buffer = this.buffer.length ? Buffer.concat([this.buffer, chunk]) : chunk;
    if (!this.waiter || this.buffer.length < 13) return;
    const n = this.buffer.readUInt32LE(9);
    if (n > MAX_BODY) {
      this.waiter.reject(new ServerError("response too large"));
      this.close();
      return;
    }
    if (this.buffer.length < 13 + n) return;
    const status = this.buffer[0];
    const version = this.buffer.readBigUInt64LE(1);
    const body = new Uint8Array(this.buffer.subarray(13, 13 + n));
    this.buffer = this.buffer.subarray(13 + n);
    const waiter = this.waiter;
    this.waiter = undefined;
    clearTimeout(waiter.timer);
    waiter.resolve({ status, version, body });
  }

  close(): void {
    this.closed = true;
    this.socket.destroy();
  }
}

function splitAddress(address: string): [string, number] {
  const i = address.lastIndexOf(":");
  if (i < 0) return [address, 7878];
  return [address.slice(0, i).replace(/^\[|\]$/g, ""), Number(address.slice(i + 1))];
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Connection pool to a Storine server or cluster. Create it with `connect()`. */
export class Client {
  private target: string;
  private idle: Connection[] = [];
  private readonly seeds: string[];
  private readonly timeoutMs: number;
  private readonly poolSize: number;

  constructor(seeds: string[], private readonly options: ConnectOptions = {}) {
    if (seeds.length === 0) throw new InvalidError("no server address given");
    this.seeds = seeds;
    this.target = seeds[0];
    this.timeoutMs = options.timeoutMs ?? 5000;
    this.poolSize = options.poolSize ?? 8;
  }

  /** A handle on the collection `name` (created on first write). */
  collection<T extends Fields = Fields>(name: string): Collection<T> {
    return new Collection<T>(this, name);
  }

  /** Buffered writes committed atomically: `await db.transaction(async (tx) => { tx.update(...) })`. */
  async transaction(body: (tx: Transaction) => void | Promise<void>): Promise<Array<bigint | null>> {
    const tx = new Transaction(this);
    await body(tx);
    return tx.commit();
  }

  /** Applies `writes` atomically, exactly once even across retries; returns the new versions. */
  async write(writes: w.Write[]): Promise<Array<bigint | null>> {
    const body = w.encodeWrites(writes, new Uint8Array(randomBytes(16)));
    const r = await this.request(w.OP.DOC_WRITE, body);
    if (r.status === w.ST.NOT_FOUND) {
      const err = new NotFoundError(new TextDecoder().decode(r.body.subarray(4)) || "document not found", r.status);
      if (r.body.length >= 4) err.writeIndex = new DataView(r.body.buffer, r.body.byteOffset).getUint32(0, true);
      throw err;
    }
    if (r.status !== w.ST.OK) throw Client.error(r);
    return w.decodeWriteResponse(r.body).versions;
  }

  /** Names of every declared collection. */
  async collections(): Promise<string[]> {
    const r = await this.request(w.OP.DOC_INFO, Uint8Array.of(3));
    if (r.status !== w.ST.OK) throw Client.error(r);
    const rd = new Reader(r.body);
    const out: string[] = [];
    for (let i = rd.count(17); i > 0; i--) out.push(w.decodeInfo(rd).name);
    return out;
  }

  /** Node id, leader, term and commit indexes of the server currently used. */
  async clusterInfo(): Promise<w.ClusterInfo> {
    const r = await this.request(w.OP.CLUSTER_INFO, new Uint8Array(0));
    if (r.status !== w.ST.OK) throw Client.error(r);
    return w.decodeClusterInfo(r.body);
  }

  /** Closes every pooled connection. */
  close(): void {
    for (const c of this.idle.splice(0)) c.close();
  }

  private async acquire(): Promise<Connection> {
    while (this.idle.length) {
      const c = this.idle.pop()!;
      if (!c.isClosed && c.address === this.target) return c;
      c.close();
    }
    return Connection.open(this.target, this.timeoutMs, this.options.token, this.options.followerReads);
  }

  private release(c: Connection): void {
    if (!c.isClosed && c.address === this.target && this.idle.length < this.poolSize) this.idle.push(c);
    else c.close();
  }

  private redirect(address: string | undefined, failed: Set<string>): void {
    if (address) {
      this.target = address;
      return;
    }
    let others = this.seeds.filter((s) => s !== this.target && !failed.has(s));
    if (others.length === 0) others = this.seeds.filter((s) => s !== this.target);
    if (others.length === 0) others = this.seeds;
    this.target = others[Math.floor(Math.random() * others.length)];
  }

  /** Sends a request, following leader redirects and retrying transient failures (reads and
   * request-id-carrying writes are safe to resend). Retries continue for up to 3x the timeout. */
  async request(op: number, body: Uint8Array): Promise<Reply> {
    const deadline = Date.now() + Math.max(3 * this.timeoutMs, 1000);
    let delay = 20;
    let last: unknown;
    const failed = new Set<string>();
    for (let attempt = 0; attempt < 1000 && (attempt < 8 || Date.now() < deadline); attempt++) {
      let c: Connection;
      try {
        c = await this.acquire();
      } catch (e) {
        if (e instanceof StorineError) throw e;
        last = e;
        failed.add(this.target);
        this.redirect(undefined, failed);
        await sleep(delay);
        delay = Math.min(delay * 2, 500);
        continue;
      }
      let r: Reply;
      try {
        r = await c.call(op, body);
      } catch (e) {
        c.close();
        last = e;
        failed.add(c.address);
        this.redirect(undefined, failed);
        await sleep(delay);
        delay = Math.min(delay * 2, 500);
        continue;
      }
      if (r.status === w.ST.NOT_LEADER) {
        c.close();
        let hint: string | undefined = new TextDecoder().decode(r.body) || undefined;
        if (hint && failed.has(hint)) hint = undefined;
        this.redirect(hint, failed);
        last = new UnavailableError("no cluster leader is available yet");
        if (!hint) {
          await sleep(delay);
          delay = Math.min(delay * 2, 500);
        }
        continue;
      }
      if (r.status === w.ST.COMMIT_UNKNOWN || r.status === w.ST.OVERLOADED) {
        c.close();
        last = r.status === w.ST.OVERLOADED ? new OverloadedError("server overloaded", r.status) : new UnavailableError("write outcome unknown");
        await sleep(delay);
        delay = Math.min(delay * 2, 500);
        continue;
      }
      if (r.status === w.ST.UNAUTHORIZED || r.status === w.ST.RATE_LIMITED) c.close();
      else this.release(c);
      return r;
    }
    if (last instanceof StorineError) throw last;
    throw new UnavailableError(`could not reach any server: ${String((last as Error)?.message ?? last)}`);
  }

  static error(r: Reply): StorineError {
    const text = new TextDecoder().decode(r.body);
    switch (r.status) {
      case w.ST.CONFLICT: {
        const c = w.decodeConflict(r.body);
        return new ConflictError(c.message, c.index, c.version);
      }
      case w.ST.NOT_FOUND:
        return new NotFoundError(text || "document not found", r.status);
      case w.ST.INVALID:
        return new InvalidError(text, r.status);
      case w.ST.UNAUTHORIZED:
        return new UnauthorizedError(text || "not authorized", r.status);
      case w.ST.RATE_LIMITED:
        return new RateLimitedError(text || "rate limited", r.status);
      case w.ST.OVERLOADED:
        return new OverloadedError(text || "server overloaded", r.status);
      default:
        return new ServerError(text || `server error (status ${r.status})`, r.status);
    }
  }
}

/**
 * Connects to a Storine server or cluster: `connect("storine://[token@]host:port[,host:port...]")`.
 * Connections are opened lazily; every method is async.
 */
export function connect(url = "storine://127.0.0.1:7878", options: ConnectOptions = {}): Client {
  let rest = url.includes("://") ? url : "storine://" + url;
  if (!rest.startsWith("storine://")) throw new InvalidError("connection strings start with storine://");
  rest = rest.slice("storine://".length);
  const [hostsPart, query = ""] = rest.split("?", 2);
  let hosts = hostsPart;
  const opts = { ...options };
  const at = hosts.lastIndexOf("@");
  if (at >= 0) {
    opts.token = opts.token ?? decodeURIComponent(hosts.slice(0, at));
    hosts = hosts.slice(at + 1);
  }
  for (const pair of query.split("&").filter(Boolean)) {
    const [k, v = ""] = pair.split("=");
    if (k === "timeout_ms") opts.timeoutMs = Number(v);
    if (k === "follower_reads") opts.followerReads = v === "1" || v === "true";
  }
  return new Client(hosts.split(",").filter(Boolean), opts);
}

function opsOf(changes: Array<w.FieldOp | Fields>): w.FieldOp[] {
  const ops: w.FieldOp[] = [];
  for (const c of changes) {
    if (typeof (c as w.FieldOp).kind === "number" && typeof (c as w.FieldOp).path === "string") ops.push(c as w.FieldOp);
    else for (const [k, v] of Object.entries(c as Fields)) ops.push(w.set(k, v));
  }
  if (ops.length === 0) throw new InvalidError("update() needs at least one change");
  return ops;
}

function putWrite(collection: string, key: string, fields: Fields, ttlSeconds: number | undefined, precondition: 0 | 1 | 2 | 3, version?: bigint): w.Write {
  if (ttlSeconds !== undefined && !(ttlSeconds > 0)) throw new InvalidError("ttl must be positive (seconds)");
  const ttlMs = ttlSeconds === undefined ? 0 : Math.max(1, Math.round(ttlSeconds * 1000));
  return { collection, key, kind: ttlMs ? 2 : 1, precondition, version, fields, ttlMs };
}

/** Optional version condition. */
export interface IfVersion {
  ifVersion?: bigint;
}

/** Query options. */
export interface FindOptions {
  orderBy?: w.OrderBy;
  limit?: number;
  offset?: number;
}

/** Options of `get`. */
export interface GetOptions {
  /** Only these (dotted) field paths, e.g. `["name", "meta.size"]` (at most 256). */
  fields?: readonly string[];
}

/** A slice of a bytes / text field, from `readBytes`. */
export interface ByteRange {
  /** Version of the document the bytes were read from. */
  version: bigint;
  /** Length of the whole value in bytes. */
  total: number;
  /** The bytes, starting at the requested offset. */
  data: Uint8Array;
}

/** Options of `nearest`. */
export interface NearestOptions {
  /** Number of documents to return (default 10). */
  k?: number;
  where?: w.Where;
  /** Score every candidate instead of using the vector index (default false). */
  exact?: boolean;
}

/** A named set of documents. Every method is one atomic, exactly-once operation. */
export class Collection<T extends Fields = Fields> {
  constructor(readonly client: Client, readonly name: string) {}

  /**
   * The document `key`, or `undefined` if it does not exist.
   *
   * With `fields` (paths such as `"name"` or `"meta.size"`), only those fields are sent, so metadata
   * can be read without transferring a large value stored beside it; nested paths come back nested.
   */
  async get(key: string, options: GetOptions = {}): Promise<Document<T> | undefined> {
    const req = options.fields === undefined ? w.encodeGet(this.name, key) : w.encodeGetFields(this.name, key, options.fields);
    const r = await this.client.request(w.OP.DOC_GET, req);
    if (r.status === w.ST.NOT_FOUND) return undefined;
    if (r.status !== w.ST.OK) throw Client.error(r);
    return { key, version: r.version, fields: new Reader(r.body).fields() as T };
  }

  /**
   * Bytes `offset .. offset + length` of a bytes (or text) field, without reading the rest of the
   * document; `undefined` if the document does not exist. `length` is at most 16 MiB (the default);
   * past the end of the value, `data` is empty. InvalidError if the field is missing or not bytes/text.
   */
  async readBytes(key: string, field: string, offset: number | bigint = 0, length: number = w.MAX_BYTE_RANGE): Promise<ByteRange | undefined> {
    const r = await this.client.request(w.OP.DOC_GET, w.encodeGetRange(this.name, key, field, offset, length));
    if (r.status === w.ST.NOT_FOUND) return undefined;
    if (r.status !== w.ST.OK) throw Client.error(r);
    const { total, data } = w.decodeByteRange(r.body);
    return { version: r.version, total, data };
  }

  /**
   * Yields a large bytes field chunk by chunk (each at most `chunkSize` bytes, default 1 MiB).
   * Throws NotFoundError if the document does not exist and ConflictError if it changes while being
   * read (the chunks would otherwise mix two versions).
   */
  async *streamBytes(key: string, field: string, options: { chunkSize?: number; offset?: number } = {}): AsyncGenerator<Uint8Array, void, undefined> {
    const chunkSize = options.chunkSize ?? 1 << 20;
    let offset = options.offset ?? 0;
    let version: bigint | undefined;
    for (;;) {
      const r = await this.readBytes(key, field, offset, chunkSize);
      if (r === undefined) throw new NotFoundError(`document ${JSON.stringify(key)} does not exist`);
      if (version !== undefined && r.version !== version) throw new ConflictError("the document changed while it was being read", undefined, r.version);
      version = r.version;
      if (r.data.length === 0) return;
      offset += r.data.length;
      yield r.data;
      if (offset >= r.total) return;
    }
  }

  /** Creates or replaces the document; returns its new version. `ttl` in seconds. */
  async put(key: string, fields: T, options: { ttl?: number } = {}): Promise<bigint> {
    return (await this.client.write([putWrite(this.name, key, fields, options.ttl, 0)]))[0] ?? 0n;
  }

  /** Creates the document; throws ConflictError if it already exists. */
  async insert(key: string, fields: T, options: { ttl?: number } = {}): Promise<bigint> {
    return (await this.client.write([putWrite(this.name, key, fields, options.ttl, 1)]))[0] ?? 0n;
  }

  /** Replaces an existing document (only at `ifVersion` when given); ConflictError otherwise. */
  async replace(key: string, fields: T, options: IfVersion = {}): Promise<bigint> {
    const pre = options.ifVersion !== undefined ? 3 : 2;
    return (await this.client.write([putWrite(this.name, key, fields, undefined, pre, options.ifVersion)]))[0] ?? 0n;
  }

  /**
   * Applies field changes atomically on the server; returns the new version. Changes are
   * `set`, `unset`, `add`, `append` operations or `{ field: value }` objects:
   * `accounts.update("alice", add("balance", new Decimal("-25"), { min: 0 }))`.
   * Throws NotFoundError if the document does not exist.
   */
  async update(key: string, ...changes: Array<w.FieldOp | Fields>): Promise<bigint> {
    return (await this.client.write([{ collection: this.name, key, kind: 4, ops: opsOf(changes) }]))[0] ?? 0n;
  }

  /** `update` that applies only if the document is at `version` (ConflictError otherwise). */
  async updateIfVersion(key: string, version: bigint, ...changes: Array<w.FieldOp | Fields>): Promise<bigint> {
    return (await this.client.write([{ collection: this.name, key, kind: 4, precondition: 3, version, ops: opsOf(changes) }]))[0] ?? 0n;
  }

  /** Deletes the document; resolves to false if it did not exist. */
  async delete(key: string, options: IfVersion = {}): Promise<boolean> {
    const pre = options.ifVersion !== undefined ? 3 : 2;
    try {
      await this.client.write([{ collection: this.name, key, kind: 3, precondition: pre, version: options.ifVersion }]);
      return true;
    } catch (e) {
      if (e instanceof ConflictError && options.ifVersion === undefined && e.currentVersion === undefined) return false;
      throw e;
    }
  }

  /** Documents matching `where` (a filter or `{ field: value }`); at most `limit` (default 1,000). */
  async find(where?: w.Where, options: FindOptions = {}): Promise<Array<Document<T>>> {
    return (await this.query(where, options, undefined)).docs;
  }

  /** The first matching document, or undefined. */
  async first(where?: w.Where, options: { orderBy?: w.OrderBy } = {}): Promise<Document<T> | undefined> {
    return (await this.find(where, { ...options, limit: 1 }))[0];
  }

  /** Number of matching documents. */
  async count(where?: w.Where): Promise<number> {
    return (await this.query(where, { limit: 1 }, undefined)).total;
  }

  /** Iterates over every matching document in key order: `for await (const d of c.scan()) ...`. */
  async *scan(where?: w.Where, batch = 1000): AsyncGenerator<Document<T>> {
    let after: string | undefined;
    for (;;) {
      const page = await this.query(where, { limit: batch }, after);
      yield* page.docs;
      if (page.next === undefined) return;
      after = page.next;
    }
  }

  /**
   * The `k` documents whose `field` (a list of numbers) is most similar to `vector` (cosine), best
   * first, among those matching `where`. Each document's similarity is in `doc.score` (also
   * `doc.fields["$score"]`). Uses the field's vector index when it has one (approximate candidates,
   * exact scores); `exact: true` scores every candidate.
   */
  async nearest(field: string, vector: ArrayLike<number>, options: NearestOptions = {}): Promise<Array<Document<T>>> {
    const k = options.k ?? 10;
    const docs = (await this.query(options.where, { limit: k }, undefined, { field, vector, k, exact: options.exact ?? false })).docs;
    return docs.map((d) => ({ ...d, score: d.fields["$score"] as number }));
  }

  private async query(where: w.Where, o: FindOptions, after: string | undefined, nearest?: w.NearestQuery) {
    const body = w.encodeQuery(this.name, w.toFilter(where), w.parseOrderBy(o.orderBy), o.limit ?? 0, o.offset ?? 0, after, nearest);
    const r = await this.client.request(w.OP.DOC_QUERY, body);
    if (r.status !== w.ST.OK) throw Client.error(r);
    const page = w.decodePage(r.body);
    return { total: page.total, next: page.next, docs: page.docs.map((d) => ({ key: d.key, version: d.version, fields: d.fields as T })) };
  }

  /**
   * Indexes a top-level field holding `dims` numbers for `nearest` (at most 4 per collection);
   * existing documents included. Writes of other values to that field are then refused.
   */
  async createVectorIndex(field: string, dims: number): Promise<void> {
    const n = new Uint8Array(4);
    new DataView(n.buffer).setUint32(0, dims, true);
    await this.admin(w.OP.DOC_ADMIN, concat(Uint8Array.of(7), w.str(this.name), w.str(field), n));
  }

  async dropVectorIndex(field: string): Promise<void> {
    await this.admin(w.OP.DOC_ADMIN, concat(Uint8Array.of(8), w.str(this.name), w.str(field)));
  }

  /** `[{ field, dims, ready }]` for the collection's vector indexes. */
  async vectorIndexes(): Promise<w.VectorIndexInfo[]> {
    const r = await this.admin(w.OP.DOC_INFO, concat(Uint8Array.of(9), w.str(this.name)));
    return w.decodeVectorIndexes(r.body);
  }

  /** Indexes a top-level field (at most 16 per collection); existing documents included. */
  async createIndex(field: string): Promise<void> {
    await this.admin(w.OP.DOC_ADMIN, concat(Uint8Array.of(1), w.str(this.name), w.str(field)));
  }

  async dropIndex(field: string): Promise<void> {
    await this.admin(w.OP.DOC_ADMIN, concat(Uint8Array.of(2), w.str(this.name), w.str(field)));
  }

  /** Deletes every document and the collection's metadata; returns the number deleted. */
  async drop(): Promise<number> {
    return Number((await this.admin(w.OP.DOC_ADMIN, concat(Uint8Array.of(4), w.str(this.name)))).version);
  }

  /** Indexes and metadata of the collection. */
  async info(): Promise<w.CollectionInfo> {
    const r = await this.admin(w.OP.DOC_INFO, concat(Uint8Array.of(6), w.str(this.name)));
    return w.decodeInfo(new Reader(r.body));
  }

  private async admin(op: number, body: Uint8Array): Promise<Reply> {
    const r = await this.client.request(op, body);
    if (r.status !== w.ST.OK) throw Client.error(r);
    return r;
  }
}

function concat(...parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let o = 0;
  for (const p of parts) {
    out.set(p, o);
    o += p.length;
  }
  return out;
}

/** Writes buffered locally and committed atomically (all or none). */
export class Transaction {
  readonly writes: w.Write[] = [];
  constructor(readonly client: Client) {}

  put(collection: string, key: string, fields: Fields, options: { ttl?: number } = {}): this {
    this.writes.push(putWrite(collection, key, fields, options.ttl, 0));
    return this;
  }
  insert(collection: string, key: string, fields: Fields): this {
    this.writes.push(putWrite(collection, key, fields, undefined, 1));
    return this;
  }
  replace(collection: string, key: string, fields: Fields, options: IfVersion = {}): this {
    this.writes.push(putWrite(collection, key, fields, undefined, options.ifVersion !== undefined ? 3 : 2, options.ifVersion));
    return this;
  }
  update(collection: string, key: string, ...changes: Array<w.FieldOp | Fields>): this {
    this.writes.push({ collection, key, kind: 4, ops: opsOf(changes) });
    return this;
  }
  delete(collection: string, key: string, options: IfVersion = {}): this {
    this.writes.push({ collection, key, kind: 3, precondition: options.ifVersion !== undefined ? 3 : 0, version: options.ifVersion });
    return this;
  }
  /** Commit only if the document is at `version` (or exists / does not exist). */
  require(collection: string, key: string, condition: { version?: bigint; exists?: boolean } = {}): this {
    if (condition.version !== undefined) this.writes.push({ collection, key, kind: 5, precondition: 3, version: condition.version });
    else this.writes.push({ collection, key, kind: 5, precondition: condition.exists === false ? 1 : 2 });
    return this;
  }
  /** Applies every buffered write atomically; returns the new versions in order. */
  async commit(): Promise<Array<bigint | null>> {
    if (this.writes.length === 0) return [];
    const writes = this.writes.splice(0);
    return this.client.write(writes);
  }
}

export type { Value };
