/** Analytics requests and responses: `OP_DOC_AGGREGATE` / `OP_DOC_SCAN_COLUMNS` (protocol/PROTOCOL.md §5.5-§5.6). */

import { InvalidError } from "./errors";
import { Decimal, encodeValue, Fields, Reader, Value, Writer } from "./values";
import { encodeFilter, Filter, OrderBy, parseOrderBy, toFilter, Where } from "./wire";

export const OP_DOC_AGGREGATE = 0x29;
export const OP_DOC_SCAN_COLUMNS = 0x2a;

const MAX_ITEMS = 64;
const MAX_EXPR_DEPTH = 32;
const U32_MAX = 0xffff_ffff;

// ── Expressions ──────────────────────────────────────────────────────────────────────────────

/**
 * Something usable as an expression: an `Expr`, a field path (`"price"` is `field("price")`), or a
 * constant (`number`, `bigint`, `Decimal`, ... becomes `lit(value)`).
 */
export type ExprLike = Expr | string | number | bigint | Decimal | Value;

/** An arithmetic expression over a document's fields: `field("price").times("qty").minus(1)`. */
export class Expr {
  /** @internal: 1 field, 2 const, 3 add, 4 sub, 5 mul, 6 neg. */
  private constructor(readonly op: 1 | 2 | 3 | 4 | 5 | 6, readonly path?: string, readonly value?: Value, readonly args: readonly Expr[] = []) {}

  /** @internal */
  static make(op: 1 | 2 | 3 | 4 | 5 | 6, path?: string, value?: Value, args: readonly Expr[] = []): Expr {
    return new Expr(op, path, value, args);
  }

  /** `this + x`. */
  plus(x: ExprLike): Expr {
    return new Expr(3, undefined, undefined, [this, toExpr(x)]);
  }
  /** `this - x`. */
  minus(x: ExprLike): Expr {
    return new Expr(4, undefined, undefined, [this, toExpr(x)]);
  }
  /** `this * x`. */
  times(x: ExprLike): Expr {
    return new Expr(5, undefined, undefined, [this, toExpr(x)]);
  }
  /** `-this`. */
  neg(): Expr {
    return new Expr(6, undefined, undefined, [this]);
  }
}

/** The value of the (dotted) field `path`. */
export const field = (path: string): Expr => {
  if (typeof path !== "string") throw new InvalidError("a field path is a string");
  return Expr.make(1, path);
};
/** A constant (`lit(new Decimal("2.5"))`; a fractional `number` is a float). */
export const lit = (value: Value): Expr => Expr.make(2, undefined, value);

/** A string is a field path; an `Expr` is itself; anything else is a constant. */
export function toExpr(x: ExprLike): Expr {
  if (x instanceof Expr) return x;
  if (typeof x === "string") return field(x);
  return lit(x as Value);
}

function encodeExpr(e: Expr, w: Writer, depth = 0): void {
  if (depth > MAX_EXPR_DEPTH) throw new InvalidError("expression nesting is too deep (max 32)");
  w.u8(e.op);
  if (e.op === 1) w.str(e.path ?? "");
  else if (e.op === 2) encodeValue(e.value ?? null, w);
  else for (const a of e.args) encodeExpr(a, w, depth + 1);
}

// ── Aggregates ───────────────────────────────────────────────────────────────────────────────

/** One aggregate (build with count, sum, avg, min, max, countDistinct, approxCountDistinct). */
export class Aggregate {
  /** `func`: 1 count, 2 sum, 3 avg, 4 min, 5 max, 6 count_distinct, 7 approx_count_distinct. */
  constructor(readonly func: 1 | 2 | 3 | 4 | 5 | 6 | 7, readonly arg?: Expr) {}
}

/** `count()` counts rows; `count(arg)` counts rows where `arg` is not null. */
export const count = (arg?: ExprLike): Aggregate => new Aggregate(1, arg === undefined ? undefined : toExpr(arg));
/** Exact sum of integers / decimals (a float sum if any input is a float); null over no values. */
export const sum = (arg: ExprLike): Aggregate => new Aggregate(2, toExpr(arg));
/** Mean: a `Decimal` for exact inputs (6 more fractional digits, half-even), a float otherwise. */
export const avg = (arg: ExprLike): Aggregate => new Aggregate(3, toExpr(arg));
/** Smallest value in the document sort order (nulls ignored). */
export const min = (arg: ExprLike): Aggregate => new Aggregate(4, toExpr(arg));
/** Largest value in the document sort order (nulls ignored). */
export const max = (arg: ExprLike): Aggregate => new Aggregate(5, toExpr(arg));
/** Exact number of distinct non-null values. */
export const countDistinct = (arg: ExprLike): Aggregate => new Aggregate(6, toExpr(arg));
/** HyperLogLog estimate of the number of distinct values (about 0.8 % standard error). */
export const approxCountDistinct = (arg: ExprLike): Aggregate => new Aggregate(7, toExpr(arg));

/** Output name -> aggregate, in output order: `{ n: count(), revenue: sum(field("price").times("qty")) }` or pairs. */
export type Aggregates = Record<string, Aggregate> | ReadonlyArray<readonly [string, Aggregate]>;

// ── Group keys ───────────────────────────────────────────────────────────────────────────────

/** Groups by `offset + floor((v - offset) / width) * width` of a numeric field. */
export interface Bucket {
  readonly kind: 2;
  readonly field: string;
  readonly width: bigint;
  readonly offset: bigint;
}

/** Group by buckets of `width` of an integer field (e.g. weeks of a day number), shifted by `offset`. */
export function bucket(fieldPath: string, width: number | bigint, offset: number | bigint = 0): Bucket {
  const toI64 = (x: number | bigint, what: string): bigint => {
    if (typeof x === "number" && !Number.isSafeInteger(x)) throw new InvalidError(`bucket ${what} must be an integer`);
    const b = BigInt(x);
    if (b < -(1n << 63n) || b >= 1n << 63n) throw new InvalidError(`bucket ${what} does not fit in 64 bits`);
    return b;
  };
  const wd = toI64(width, "width");
  if (wd <= 0n) throw new InvalidError("bucket width must be positive");
  return { kind: 2, field: fieldPath, width: wd, offset: toI64(offset, "offset") };
}

/** A field path or a `bucket(...)`. */
export type GroupKey = string | Bucket;

/** Options of `aggregate`. */
export interface AggregateOptions {
  /** A filter or `{ field: value }`. */
  where?: Where;
  /** Group keys (field paths or `bucket(...)`); a group key's column is named by its field path. */
  groupBy?: GroupKey | GroupKey[];
  /** Output column names (group keys or aggregate names), `"-name"` for descending. */
  orderBy?: OrderBy;
  /** Most groups returned (default: every group). */
  limit?: number;
  /** Bytes of group tables kept in memory before spilling to disk (default 256 MiB on the server). */
  memoryBudget?: number | bigint;
  /** Bytes of spill runs allowed (default 16 GiB on the server). */
  spillBudget?: number | bigint;
}

/** The result of `aggregate`. */
export interface AggregateResult {
  /** One object per group: group-key columns first, then the aggregates in the order given. */
  rows: Fields[];
  /** Column names, in order. */
  columns: string[];
  /** Rows that matched the filter. */
  matched: number;
  /** Row groups skipped by zone maps without being read (columnar collections). */
  prunedRowGroups: number;
  /** Bytes spilled to disk (> 0: the query outgrew its memory budget). */
  spilledBytes: number;
}

function aggregateEntries(aggregates: Aggregates): Array<[string, Aggregate]> {
  if (aggregates === null || typeof aggregates !== "object") throw new InvalidError("aggregates is an object or a list of [name, aggregate] pairs");
  const entries = Array.isArray(aggregates) ? aggregates.map(([n, a]) => [n, a] as [string, Aggregate]) : Object.entries(aggregates as Record<string, Aggregate>);
  for (const [n, a] of entries) {
    if (typeof n !== "string") throw new InvalidError("aggregate names are strings");
    if (!(a instanceof Aggregate)) throw new InvalidError(`aggregate ${JSON.stringify(n)} is not built with count / sum / avg / min / max / countDistinct / approxCountDistinct`);
  }
  return entries;
}

function budget(x: number | bigint | undefined, what: string): bigint {
  if (x === undefined) return 0n;
  if (typeof x === "number" && !Number.isSafeInteger(x)) throw new InvalidError(`${what} must be an integer`);
  const b = BigInt(x);
  if (b < 0n || b >= 1n << 64n) throw new InvalidError(`${what} must be between 0 and 2^64 - 1`);
  return b;
}

/** Encodes an `OP_DOC_AGGREGATE` request; also returns the output column names. */
export function encodeAggregate(collection: string, aggregates: Aggregates, options: AggregateOptions = {}): { body: Uint8Array; columns: string[] } {
  const aggs = aggregateEntries(aggregates);
  const keys: GroupKey[] = options.groupBy === undefined ? [] : Array.isArray(options.groupBy) ? options.groupBy : [options.groupBy];
  if (keys.length > MAX_ITEMS) throw new InvalidError("at most 64 group keys");
  if (aggs.length > MAX_ITEMS) throw new InvalidError("at most 64 aggregates");
  const columns = [...keys.map((k) => (typeof k === "string" ? k : k.field)), ...aggs.map(([n]) => n)];
  const index = new Map<string, number>();
  columns.forEach((c, i) => {
    if (index.has(c)) throw new InvalidError(`duplicate output column ${JSON.stringify(c)}`);
    index.set(c, i);
  });
  const order = parseOrderBy(options.orderBy).map(([name, desc]) => {
    const i = index.get(name);
    if (i === undefined) throw new InvalidError(`orderBy ${JSON.stringify(name)} is not an output column (${columns.join(", ")})`);
    return [i, desc] as const;
  });
  if (order.length > MAX_ITEMS) throw new InvalidError("at most 64 order columns");
  const limit = options.limit ?? 0;
  if (!Number.isInteger(limit) || limit < 0 || limit > U32_MAX) throw new InvalidError("limit must be a non-negative integer");

  const filter = toFilter(options.where);
  const w = new Writer();
  w.str(collection);
  if (filter) {
    w.u8(1);
    encodeFilter(filter, w);
  } else {
    w.u8(0);
  }
  w.u32(keys.length);
  for (const k of keys) {
    if (typeof k === "string") {
      w.u8(1);
      w.str(k);
    } else if (k && k.kind === 2) {
      w.u8(2);
      w.str(k.field);
      w.i64(k.width);
      w.i64(k.offset);
    } else {
      throw new InvalidError("group keys are field paths or bucket(...)");
    }
  }
  w.u32(aggs.length);
  for (const [, a] of aggs) {
    w.u8(a.func);
    if (a.arg === undefined) {
      if (a.func !== 1) throw new InvalidError("only count may omit its argument");
      w.u8(0);
    } else {
      w.u8(1);
      encodeExpr(a.arg, w);
    }
  }
  w.u32(order.length);
  for (const [i, desc] of order) {
    w.u32(i);
    w.u8(desc ? 1 : 0);
  }
  w.u32(limit);
  w.u64(budget(options.memoryBudget, "memoryBudget"));
  w.u64(budget(options.spillBudget, "spillBudget"));
  return { body: w.finish(), columns };
}

/** `u64 matched | u64 pruned | u64 spilled | u32 columns | u32 rows | values` (rows as arrays). */
export function decodeAggregateResult(body: Uint8Array): { matched: number; prunedRowGroups: number; spilledBytes: number; columns: number; rows: Value[][] } {
  const r = new Reader(body);
  const matched = Number(r.u64());
  const prunedRowGroups = Number(r.u64());
  const spilledBytes = Number(r.u64());
  const columns = r.u32();
  const n = r.u32();
  if (columns > 0 && n * columns > r.remaining()) throw new InvalidError("truncated response");
  const rows: Value[][] = [];
  for (let i = 0; i < n; i++) {
    const row: Value[] = [];
    for (let j = 0; j < columns; j++) row.push(r.value());
    rows.push(row);
  }
  if (r.remaining()) throw new InvalidError("trailing bytes after aggregate result");
  return { matched, prunedRowGroups, spilledBytes, columns, rows };
}

/** Names each row's values: `[["eu", 3]]` with `["region", "n"]` -> `[{ region: "eu", n: 3 }]`. */
export function namedRows(names: readonly string[], rows: readonly Value[][]): Fields[] {
  return rows.map((row) => {
    if (row.length !== names.length) throw new InvalidError(`expected ${names.length} columns, the server sent ${row.length}`);
    const out: Fields = {};
    // defineProperty: a column named "__proto__" stays data.
    names.forEach((n, i) => Object.defineProperty(out, n, { value: row[i], enumerable: true, writable: true, configurable: true }));
    return out;
  });
}

// ── Column scans ─────────────────────────────────────────────────────────────────────────────

/** Options of `scanColumns`. */
export interface ScanColumnsOptions {
  /** A filter or `{ field: value }`. */
  where?: Where;
  /** Rows per round trip (default 1,000; at most 100,000). */
  pageSize?: number;
}

/** One row of `scanColumns`: the document key and the requested fields (by path; missing -> null). */
export interface ScanRow {
  key: string;
  fields: Fields;
}

/** Encodes an `OP_DOC_SCAN_COLUMNS` request. */
export function encodeScanColumns(collection: string, filter: Filter | undefined, fields: readonly string[], afterKey: string | undefined, limit: number): Uint8Array {
  if (typeof fields === "string" || !Array.isArray(fields)) throw new InvalidError("fields is a list of field paths");
  if (fields.length > MAX_ITEMS) throw new InvalidError("at most 64 fields can be scanned");
  if (!Number.isInteger(limit) || limit < 0 || limit > 100_000) throw new InvalidError("pageSize must be between 1 and 100,000");
  const w = new Writer();
  w.str(collection);
  if (filter) {
    w.u8(1);
    encodeFilter(filter, w);
  } else {
    w.u8(0);
  }
  w.u32(fields.length);
  for (const f of fields) w.str(f);
  if (afterKey === undefined) {
    w.u8(0);
  } else {
    w.u8(1);
    w.str(afterKey);
  }
  w.u32(limit);
  return w.finish();
}

/** `u64 total | u8 has_next | str next | u32 n_fields | u32 rows | (str key | values)*`. */
export function decodeScanPage(body: Uint8Array): { total: number; next?: string; fields: number; rows: Array<{ key: string; values: Value[] }> } {
  const r = new Reader(body);
  const total = Number(r.u64());
  const next = r.u8() ? r.str() : undefined;
  const fields = r.u32();
  const n = r.count(4 + fields);
  const rows: Array<{ key: string; values: Value[] }> = [];
  for (let i = 0; i < n; i++) {
    const key = r.str();
    const values: Value[] = [];
    for (let j = 0; j < fields; j++) values.push(r.value());
    rows.push({ key, values });
  }
  if (r.remaining()) throw new InvalidError("trailing bytes after scan page");
  return { total, next, fields, rows };
}
