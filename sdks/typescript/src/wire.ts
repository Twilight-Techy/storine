/** Document API requests and responses (protocol/PROTOCOL.md §5-§6). */

import { InvalidError } from "./errors";
import { encodeFields, encodeValue, Fields, Reader, Value, Writer } from "./values";

export const OP = {
  AUTH: 0x0c,
  DOC_GET: 0x20,
  DOC_WRITE: 0x21,
  DOC_QUERY: 0x22,
  DOC_ADMIN: 0x23,
  DOC_INFO: 0x25,
  SET_OPTIONS: 0x26,
  CLUSTER_INFO: 0x27,
} as const;

export const ST = {
  OK: 0,
  NOT_FOUND: 1,
  ERROR: 2,
  OVERLOADED: 3,
  CONFLICT: 4,
  UNAUTHORIZED: 5,
  RATE_LIMITED: 6,
  NOT_LEADER: 7,
  INVALID: 8,
  COMMIT_UNKNOWN: 9,
} as const;

// ── Field operations ─────────────────────────────────────────────────────────────────────────

/** One atomic change applied server-side by `update` (build with set, unset, add, append). */
export interface FieldOp {
  readonly kind: 1 | 2 | 3 | 4;
  readonly path: string;
  readonly value?: Value;
  readonly min?: Value;
  readonly max?: Value;
  readonly hasMin?: boolean;
  readonly hasMax?: boolean;
}

/** Set the field at `path` (dotted for nested objects) to `value`. */
export const set = (path: string, value: Value): FieldOp => ({ kind: 1, path, value });
/** Remove the field at `path`. */
export const unset = (path: string): FieldOp => ({ kind: 2, path });
/**
 * Add `amount` (negative to subtract) exactly and atomically; refuse results below `min` /
 * above `max`. `add("balance", new Decimal("-25.00"), { min: 0 })` can never overdraw.
 */
export const add = (path: string, amount: Value, bounds: { min?: Value; max?: Value } = {}): FieldOp => ({
  kind: 3,
  path,
  value: amount,
  min: bounds.min,
  max: bounds.max,
  hasMin: "min" in bounds && bounds.min !== undefined,
  hasMax: "max" in bounds && bounds.max !== undefined,
});
/** Append `value` to the list at `path` (a missing field becomes a one-element list). */
export const append = (path: string, value: Value): FieldOp => ({ kind: 4, path, value });

function encodeOp(op: FieldOp, w: Writer): void {
  w.u8(op.kind);
  w.str(op.path);
  if (op.kind === 1 || op.kind === 4) encodeValue(op.value ?? null, w);
  if (op.kind === 3) {
    encodeValue(op.value ?? null, w);
    w.u8((op.hasMin ? 1 : 0) | (op.hasMax ? 2 : 0));
    if (op.hasMin) encodeValue(op.min ?? null, w);
    if (op.hasMax) encodeValue(op.max ?? null, w);
  }
}

// ── Filters ──────────────────────────────────────────────────────────────────────────────────

/** A query condition (build with eq, gt, between, and, ...). */
export interface Filter {
  readonly kind: number;
  readonly path?: string;
  readonly values?: readonly Value[];
  readonly children?: readonly Filter[];
}

/** Field equals value (numbers compare numerically; `null` matches missing fields). */
export const eq = (path: string, value: Value): Filter => ({ kind: 1, path, values: [value] });
export const ne = (path: string, value: Value): Filter => ({ kind: 2, path, values: [value] });
export const lt = (path: string, value: Value): Filter => ({ kind: 3, path, values: [value] });
export const le = (path: string, value: Value): Filter => ({ kind: 4, path, values: [value] });
export const gt = (path: string, value: Value): Filter => ({ kind: 5, path, values: [value] });
export const ge = (path: string, value: Value): Filter => ({ kind: 6, path, values: [value] });
/** `low <= field <= high`. */
export const between = (path: string, low: Value, high: Value): Filter => ({ kind: 7, path, values: [low, high] });
/** Field equals any of `values`. */
export const isIn = (path: string, values: Value[]): Filter => ({ kind: 8, path, values });
/** Text field starts with `text`. */
export const prefix = (path: string, text: string): Filter => ({ kind: 9, path, values: [text] });
/** Text field contains `value`, or list field has an element equal to `value`. */
export const contains = (path: string, value: Value): Filter => ({ kind: 10, path, values: [value] });
/** Field is present and not null. */
export const exists = (path: string): Filter => ({ kind: 11, path });
/** Field is absent or null. */
export const missing = (path: string): Filter => ({ kind: 12, path });
/** Document key starts with `text`. */
export const keyPrefix = (text: string): Filter => ({ kind: 13, values: [text] });
export const and = (...filters: Filter[]): Filter => ({ kind: 14, children: filters });
export const or = (...filters: Filter[]): Filter => ({ kind: 15, children: filters });
export const not = (filter: Filter): Filter => ({ kind: 16, children: [filter] });

/** A filter, a `{ field: value }` object (all equal), or nothing. */
export type Where = Filter | Fields | undefined;

function isFilter(x: unknown): x is Filter {
  return typeof x === "object" && x !== null && typeof (x as Filter).kind === "number" && Object.keys(x).every((k) => ["kind", "path", "values", "children"].includes(k));
}

export function toFilter(where: Where): Filter | undefined {
  if (where === undefined) return undefined;
  if (isFilter(where)) return where;
  const parts = Object.entries(where).map(([k, v]) => (isFilter(v) ? v : eq(k, v)));
  if (parts.length === 0) return undefined;
  return parts.length === 1 ? parts[0] : and(...parts);
}

function encodeFilter(f: Filter, w: Writer, depth = 0): void {
  if (depth > 32) throw new InvalidError("filter nesting is too deep (max 32)");
  w.u8(f.kind);
  const vals = f.values ?? [];
  switch (f.kind) {
    case 1: case 2: case 3: case 4: case 5: case 6: case 10:
      w.str(f.path ?? "");
      encodeValue(vals[0] ?? null, w);
      break;
    case 7:
      w.str(f.path ?? "");
      encodeValue(vals[0] ?? null, w);
      encodeValue(vals[1] ?? null, w);
      break;
    case 8:
      w.str(f.path ?? "");
      w.u32(vals.length);
      for (const v of vals) encodeValue(v, w);
      break;
    case 9:
      w.str(f.path ?? "");
      w.str(String(vals[0] ?? ""));
      break;
    case 11: case 12:
      w.str(f.path ?? "");
      break;
    case 13:
      w.str(String(vals[0] ?? ""));
      break;
    case 14: case 15:
      w.u32((f.children ?? []).length);
      for (const c of f.children ?? []) encodeFilter(c, w, depth + 1);
      break;
    case 16:
      encodeFilter((f.children ?? [])[0], w, depth + 1);
      break;
    default:
      throw new InvalidError(`unknown filter kind ${f.kind}`);
  }
}

// ── Requests ─────────────────────────────────────────────────────────────────────────────────

/** One write of an atomic request. */
export interface Write {
  collection: string;
  key: string;
  kind: 1 | 2 | 3 | 4 | 5; // put, put+ttl, delete, patch, check
  precondition?: 0 | 1 | 2 | 3; // none, absent, exists, version
  version?: bigint;
  fields?: Fields;
  ttlMs?: number;
  ops?: FieldOp[];
}

export function encodeGet(collection: string, key: string): Uint8Array {
  const w = new Writer();
  w.str(collection);
  w.str(key);
  return w.finish();
}

export function encodeWrites(writes: Write[], requestId?: Uint8Array): Uint8Array {
  const w = new Writer();
  if (requestId) {
    w.u8(1);
    w.bytes(requestId);
  } else {
    w.u8(0);
  }
  w.u32(writes.length);
  for (const x of writes) {
    w.str(x.collection);
    w.str(x.key);
    const pre = x.precondition ?? 0;
    w.u8(pre);
    if (pre === 3) w.u64(x.version ?? 0n);
    w.u8(x.kind);
    if (x.kind === 1) encodeFields(x.fields ?? {}, w);
    if (x.kind === 2) {
      w.u64(BigInt(x.ttlMs ?? 0));
      encodeFields(x.fields ?? {}, w);
    }
    if (x.kind === 4) {
      w.u32((x.ops ?? []).length);
      for (const op of x.ops ?? []) encodeOp(op, w);
    }
  }
  return w.finish();
}

/** `"price"`, `"-price"` (descending), or a list of those. */
export type OrderBy = string | string[] | undefined;

export function parseOrderBy(order: OrderBy): Array<[string, boolean]> {
  if (order === undefined) return [];
  const items = Array.isArray(order) ? order : [order];
  return items.map((it) => (it.startsWith("-") ? [it.slice(1), true] : [it, false]));
}

export function encodeQuery(collection: string, filter: Filter | undefined, order: Array<[string, boolean]>, limit: number, offset: number, afterKey?: string): Uint8Array {
  const w = new Writer();
  w.str(collection);
  if (filter) {
    w.u8(1);
    encodeFilter(filter, w);
  } else {
    w.u8(0);
  }
  w.u32(order.length);
  for (const [path, desc] of order) {
    w.str(path);
    w.u8(desc ? 1 : 0);
  }
  w.u32(limit);
  w.u32(offset);
  if (afterKey === undefined) {
    w.u8(0);
  } else {
    w.u8(1);
    w.str(afterKey);
  }
  return w.finish();
}

export function str(text: string): Uint8Array {
  const w = new Writer();
  w.str(text);
  return w.finish();
}

// ── Responses ────────────────────────────────────────────────────────────────────────────────

export function decodeWriteResponse(body: Uint8Array): { versions: Array<bigint | null>; duplicate: boolean } {
  const r = new Reader(body);
  const duplicate = r.u8() !== 0;
  const n = r.count(9);
  const versions: Array<bigint | null> = [];
  for (let i = 0; i < n; i++) {
    const exists = r.u8() !== 0;
    const v = r.u64();
    versions.push(exists ? v : null);
  }
  return { versions, duplicate };
}

export interface RawDoc {
  key: string;
  version: bigint;
  fields: Fields;
}

export function decodePage(body: Uint8Array): { total: number; next?: string; docs: RawDoc[] } {
  const r = new Reader(body);
  const total = r.u32();
  const next = r.u8() ? r.str() : undefined;
  const n = r.count(16);
  const docs: RawDoc[] = [];
  for (let i = 0; i < n; i++) {
    const key = r.str();
    const version = r.u64();
    docs.push({ key, version, fields: r.fields() });
  }
  return { total, next, docs };
}

export function decodeConflict(body: Uint8Array): { index?: number; version?: bigint; message: string } {
  if (body.length < 13) return { message: new TextDecoder().decode(body) };
  const v = new DataView(body.buffer, body.byteOffset, body.byteLength);
  const index = v.getUint32(0, true);
  const exists = body[4] !== 0;
  const version = v.getBigUint64(5, true);
  return {
    index: index === 0xffffffff ? undefined : index,
    version: exists ? version : undefined,
    message: new TextDecoder().decode(body.subarray(13)),
  };
}

export interface CollectionInfo {
  name: string;
  indexes: string[];
  building: string[];
  metadata: Fields;
}

export function decodeInfo(r: Reader): CollectionInfo {
  const name = r.str();
  const indexes: string[] = [];
  for (let i = r.count(4); i > 0; i--) indexes.push(r.str());
  const building: string[] = [];
  for (let i = r.count(4); i > 0; i--) building.push(r.str());
  return { name, indexes, building, metadata: r.fields() };
}

export interface ClusterInfo {
  nodeId: number;
  isLeader: boolean;
  leaderId?: number;
  leaderAddress?: string;
  term: bigint;
  commitIndex: bigint;
  lastApplied: bigint;
  members: Record<number, string>;
}

export function decodeClusterInfo(body: Uint8Array): ClusterInfo {
  const r = new Reader(body);
  const nodeId = Number(r.u64());
  const isLeader = r.u8() !== 0;
  const leaderId = Number(r.u64());
  const leaderAddress = r.str();
  const term = r.u64();
  const commitIndex = r.u64();
  const lastApplied = r.u64();
  const members: Record<number, string> = {};
  for (let i = r.count(12); i > 0; i--) {
    const id = Number(r.u64());
    members[id] = r.str();
  }
  return { nodeId, isLeader, leaderId: leaderId || undefined, leaderAddress: leaderAddress || undefined, term, commitIndex, lastApplied, members };
}
