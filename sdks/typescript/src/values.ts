/** Value types and the Storine Value Encoding (protocol/PROTOCOL.md §4). */

import { InvalidError } from "./errors";

const I64_MIN = -(1n << 63n);
const I64_MAX = (1n << 63n) - 1n;
const I128_MIN = -(1n << 127n);
const I128_MAX = (1n << 127n) - 1n;
const MAX_SCALE = 38;
const MAX_DEPTH = 32;

/** Exact decimal number (money): `mantissa / 10^scale`, up to 38 digits. Never a binary float. */
export class Decimal {
  readonly mantissa: bigint;
  readonly scale: number;

  /** `new Decimal("19.99")`, `new Decimal(5n)`, or `new Decimal(1999n, 2)`. */
  constructor(value: string | bigint | number, scale?: number) {
    if (typeof value === "bigint") {
      this.mantissa = value;
      this.scale = scale ?? 0;
    } else if (typeof value === "number") {
      if (!Number.isSafeInteger(value)) {
        throw new InvalidError("Decimal from a number must be a safe integer; pass a string for fractions (\"0.1\")");
      }
      this.mantissa = BigInt(value);
      this.scale = scale ?? 0;
    } else {
      const parsed = Decimal.parse(value);
      this.mantissa = parsed[0];
      this.scale = parsed[1];
    }
    if (!Number.isInteger(this.scale) || this.scale < 0 || this.scale > MAX_SCALE) {
      throw new InvalidError("decimal scale must be 0..38");
    }
    if (this.mantissa < I128_MIN || this.mantissa > I128_MAX) {
      throw new InvalidError("decimal does not fit in 38 digits");
    }
  }

  private static parse(text: string): [bigint, number] {
    const m = /^\s*([+-]?)(\d*)(?:\.(\d*))?(?:[eE]([+-]?\d+))?\s*$/.exec(text);
    if (!m || (m[2] === "" && (m[3] === undefined || m[3] === ""))) {
      throw new InvalidError(`not a decimal number: ${JSON.stringify(text)}`);
    }
    const frac = m[3] ?? "";
    let digits = BigInt((m[2] || "0") + frac);
    let scale = frac.length - Number(m[4] ?? "0");
    if (digits === 0n) {
      return [0n, Math.max(0, Math.min(frac.length, MAX_SCALE))];
    }
    if (scale < -40 || scale > 200) throw new InvalidError("decimal out of range");
    while (scale < 0) {
      digits *= 10n;
      scale += 1;
    }
    while (scale > MAX_SCALE && digits % 10n === 0n) {
      digits /= 10n;
      scale -= 1;
    }
    if (scale > MAX_SCALE) throw new InvalidError("decimals have at most 38 fractional digits");
    return [m[1] === "-" ? -digits : digits, scale];
  }

  /** Plain notation with exactly `scale` fractional digits. */
  toString(): string {
    const neg = this.mantissa < 0n;
    let s = (neg ? -this.mantissa : this.mantissa).toString();
    if (this.scale > 0) {
      s = s.padStart(this.scale + 1, "0");
      s = s.slice(0, s.length - this.scale) + "." + s.slice(s.length - this.scale);
    }
    return (neg ? "-" : "") + s;
  }

  toJSON(): string {
    return this.toString();
  }

  /** Nearest JavaScript number (lossy: for display only, never for money arithmetic). */
  toNumber(): number {
    return Number(this.toString());
  }

  /** Numeric comparison: -1, 0 or 1 (`1.50` equals `1.5`). */
  compare(other: Decimal): number {
    const s = Math.max(this.scale, other.scale);
    const a = this.mantissa * 10n ** BigInt(s - this.scale);
    const b = other.mantissa * 10n ** BigInt(s - other.scale);
    return a < b ? -1 : a > b ? 1 : 0;
  }

  equals(other: Decimal): boolean {
    return this.compare(other) === 0;
  }
}

/** Timestamp with nanosecond precision (returned when a `Date` would lose precision). */
export class Timestamp {
  constructor(readonly ns: bigint) {}
  toDate(): Date {
    return new Date(Number(this.ns / 1_000_000n));
  }
}

/** UUID value (16 bytes, RFC 4122 order). `new Uuid("123e4567-e89b-12d3-a456-426614174000")`. */
export class Uuid {
  readonly bytes: Uint8Array;
  constructor(value: string | Uint8Array) {
    if (typeof value === "string") {
      const hex = value.replace(/-/g, "");
      if (!/^[0-9a-fA-F]{32}$/.test(hex)) throw new InvalidError("not a UUID");
      this.bytes = Uint8Array.from(hex.match(/../g)!.map((h) => parseInt(h, 16)));
    } else {
      if (value.length !== 16) throw new InvalidError("a UUID is 16 bytes");
      this.bytes = Uint8Array.from(value);
    }
  }
  toString(): string {
    const h = Array.from(this.bytes, (b) => b.toString(16).padStart(2, "0")).join("");
    return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
  }
}

/** Engine id value (24 raw bytes). */
export class Id {
  readonly bytes: Uint8Array;
  constructor(bytes: Uint8Array) {
    if (bytes.length !== 24) throw new InvalidError("an Id is 24 bytes");
    this.bytes = Uint8Array.from(bytes);
  }
}

/** Forces an unsigned 64-bit integer (plain integers are signed). */
export class UInt64 {
  constructor(readonly value: bigint) {
    if (value < 0n || value > (1n << 64n) - 1n) throw new InvalidError("UInt64 out of range");
  }
}

/** Forces a float64 for an integral JavaScript number (`float(5)`); fractional numbers are floats already. */
export class Float {
  constructor(readonly value: number) {}
}

/** Wraps a number so it is stored as a float64 even when integral. */
export function float(value: number): Float {
  return new Float(value);
}

export type Value =
  | null
  | boolean
  | number
  | bigint
  | string
  | Uint8Array
  | Date
  | Decimal
  | Timestamp
  | Uuid
  | Id
  | UInt64
  | Float
  | Value[]
  | { [field: string]: Value };

/** A document's fields. */
export type Fields = { [field: string]: Value };

const enc = new TextEncoder();
const dec = new TextDecoder("utf-8", { fatal: true });

/** Growable little-endian byte writer. */
export class Writer {
  private buf = new Uint8Array(256);
  private view = new DataView(this.buf.buffer);
  length = 0;

  private reserve(n: number): void {
    if (this.length + n <= this.buf.length) return;
    let cap = this.buf.length * 2;
    while (cap < this.length + n) cap *= 2;
    const next = new Uint8Array(cap);
    next.set(this.buf.subarray(0, this.length));
    this.buf = next;
    this.view = new DataView(next.buffer);
  }
  u8(v: number): void {
    this.reserve(1);
    this.buf[this.length++] = v;
  }
  u32(v: number): void {
    this.reserve(4);
    this.view.setUint32(this.length, v, true);
    this.length += 4;
  }
  u64(v: bigint): void {
    this.reserve(8);
    this.view.setBigUint64(this.length, BigInt.asUintN(64, v), true);
    this.length += 8;
  }
  i64(v: bigint): void {
    this.reserve(8);
    this.view.setBigInt64(this.length, v, true);
    this.length += 8;
  }
  f64(v: number): void {
    this.reserve(8);
    this.view.setFloat64(this.length, v, true);
    this.length += 8;
  }
  i128(v: bigint): void {
    const u = BigInt.asUintN(128, v);
    this.u64(u & 0xffff_ffff_ffff_ffffn);
    this.u64(u >> 64n);
  }
  bytes(b: Uint8Array): void {
    this.reserve(b.length);
    this.buf.set(b, this.length);
    this.length += b.length;
  }
  str(s: string): void {
    const b = enc.encode(s);
    this.u32(b.length);
    this.bytes(b);
  }
  finish(): Uint8Array {
    return this.buf.slice(0, this.length);
  }
}

function dateToNs(d: Date): bigint {
  const ms = d.getTime();
  if (!Number.isFinite(ms)) throw new InvalidError("invalid Date");
  return BigInt(ms) * 1_000_000n;
}

function isPlainObject(v: unknown): v is { [k: string]: Value } {
  if (typeof v !== "object" || v === null) return false;
  const proto = Object.getPrototypeOf(v);
  return proto === Object.prototype || proto === null;
}

/** Appends the SVE encoding of `value`. */
export function encodeValue(value: Value, w: Writer, depth = 0): void {
  if (depth > MAX_DEPTH) throw new InvalidError("value nesting is too deep (max 32)");
  if (value === null || value === undefined) {
    w.u8(0);
  } else if (typeof value === "boolean") {
    w.u8(1);
    w.u8(value ? 1 : 0);
  } else if (typeof value === "number") {
    if (Number.isInteger(value) && Number.isSafeInteger(value)) {
      w.u8(2);
      w.i64(BigInt(value));
    } else if (Number.isInteger(value)) {
      throw new InvalidError("integer beyond 2^53 loses precision as a number: use a bigint");
    } else {
      w.u8(4);
      w.f64(value);
    }
  } else if (typeof value === "bigint") {
    if (value >= I64_MIN && value <= I64_MAX) {
      w.u8(2);
      w.i64(value);
    } else if (value >= I128_MIN && value <= I128_MAX) {
      w.u8(5);
      w.i128(value);
    } else {
      throw new InvalidError("integer does not fit in 128 bits");
    }
  } else if (typeof value === "string") {
    w.u8(9);
    w.str(value);
  } else if (value instanceof Float) {
    w.u8(4);
    w.f64(value.value);
  } else if (value instanceof UInt64) {
    w.u8(3);
    w.u64(value.value);
  } else if (value instanceof Decimal) {
    w.u8(11);
    w.i128(value.mantissa);
    w.u8(value.scale);
  } else if (value instanceof Timestamp) {
    w.u8(6);
    w.i64(value.ns);
  } else if (value instanceof Date) {
    w.u8(6);
    w.i64(dateToNs(value));
  } else if (value instanceof Uuid) {
    w.u8(7);
    w.bytes(value.bytes);
  } else if (value instanceof Id) {
    w.u8(8);
    w.bytes(value.bytes);
  } else if (value instanceof Uint8Array) {
    w.u8(10);
    w.u32(value.length);
    w.bytes(value);
  } else if (Array.isArray(value)) {
    w.u8(12);
    w.u32(value.length);
    for (const item of value) encodeValue(item, w, depth + 1);
  } else if (value instanceof Map) {
    w.u8(13);
    encodeFields(Object.fromEntries(value as Map<string, Value>), w, depth + 1);
  } else if (isPlainObject(value)) {
    w.u8(13);
    encodeFields(value, w, depth + 1);
  } else {
    throw new InvalidError(`cannot store a value of type ${(value as object).constructor?.name ?? typeof value}`);
  }
}

/** Appends `u32 count | (str key, value)*` with keys in ascending byte order. */
export function encodeFields(fields: Fields, w: Writer, depth = 0): void {
  if (!isPlainObject(fields)) throw new InvalidError("a document is a plain object of fields");
  const entries = Object.entries(fields)
    .filter(([, v]) => v !== undefined)
    .map(([k, v]) => [enc.encode(k), k, v] as const);
  entries.sort((a, b) => compareBytes(a[0], b[0]));
  w.u32(entries.length);
  for (const [raw, , v] of entries) {
    w.u32(raw.length);
    w.bytes(raw);
    encodeValue(v, w, depth);
  }
}

function compareBytes(a: Uint8Array, b: Uint8Array): number {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) if (a[i] !== b[i]) return a[i] - b[i];
  return a.length - b.length;
}

/** Bounds-checked reader over untrusted bytes. */
export class Reader {
  private view: DataView;
  pos = 0;
  constructor(private buf: Uint8Array) {
    this.view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  }
  remaining(): number {
    return this.buf.length - this.pos;
  }
  take(n: number): Uint8Array {
    if (n < 0 || this.pos + n > this.buf.length) throw new InvalidError("truncated response");
    const out = this.buf.subarray(this.pos, this.pos + n);
    this.pos += n;
    return out;
  }
  u8(): number {
    return this.take(1)[0];
  }
  u32(): number {
    const p = this.pos;
    this.take(4);
    return this.view.getUint32(p, true);
  }
  u64(): bigint {
    const p = this.pos;
    this.take(8);
    return this.view.getBigUint64(p, true);
  }
  i64(): bigint {
    const p = this.pos;
    this.take(8);
    return this.view.getBigInt64(p, true);
  }
  f64(): number {
    const p = this.pos;
    this.take(8);
    return this.view.getFloat64(p, true);
  }
  i128(): bigint {
    const lo = this.u64();
    const hi = this.u64();
    return BigInt.asIntN(128, (hi << 64n) | lo);
  }
  str(): string {
    const b = this.take(this.u32());
    try {
      return dec.decode(b);
    } catch {
      throw new InvalidError("invalid UTF-8 in response");
    }
  }
  count(minSize: number): number {
    const n = this.u32();
    if (n * Math.max(minSize, 1) > this.remaining()) throw new InvalidError("truncated response");
    return n;
  }
  value(depth = 0): Value {
    const tag = this.u8();
    switch (tag) {
      case 0:
        return null;
      case 1: {
        const b = this.u8();
        if (b > 1) throw new InvalidError("bool byte must be 0 or 1");
        return b === 1;
      }
      case 2: {
        const v = this.i64();
        return v >= BigInt(Number.MIN_SAFE_INTEGER) && v <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(v) : v;
      }
      case 3: {
        const v = this.u64();
        return v <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(v) : v;
      }
      case 4:
        return this.f64();
      case 5:
        return this.i128();
      case 6: {
        const ns = this.i64();
        return ns % 1_000_000n === 0n ? new Date(Number(ns / 1_000_000n)) : new Timestamp(ns);
      }
      case 7:
        return new Uuid(this.take(16));
      case 8:
        return new Id(this.take(24));
      case 9:
        return this.str();
      case 10:
        return Uint8Array.from(this.take(this.u32()));
      case 11: {
        const m = this.i128();
        const scale = this.u8();
        if (scale > MAX_SCALE) throw new InvalidError("decimal scale above 38");
        return new Decimal(m, scale);
      }
      case 12: {
        if (depth >= MAX_DEPTH) throw new InvalidError("value nesting is too deep");
        const n = this.count(1);
        const out: Value[] = [];
        for (let i = 0; i < n; i++) out.push(this.value(depth + 1));
        return out;
      }
      case 13:
        if (depth >= MAX_DEPTH) throw new InvalidError("value nesting is too deep");
        return this.fields(depth + 1);
      default:
        throw new InvalidError(`unknown value tag ${tag}`);
    }
  }
  fields(depth = 0): Fields {
    const n = this.count(5);
    const out: Fields = {};
    for (let i = 0; i < n; i++) {
      const k = this.str();
      if (Object.prototype.hasOwnProperty.call(out, k)) throw new InvalidError("duplicate map key");
      // defineProperty, not assignment: a field named "__proto__" stays an ordinary own property
      // (no prototype pollution from untrusted data).
      Object.defineProperty(out, k, { value: this.value(depth), enumerable: true, writable: true, configurable: true });
    }
    return out;
  }
}

/** Encodes one value. */
export function encode(value: Value): Uint8Array {
  const w = new Writer();
  encodeValue(value, w);
  return w.finish();
}

/** Decodes exactly one value. */
export function decode(bytes: Uint8Array): Value {
  const r = new Reader(bytes);
  const v = r.value();
  if (r.remaining() !== 0) throw new InvalidError("trailing bytes after value");
  return v;
}
