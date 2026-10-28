/**
 * Storine Node.js client.
 *
 * ```ts
 * import { connect, add, Decimal } from "storine";
 * const db = connect("storine://127.0.0.1:7878");
 * const accounts = db.collection("accounts");
 * await accounts.insert("alice", { balance: new Decimal("100.00") });
 * await accounts.update("alice", add("balance", new Decimal("-25.00"), { min: 0 })); // no overdraft, no lost update
 * ```
 */

export { connect, Client, Collection, Transaction } from "./client";
export type { ConnectOptions, Document, FindOptions, IfVersion, NearestOptions } from "./client";
export {
  add, and, append, between, contains, eq, exists, ge, gt, isIn, keyPrefix, le, lt, missing, ne, not, or, prefix, set, unset,
} from "./wire";
export type { CollectionInfo, ClusterInfo, FieldOp, Filter, NearestQuery, OrderBy, VectorIndexInfo, Where } from "./wire";
export { Decimal, Float, float, Id, Timestamp, UInt64, Uuid } from "./values";
export type { Fields, Value } from "./values";
export {
  ConflictError, InvalidError, NotFoundError, OverloadedError, RateLimitedError, ServerError, StorineError,
  UnauthorizedError, UnavailableError,
} from "./errors";
