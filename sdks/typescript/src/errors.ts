/** Errors raised by the Storine client. Each says what happened and what to do about it. */

export class StorineError extends Error {
  constructor(message: string, readonly status?: number) {
    super(message);
    this.name = new.target.name;
  }
}

/** The document does not exist (e.g. `update` of a missing key). */
export class NotFoundError extends StorineError {
  writeIndex?: number;
}

/**
 * A condition did not hold: the document already exists, does not exist, or changed.
 * `writeIndex` is the failing write and `currentVersion` the document's version now
 * (`undefined` if it does not exist). Re-read and decide again.
 */
export class ConflictError extends StorineError {
  constructor(message: string, readonly writeIndex?: number, readonly currentVersion?: bigint) {
    super(message, 0x04);
  }
}

/** The request was rejected as invalid (bad value, type mismatch, bound violated). Do not retry. */
export class InvalidError extends StorineError {}

/** Authentication failed or the token's role does not allow this operation. */
export class UnauthorizedError extends StorineError {}

/** The server is temporarily overloaded. Retry later. */
export class OverloadedError extends StorineError {}

/** This connection exceeded its rate limit. Retry later. */
export class RateLimitedError extends OverloadedError {}

/** No server (or no cluster leader) could be reached in time. Retry later. */
export class UnavailableError extends StorineError {}

/** The server failed internally (I/O error...). */
export class ServerError extends StorineError {}
