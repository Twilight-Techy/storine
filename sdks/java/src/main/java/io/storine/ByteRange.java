package io.storine;

/**
 * A slice of a bytes (or text) field read with {@link Collection#readBytes}: {@code data} starts at the requested
 * offset, {@code total} is the whole value's length and {@code version} the document version it was read from.
 */
public record ByteRange(long version, long total, byte[] data) {}
