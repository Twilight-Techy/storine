package io.storine;

/** A vector index of a collection ({@code ready}: built from the existing documents). */
public record VectorIndex(String field, int dims, boolean ready) {}
