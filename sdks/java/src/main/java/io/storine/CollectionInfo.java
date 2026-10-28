package io.storine;

import java.util.List;
import java.util.Map;

/** A collection's indexes ({@code building}: being built from existing documents) and metadata. */
public record CollectionInfo(String name, List<String> indexes, List<String> building, Map<String, Object> metadata) {}
