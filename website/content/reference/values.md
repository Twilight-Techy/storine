---
title: Values
description: Every Storine value type, how each client represents it, how the console and SQL write it, and how it is encoded on the wire.
---

## Types

| Type | Range | Wire tag |
| :--- | :--- | :--- |
| null | | `0x00` |
| bool | | `0x01` |
| int64 | 64-bit signed | `0x02` |
| uint64 | 64-bit unsigned | `0x03` |
| float64 | IEEE 754 double | `0x04` |
| int128 | 128-bit signed | `0x05` |
| timestamp | nanoseconds since 1970-01-01T00:00:00Z, signed 64-bit | `0x06` |
| uuid | 16 bytes, RFC 4122 order | `0x07` |
| id | 24 opaque bytes | `0x08` |
| text | UTF-8 | `0x09` |
| bytes | raw bytes | `0x0A` |
| decimal | 128-bit mantissa and a scale of 0 to 38: the value is `mantissa / 10^scale` | `0x0B` |
| list | values in order | `0x0C` |
| map | text keys, unique, in byte order | `0x0D` |

Lists and maps nest up to 32 levels. Field names in a document must not be empty and must not contain a dot. The byte encoding of each type is in the [wire protocol](../../../protocol/PROTOCOL.md), section 4.

## In each client

| Type | Python | TypeScript | Go | Java | C# | Rust | PHP | Ruby |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| null | `None` | `null` | `nil` | `null` | `null` | `Value::Null` | `null` | `nil` |
| bool | `bool` | `boolean` | `bool` | `Boolean` | `bool` | `Value::Bool` | `bool` | `true`, `false` |
| int64 | `int` | `number` up to 2^53, else `bigint` | `int64` | `Long` | `long` | `Value::Int` | `int` | `Integer` |
| uint64 | `storine.UInt64` | `bigint` | `uint64` | `UInt64` | `ulong` | `Value::UInt` | `UInt64` | `Storine::UInt64` |
| int128 | `int` | `bigint` | `*big.Int` | `BigInteger` | `BigInteger`, `Int128` | `Value::Int128` | `Int128` | `Integer` |
| float64 | `float` | `number` (`float(5)` forces a float) | `float64` | `Double` | `double` | `Value::Float` | `float` | `Float` |
| decimal | `decimal.Decimal` | `Decimal` | `storine.Decimal` | `BigDecimal` | `decimal`, or `BigDecimal` beyond 28 digits | `Decimal` (`dec("19.99")`) | `Storine\Decimal` | `Storine::Decimal` (accepts `BigDecimal`) |
| timestamp | `datetime` (naive is taken as UTC) | `Date` | `time.Time` | `Instant` | `DateTime`, `DateTimeOffset`, `Timestamp` | `Value::Timestamp`, from `SystemTime` | `DateTimeInterface`, `Timestamp` | `Time` |
| uuid | `uuid.UUID` | `Uuid` | `storine.UUID` | `UUID` | `Guid` | `Value::Uuid` | `Uuid` | `Storine::Uuid` |
| id | `storine.Id` | | | | | `Value::Id` | `Id` | `Storine::Id` |
| text | `str` | `string` | `string` | `String` | `string` | `Value::Text` | `string` | `String` |
| bytes | `bytes` | `Uint8Array` | `[]byte` | `byte[]` | `byte[]` | `Value::Bytes` | `Bytes` | binary `String` (`"\x00".b`) |
| list | `list` | arrays | slices | collections, arrays | lists, arrays | `Value::List` (`list![...]`) | arrays with keys `0..n-1` | `Array` |
| map | `dict` | plain objects | `map[string]T`, `storine.Doc` | `Map<String, ?>` | `Doc`, dictionaries | `Value::Map` (`doc!{...}`) | other arrays, `stdClass` | `Hash` |

Decimals keep their scale: `Decimal("1.50")` comes back as `1.50`, not `1.5`. Each client's page has the details, such as how PHP tells an empty list from an empty map.

## In JSON: the console

The console shows and edits documents as JSON, so types that JSON lacks are written as one-key objects:

| Type | JSON form |
| :--- | :--- |
| decimal | `{"$decimal": "9.99"}` |
| timestamp | `{"$time": "2026-11-13T08:00:00Z"}` |
| bytes | `{"$bytes": "<base64>"}` |
| uuid | `{"$uuid": "6f1c2a7e-4b1d-4c2e-9a51-0e8f3b7d9c11"}` |
| id | `{"$id": "<48 hex digits>"}` |
| integer beyond 2^53 | `{"$int": "123456789012345678901"}` |
| NaN and infinities | `{"$float": "NaN"}`, `{"$float": "Infinity"}`, `{"$float": "-Infinity"}` |

A bare `1` is an integer and `1.0` a float. How SQL column types map to these values is in the SQL gateway's [SQL support](../../../crates/storage-sql/README.md#sql-support).

## Comparison and order

Numbers of every kind compare by value: `1`, `1.0` and `1.00` are equal in filters and in group keys. Ordered comparisons match only values of the same kind. Sorting uses one total order across kinds:

```text
null < bool < numbers < timestamps < text < bytes < uuid < id < list < map
```

with ties broken by document key.
