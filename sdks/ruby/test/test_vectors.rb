# frozen_string_literal: true

# Conformance: the SDK reproduces the server's encodings byte for byte (protocol/vectors).

require "minitest/autorun"
require "json"
require_relative "../lib/storine"

VECTORS = JSON.parse(File.read(File.expand_path("../../../protocol/vectors/vectors.json", __dir__)))

def unhex(h) = [h].pack("H*")
def hex(b) = b.unpack1("H*")

def native(desc)
  t = desc["type"]
  v = desc["value"]
  case t
  when "null" then nil
  when "bool" then v
  when "int64", "int128" then Integer(v)
  when "uint64" then Storine::UInt64.new(Integer(v))
  when "float64" then unhex(desc["bits"]).reverse.unpack1("E")
  when "timestamp"
    ns = Integer(v)
    Time.at(ns.div(1_000_000_000), ns % 1_000_000_000, :nsec, in: "UTC")
  when "uuid" then Storine::Uuid.new(unhex(v))
  when "id" then Storine::Id.new(unhex(v))
  when "text" then v
  when "bytes" then unhex(v)
  when "decimal" then Storine::Decimal(v)
  when "list" then v.map { |x| native(x) }
  when "map" then v.to_h { |k, x| [k, native(x)] }
  else raise "unknown vector type #{t}"
  end
end

# Values compared by type and exact content (decimals by text, so the scale counts).
def comparable(x)
  case x
  when Storine::Decimal then [:dec, x.to_s]
  when Storine::UInt64 then x.value
  when Float then [:float, [x].pack("E")]
  when Time then [:ts, (x.to_i * 1_000_000_000) + x.nsec]
  when Array then x.map { |i| comparable(i) }
  when Hash then x.transform_values { |i| comparable(i) }
  when String then [x.encoding == Encoding::BINARY ? :bytes : :text, x.b]
  else x
  end
end

class VectorTest < Minitest::Test
  W = Storine::Wire::Write

  def test_values_encode_and_decode
    VECTORS["values"].each do |vec|
      value = native(vec["value"])
      assert_equal vec["hex"], hex(Storine::SVE.encode_value(value)), vec["name"]
      decoded = Storine::SVE.decode(unhex(vec["hex"]))
      assert_equal [comparable(value)], [comparable(decoded)], vec["name"]
    end
  end

  def test_document_body
    doc = VECTORS["documents"][0]
    out = String.new(encoding: Encoding::BINARY)
    Storine::SVE.encode_map(native(doc["fields"]), out)
    assert_equal doc["hex"], hex(out)
  end

  def test_requests
    d = { "balance" => Storine::Decimal("100.00"), "owner" => "alice", "active" => true }
    rid = unhex("0102030405060708090a0b0c0d0e0f10")
    wire = Storine::Wire
    built = {
      "get accounts/alice" => wire.encode_get("accounts", "alice"),
      "get accounts/alice: bytes 1000..1004 of photo" => wire.encode_get_range("accounts", "alice", "photo", 1000, 4),
      "get accounts/alice: fields owner, meta.size" => wire.encode_get_fields("accounts", "alice", %w[owner meta.size]),
      "write: put" => wire.encode_writes([W.new("accounts", "alice", 1, 0, 0, d)], nil),
      "write: insert (must not exist) with request id" =>
        wire.encode_writes([W.new("accounts", "alice", 1, 1, 0, d)], "\xAB".b * 16),
      "write: replace if version 7" => wire.encode_writes([W.new("accounts", "alice", 1, 3, 7, d)], nil),
      "write: put with ttl 1500 ms" => wire.encode_writes([W.new("sessions", "s1", 2, 0, 0, { "u" => 1 }, 1500)], nil),
      "write: delete" => wire.encode_writes([W.new("accounts", "alice", 3)], nil),
      "write: delete if exists" => wire.encode_writes([W.new("accounts", "alice", 3, 2)], nil),
      "write: transfer 25.00 (two patches, no overdraft) with request id" => wire.encode_writes([
        W.new("accounts", "alice", 4, 2, 0, nil, 0, [Storine.add("balance", Storine::Decimal("-25.00"), min: 0)]),
        W.new("accounts", "bob", 4, 2, 0, nil, 0, [Storine.add("balance", Storine::Decimal("25.00"))])
      ], rid),
      "write: patch set / unset / append / add with min and max" => wire.encode_writes([
        W.new("c", "k", 4, 2, 0, nil, 0, [Storine.set("profile.name", "Ann"), Storine.unset("tmp"),
                                          Storine.append("log", 3), Storine.add("n", 1, min: 0, max: 10)])
      ], nil),
      "write: check version 3" => wire.encode_writes([W.new("c", "k", 5, 3, 3)], nil),
      "query: all (default limit)" => wire.encode_query("items", nil, [], 0, 0, nil),
      "query: eq text" => wire.encode_query("items", Storine.eq("kind", "book"), [], 10, 0, nil),
      "query: and(gte, lt) sorted desc, offset 20" => wire.encode_query(
        "items", Storine.ge("price", Storine::Decimal("10")) & Storine.lt("price", Storine::Decimal("20.50")),
        wire.parse_order_by("-price"), 10, 20, nil
      ),
      "query: between" => wire.encode_query("items", Storine.between("n", 1, 5), [], 0, 0, nil),
      "query: in" => wire.encode_query("items", Storine.in("n", [1, 2]), [], 0, 0, nil),
      "query: prefix / contains / exists / missing" => wire.encode_query("items", Storine.or(
        Storine.prefix("name", "An"), Storine.contains("tags", "x"), Storine.exists("email"), Storine.missing("deleted_at")
      ), [], 0, 0, nil),
      "query: not / ne / le / gt / key prefix" => wire.encode_query("items", Storine.and(
        ~Storine.ne("a", 1), Storine.le("b", 2.5), Storine.gt("c", 0), Storine.key_prefix("user:")
      ), [], 0, 0, nil),
      "query: after key cursor" => wire.encode_query("items", nil, [], 100, 0, "k0099"),
      "query: multi-key sort" => wire.encode_query("items", nil, wire.parse_order_by(["a", "-b"]), 5, 0, nil),
      "query: nearest neighbours with a filter" => wire.encode_query(
        "items", Storine.eq("kind", "book"), [], 5, 0, nil, ["embedding", [0.25, -1.5, 3.0], 5, false]
      ),
      "query: nearest neighbours, exact" => wire.encode_query("items", nil, [], 0, 0, nil, ["v", [1.0, 0.0], 1, true])
    }.merge(analytics_requests)
    seen = 0
    VECTORS["requests"].each do |vec|
      next unless built.key?(vec["name"])

      assert_equal vec["body_hex"], hex(built[vec["name"]]), vec["name"]
      seen += 1
    end
    assert_equal built.length, seen
  end

  # Built through the same path as Collection#aggregate / #scan_columns (minus the socket).
  def analytics_requests
    wire = Storine::Wire
    agg = lambda do |aggregates, where: nil, group_by: nil, order_by: nil, limit: nil, memory_budget: 0, spill_budget: 0|
      q = wire.build_aggregate(aggregates, group_by, order_by)
      wire.encode_aggregate("sales", wire.where_to_filter(where), q.group_by, q.aggregates, q.order, limit || 0,
                            memory_budget, spill_budget)
    end
    f = Storine.method(:field)
    {
      "aggregate: by region where day >= 20, count / sum(price * qty) / avg(qty), sum desc, limit 2" => agg.(
        { "n" => Storine.count, "revenue" => Storine.sum(f.("price") * "qty"), avg_qty: Storine.avg("qty") },
        where: Storine.ge("day", 20), group_by: ["region"], order_by: "-revenue", limit: 2
      ),
      "aggregate: totals (count distinct, approx count distinct, min, max, count of field)" => agg.(
        { "regions" => Storine.count_distinct("region"), "customers" => Storine.approx_count_distinct("customer"),
          "cheapest" => Storine.min("price"), "dearest" => Storine.max(f.("price")), "coupons" => Storine.count("coupon") }
      ),
      "aggregate: weekly buckets of day (width 7, offset 3) and region, sum(qty - 1) and sum(-price + 2.5), budgets" => agg.(
        { "q" => Storine.sum(f.("qty") - 1), "p" => Storine.sum(-f.("price") + Storine::Decimal("2.5")) },
        group_by: [Storine.bucket("day", 7, 3), "region"], order_by: ["day", "-region"],
        memory_budget: 1 << 20, spill_budget: 1 << 30
      ),
      "scan: qty, price where region = eu, limit 500" =>
        wire.encode_scan("sales", wire.where_to_filter({ "region" => "eu" }), %w[qty price], nil, 500),
      "scan: meta.size after s0099 (default limit)" => wire.encode_scan("sales", nil, ["meta.size"], "s0099", 0),
      "admin: enable columnar sales" => wire.encode_columnar("sales", true),
      "admin: disable columnar sales" => wire.encode_columnar("sales", false)
    }
  end

  def test_expressions
    f = Storine.method(:field)
    lit = Storine.method(:lit)
    assert_equal Storine::Expr.new(5, children: [lit.(2), f.("a")]), 2 * f.("a")
    assert_equal Storine::Expr.new(4, children: [lit.(1), f.("a")]), 1 - f.("a")
    assert_equal Storine::Expr.new(3, children: [lit.(1.5), f.("a")]), 1.5 + f.("a")
    d = Storine::Decimal("1.5")
    assert_equal Storine::Expr.new(5, children: [lit.(d), f.("a")]), d * f.("a")
    assert_equal Storine::Expr.new(5, children: [f.("a"), f.("b")]), f.("a") * "b"
    assert_equal Storine::Expr.new(6, children: [f.("a")]), -f.("a")
    assert_equal Storine::Aggregate.new(2, f.("a")), Storine.sum("a")
    assert_equal Storine::Aggregate.new(1, nil), Storine.count
    assert_raises(Storine::InvalidError) { f.("a") + nil }
    assert_raises(Storine::InvalidError) { Storine::Wire.build_aggregate({ "n" => Storine.count }, ["region"], "-nope") }
    assert_raises(Storine::InvalidError) { Storine::Wire.build_aggregate({ "region" => Storine.count }, ["region"], nil) }
    assert_raises(Storine::InvalidError) { Storine::Wire.build_aggregate({ "n" => "count" }, nil, nil) }
    assert_raises(Storine::InvalidError) { Storine.bucket("day", 1 << 64) }
  end

  def test_responses
    r = VECTORS["responses"].to_h { |v| [v["name"], v] }
    versions, dup = Storine::Wire.decode_write_response(unhex(r["write ok: [v12, deleted]"]["body_hex"]))
    assert_equal [[12, nil], false], [versions, dup]
    total, nxt, docs = Storine::Wire.decode_page(unhex(r["page: total 42, next k1, one doc"]["body_hex"]))
    assert_equal [42, "k1", [["k1", 3, { "n" => 1 }]]], [total, nxt, docs]
    range = unhex(r["byte range: total 5000, 4 bytes"]["body_hex"])
    assert_equal [5000, "\xDE\xAD\xBE\xEF".b], Storine::Wire.decode_byte_range(range)
    assert_raises(Storine::InvalidError) { Storine::Wire.decode_byte_range(range.byteslice(0...-1)) }
    agg = Storine::Wire.aggregate_result(%w[region n revenue],
                                         unhex(r["aggregate: matched 4, pruned 2, rows [eu, 3, 12.50] [us, 1, null]"]["body_hex"]))
    assert_equal [4, 2, 0], [agg.matched, agg.pruned_row_groups, agg.spilled_bytes]
    assert_equal comparable([["eu", 3, Storine::Decimal("12.50")], ["us", 1, nil]]), comparable(agg.map(&:values))
    assert_equal({ "region" => "us", "n" => 1, "revenue" => nil }, agg[1])
    total, nxt, nf, rows = Storine::Wire.decode_scan_page(unhex(r["scan page: total 2, next s1, row s1 [7, 9.99]"]["body_hex"]))
    assert_equal [2, "s1", 2], [total, nxt, nf]
    assert_equal comparable([["s1", [7, Storine::Decimal("9.99")]]]), comparable(rows)
  end

  def test_hostile_bytes_never_crash
    good = unhex(VECTORS["values"].last["hex"])
    (0...good.bytesize).each do |cut|
      assert_raises(Storine::InvalidError) { Storine::SVE.decode(good.byteslice(0, cut)) }
    end
    assert_raises(Storine::InvalidError) { Storine::SVE.decode("\x0C\xFF\xFF\xFF\xFF".b) }
  end

  def test_value_mapping_rules
    assert_raises(Storine::InvalidError) { Storine::SVE.encode_value(BigDecimal("NaN")) }
    assert_raises(Storine::InvalidError) { Storine::SVE.encode_value(1 << 130) }
    require "date"
    assert_raises(Storine::InvalidError) { Storine::SVE.encode_value(Date.new(2024, 1, 1)) }
    assert_equal "1000", Storine::SVE.decode(Storine::SVE.encode_value(Storine::Decimal("1E+3"))).to_s
    big = Storine::Decimal("1234567890123456789012345678.0123456789")
    assert_equal big.to_s, Storine::SVE.decode(Storine::SVE.encode_value(big)).to_s
    # BigDecimal is accepted (its value is exact; it does not keep trailing zeros).
    assert_equal Storine::Decimal("19.9"), Storine::SVE.decode(Storine::SVE.encode_value(BigDecimal("19.90")))
  end

  def test_decimal_arithmetic_is_exact
    a = Storine::Decimal("0.10")
    assert_equal "0.30", (a + Storine::Decimal("0.2")).to_s
    assert_equal "-0.05", (a - Storine::Decimal("0.15")).to_s
    assert_equal "0.0300", (a * Storine::Decimal("0.30")).to_s
    assert Storine::Decimal("1.50") == Storine::Decimal("1.5")
    assert Storine::Decimal("2") > 1
    assert_equal BigDecimal("75"), Storine::Decimal("75.00").to_d
  end
end
