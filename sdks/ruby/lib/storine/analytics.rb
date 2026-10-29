# frozen_string_literal: true

module Storine
  # An aggregate argument: Storine.field(path), Storine.lit(value), combined with + - * and
  # unary -. Numbers (Integer, Float, Storine::Decimal, BigDecimal) become constants and Strings
  # are field paths, on either side: `Storine.field("price") * "qty"`, `2 * Storine.field("qty") - 1`.
  class Expr
    FIELD = 1
    CONST = 2
    ADD = 3
    SUB = 4
    MUL = 5
    NEG = 6

    attr_reader :kind, :path, :value, :children

    def initialize(kind, path: "", value: nil, children: [])
      @kind = kind
      @path = path
      @value = value
      @children = children.freeze
      freeze
    end

    def +(other) = Expr.new(ADD, children: [self, Expr.from(other)])
    def -(other) = Expr.new(SUB, children: [self, Expr.from(other)])
    def *(other) = Expr.new(MUL, children: [self, Expr.from(other)])
    def -@ = Expr.new(NEG, children: [self])
    def +@ = self

    # `2 * field("x")`: Ruby asks the Expr to coerce the number on the left.
    def coerce(other) = [Expr.from(other), self]

    def ==(other)
      other.is_a?(Expr) && [kind, path, value, children] == [other.kind, other.path, other.value, other.children]
    end
    alias eql? ==

    def hash = [kind, path, value, children].hash

    # Expr as is; a String / Symbol is a field path; a number is a constant.
    def self.from(x)
      case x
      when Expr then x
      when String, Symbol then Storine.field(x.to_s)
      when Numeric then Storine.lit(x)
      else raise InvalidError, "not an expression: #{x.inspect} (use Storine.field(path), Storine.lit(value) or a number)"
      end
    end
  end

  # One aggregate of Collection#aggregate (build with Storine.count, sum, avg, min, max, ...).
  Aggregate = Struct.new(:func, :arg)

  # A group key: a field's value (a plain path String) or Storine.bucket(field, width, offset).
  GroupKey = Struct.new(:kind, :path, :width, :offset)

  # Result of Collection#aggregate: `rows` (Hashes of column name => value, group-key columns
  # first, then the aggregates in the order given), `matched` (rows that passed the filter),
  # `pruned_row_groups` and `spilled_bytes`. Enumerable over the rows.
  class AggregateResult
    include Enumerable

    attr_reader :columns, :rows, :matched, :pruned_row_groups, :spilled_bytes

    def initialize(columns, rows, matched, pruned_row_groups, spilled_bytes)
      @columns = columns
      @rows = rows
      @matched = matched
      @pruned_row_groups = pruned_row_groups
      @spilled_bytes = spilled_bytes
    end

    def each(&) = rows.each(&)
    def size = rows.size
    def [](index) = rows[index]

    def inspect
      "#<Storine::AggregateResult rows=#{rows.inspect} matched=#{matched} " \
        "pruned_row_groups=#{pruned_row_groups} spilled_bytes=#{spilled_bytes}>"
    end
  end

  I64_RANGE = (-(1 << 63))..((1 << 63) - 1)
  private_constant :I64_RANGE

  # ── expressions ──

  # The value of a field (dotted path for nested maps).
  def self.field(path)
    raise InvalidError, "a field path is a String" unless path.is_a?(String) || path.is_a?(Symbol)

    Expr.new(Expr::FIELD, path: path.to_s)
  end

  # A constant (any storable value; numbers for arithmetic).
  def self.lit(value) = Expr.new(Expr::CONST, value: value)

  # ── aggregates ──

  # Number of rows; with `arg`, number of rows where it is not null.
  def self.count(arg = nil) = Aggregate.new(1, arg.nil? ? nil : Expr.from(arg))
  # Exact sum (integers and decimals never round; floats are correctly rounded).
  def self.sum(arg) = Aggregate.new(2, Expr.from(arg))
  # Mean; a decimal (6 more fractional digits, half-even) for exact inputs.
  def self.avg(arg) = Aggregate.new(3, Expr.from(arg))
  # Smallest non-null value (document total order).
  def self.min(arg) = Aggregate.new(4, Expr.from(arg))
  # Largest non-null value (document total order).
  def self.max(arg) = Aggregate.new(5, Expr.from(arg))
  # Exact number of distinct non-null values.
  def self.count_distinct(arg) = Aggregate.new(6, Expr.from(arg))
  # HyperLogLog estimate of the number of distinct values (about 0.8 % standard error).
  def self.approx_count_distinct(arg) = Aggregate.new(7, Expr.from(arg))

  # Groups numbers by `offset + floor((v - offset) / width) * width` (e.g. weeks of days); the
  # group column is named after the field.
  def self.bucket(field, width, offset = 0)
    raise InvalidError, "bucket takes a field path String" unless field.is_a?(String) || field.is_a?(Symbol)

    { "width" => width, "offset" => offset }.each do |name, v|
      raise InvalidError, "bucket #{name} must be a 64-bit Integer" unless v.is_a?(Integer) && I64_RANGE.cover?(v)
    end
    GroupKey.new(2, field.to_s, width, offset)
  end

  # Encodings of OP_DOC_AGGREGATE / OP_DOC_SCAN_COLUMNS (protocol/PROTOCOL.md §5.5, §5.6).
  module Wire
    OP_DOC_AGGREGATE = 0x29
    OP_DOC_SCAN_COLUMNS = 0x2A
    MAX_ANALYTIC_ITEMS = 64
    MAX_SCAN_PAGE = 100_000

    # [group keys, aggregates, column names, [[column index, desc]]] from Collection#aggregate's arguments.
    AggregateQuery = Struct.new(:group_by, :aggregates, :columns, :order)

    module_function

    def encode_expr(e, out, depth = 0)
      raise InvalidError, "expression nesting is too deep (max 32)" if depth > 32

      out << e.kind.chr
      case e.kind
      when Expr::FIELD then SVE.put_str(out, e.path)
      when Expr::CONST then SVE.encode(e.value, out)
      else e.children.each { |c| encode_expr(c, out, depth + 1) }
      end
    end

    def put_filter(out, filter)
      if filter.nil?
        out << "\x00"
      else
        out << "\x01"
        encode_filter(filter, out)
      end
    end

    def unsigned(value, bits, name)
      unless value.is_a?(Integer) && value.between?(0, (1 << bits) - 1)
        raise InvalidError, "#{name} must be an Integer between 0 and 2**#{bits} - 1"
      end

      value
    end

    def group_key(k)
      case k
      when GroupKey then k
      when String, Symbol then GroupKey.new(1, k.to_s, 0, 0)
      else raise InvalidError, "group_by entries are field paths or Storine.bucket(field, width, offset)"
      end
    end

    def build_aggregate(aggregates, group_by, order_by)
      unless aggregates.is_a?(Hash)
        raise InvalidError, "aggregates is a Hash of output name => aggregate (Storine.count, Storine.sum(\"x\"), ...)"
      end

      keys = case group_by
             when nil then []
             when Array then group_by.map { |k| group_key(k) }
             else [group_key(group_by)]
             end
      aggs = aggregates.map do |name, a|
        raise InvalidError, "aggregate names are Strings or Symbols" unless name.is_a?(String) || name.is_a?(Symbol)
        raise InvalidError, "aggregate #{name.inspect} must be built with Storine.count, sum, avg, ..." unless a.is_a?(Aggregate)

        a
      end
      columns = keys.map(&:path) + aggregates.keys.map(&:to_s)
      if columns.uniq.length != columns.length
        raise InvalidError, "output column names (group keys and aggregate names) must be distinct"
      end

      order = parse_order_by(order_by).map do |name, desc|
        i = columns.index(name)
        raise InvalidError, "order_by #{name.inspect} is not an output column (one of #{columns.inspect})" if i.nil?

        [i, desc]
      end
      AggregateQuery.new(keys, aggs, columns, order)
    end

    def encode_aggregate(collection, filter, group_by, aggregates, order, limit, memory_budget = 0, spill_budget = 0)
      { "group keys" => group_by, "aggregates" => aggregates, "order columns" => order }.each do |what, items|
        raise InvalidError, "at most #{MAX_ANALYTIC_ITEMS} #{what}" if items.length > MAX_ANALYTIC_ITEMS
      end
      out = buf
      SVE.put_str(out, collection)
      put_filter(out, filter)
      out << [group_by.length].pack("V")
      group_by.each do |k|
        out << k.kind.chr
        SVE.put_str(out, k.path)
        out << [k.width, k.offset].pack("q<q<") if k.kind == 2
      end
      out << [aggregates.length].pack("V")
      aggregates.each do |a|
        out << a.func.chr
        if a.arg.nil?
          out << "\x00"
        else
          out << "\x01"
          encode_expr(a.arg, out)
        end
      end
      out << [order.length].pack("V")
      order.each { |column, desc| out << [column, desc ? 1 : 0].pack("VC") }
      out << [unsigned(limit, 32, "limit"), unsigned(memory_budget, 64, "memory_budget"),
              unsigned(spill_budget, 64, "spill_budget")].pack("VQ<Q<")
    end

    def encode_scan(collection, filter, fields, after_key, limit)
      out = buf
      SVE.put_str(out, collection)
      put_filter(out, filter)
      out << [fields.length].pack("V")
      fields.each { |f| SVE.put_str(out, f) }
      if after_key.nil?
        out << "\x00"
      else
        out << "\x01"
        SVE.put_str(out, after_key)
      end
      out << [unsigned(limit, 32, "limit")].pack("V")
    end

    # OP_DOC_ADMIN command 10 (enable columnar) / 11 (disable columnar).
    def encode_columnar(collection, enable)
      out = (enable ? "\x0A" : "\x0B").b
      SVE.put_str(out, collection)
      out
    end

    # -> [matched, pruned_row_groups, spilled_bytes, columns, rows (Arrays)].
    def decode_aggregate_result(body)
      r = Reader.new(body)
      matched = r.u64
      pruned = r.u64
      spilled = r.u64
      cols = r.u32
      rows = Array.new(r.count(cols)) { Array.new(cols) { r.value } }
      raise InvalidError, "trailing bytes after aggregate result" unless r.remaining.zero?

      [matched, pruned, spilled, cols, rows]
    end

    def aggregate_result(columns, body)
      matched, pruned, spilled, cols, rows = decode_aggregate_result(body)
      if !rows.empty? && cols != columns.length
        raise InvalidError, "the server returned #{cols} columns, expected #{columns.length}"
      end

      AggregateResult.new(columns, rows.map { |row| columns.zip(row).to_h }, matched, pruned, spilled)
    end

    # -> [total, next_after_key, fields, [[key, values]]].
    def decode_scan_page(body)
      r = Reader.new(body)
      total = r.u64
      nxt = r.u8.zero? ? nil : r.str
      nf = r.u32
      rows = Array.new(r.count(4 + nf)) do
        key = r.str
        [key, Array.new(nf) { r.value }]
      end
      raise InvalidError, "trailing bytes after scan page" unless r.remaining.zero?

      [total, nxt, nf, rows]
    end
  end
end
