# frozen_string_literal: true

module Storine
  # One atomic change applied server-side by `update`
  # (build with Storine.set / unset / add / append).
  FieldOp = Struct.new(:kind, :path, :value, :min, :max, :has_min, :has_max)

  # A query condition (build with Storine.eq, gt, between, and, ...; combine with & | ~).
  class Filter
    attr_reader :kind, :path, :values, :children

    def initialize(kind, path: "", values: [], children: [])
      @kind = kind
      @path = path
      @values = values
      @children = children
    end

    def &(other) = Storine.and(self, other)
    def |(other) = Storine.or(self, other)
    def ~ = Storine.not(self)
    def ! = Storine.not(self)
  end

  # ── field operations ──

  # Set the field at `path` (dotted for nested maps) to `value`.
  def self.set(path, value) = FieldOp.new(1, path, value, nil, nil, false, false)

  # Remove the field at `path`.
  def self.unset(path) = FieldOp.new(2, path, nil, nil, nil, false, false)

  # Add `amount` (negative to subtract) exactly and atomically; refuse results below `min:` or
  # above `max:`. `add("balance", Storine::Decimal("-25.00"), min: 0)` never overdraws.
  def self.add(path, amount, **bounds)
    unknown = bounds.keys - %i[min max]
    raise ArgumentError, "unknown bound #{unknown.first}" unless unknown.empty?

    FieldOp.new(3, path, amount, bounds[:min], bounds[:max], bounds.key?(:min), bounds.key?(:max))
  end

  # Append `value` to the list at `path` (a missing field becomes a one-element list).
  def self.append(path, value) = FieldOp.new(4, path, value, nil, nil, false, false)

  # ── filters ──

  # Field equals value (numbers compare numerically; nil matches missing fields).
  def self.eq(path, value) = Filter.new(1, path: path, values: [value])
  def self.ne(path, value) = Filter.new(2, path: path, values: [value])
  def self.lt(path, value) = Filter.new(3, path: path, values: [value])
  def self.le(path, value) = Filter.new(4, path: path, values: [value])
  def self.gt(path, value) = Filter.new(5, path: path, values: [value])
  def self.ge(path, value) = Filter.new(6, path: path, values: [value])
  # `low <= field <= high`.
  def self.between(path, low, high) = Filter.new(7, path: path, values: [low, high])
  # Field equals any of `values`.
  def self.in(path, values) = Filter.new(8, path: path, values: values.to_a)
  # Text field starts with `text`.
  def self.prefix(path, text) = Filter.new(9, path: path, values: [text])
  # Text field contains `value`, or list field has an element equal to `value`.
  def self.contains(path, value) = Filter.new(10, path: path, values: [value])
  # Field is present and not null.
  def self.exists(path) = Filter.new(11, path: path)
  # Field is absent or null.
  def self.missing(path) = Filter.new(12, path: path)
  # Document key starts with `text`.
  def self.key_prefix(text) = Filter.new(13, values: [text])
  def self.and(*filters) = Filter.new(14, children: filters)
  def self.or(*filters) = Filter.new(15, children: filters)
  def self.not(filter) = Filter.new(16, children: [filter])

  # Request / response encodings of the document API (protocol/PROTOCOL.md §5-§6).
  module Wire
    OP_AUTH = 0x0C
    OP_DOC_GET = 0x20
    OP_DOC_WRITE = 0x21
    OP_DOC_QUERY = 0x22
    OP_DOC_ADMIN = 0x23
    OP_DOC_INFO = 0x25
    OP_SET_OPTIONS = 0x26
    OP_CLUSTER_INFO = 0x27

    ST_OK, ST_NOT_FOUND, ST_ERROR, ST_OVERLOADED, ST_CONFLICT, ST_UNAUTHORIZED, ST_RATE_LIMITED,
      ST_NOT_LEADER, ST_INVALID, ST_COMMIT_UNKNOWN = (0..9).to_a

    # One write of an atomic request (built by Collection / Transaction methods).
    # kind: 1 put, 2 put+ttl, 3 delete, 4 patch, 5 check; precondition: 0 none, 1 absent,
    # 2 exists, 3 version.
    Write = Struct.new(:collection, :key, :kind, :precondition, :version, :fields, :ttl_ms, :ops) do
      def initialize(collection, key, kind, precondition = 0, version = 0, fields = nil, ttl_ms = 0, ops = [])
        super
      end
    end

    module_function

    def buf = String.new(encoding: Encoding::BINARY)

    def encode_op(op, out)
      out << op.kind.chr
      SVE.put_str(out, op.path)
      case op.kind
      when 1, 4 then SVE.encode(op.value, out)
      when 3
        SVE.encode(op.value, out)
        out << ((op.has_min ? 1 : 0) | (op.has_max ? 2 : 0)).chr
        SVE.encode(op.min, out) if op.has_min
        SVE.encode(op.max, out) if op.has_max
      end
    end

    def encode_filter(f, out, depth = 0)
      raise InvalidError, "filter nesting is too deep (max 32)" if depth > 32

      out << f.kind.chr
      case f.kind
      when 1..6, 10
        SVE.put_str(out, f.path)
        SVE.encode(f.values[0], out)
      when 7
        SVE.put_str(out, f.path)
        SVE.encode(f.values[0], out)
        SVE.encode(f.values[1], out)
      when 8
        SVE.put_str(out, f.path)
        out << [f.values.length].pack("V")
        f.values.each { |v| SVE.encode(v, out) }
      when 9
        SVE.put_str(out, f.path)
        SVE.put_str(out, f.values[0])
      when 11, 12 then SVE.put_str(out, f.path)
      when 13 then SVE.put_str(out, f.values[0])
      when 14, 15
        out << [f.children.length].pack("V")
        f.children.each { |c| encode_filter(c, out, depth + 1) }
      when 16 then encode_filter(f.children[0], out, depth + 1)
      end
    end

    # A Filter, a { field => value } Hash (all equal), or nil.
    def where_to_filter(where)
      case where
      when nil then nil
      when Filter then where
      when Hash
        parts = where.map { |k, v| v.is_a?(Filter) ? v : Storine.eq(k.to_s, v) }
        return nil if parts.empty?

        parts.length == 1 ? parts[0] : Storine.and(*parts)
      else
        raise InvalidError, "where must be a filter (Storine.eq, gt, ...) or a { field => value } Hash"
      end
    end

    def encode_get(collection, key)
      out = buf
      SVE.put_str(out, collection)
      SVE.put_str(out, key)
      out
    end

    def encode_writes(writes, request_id)
      out = buf
      if request_id.nil?
        out << "\x00"
      else
        out << "\x01" << request_id.b
      end
      out << [writes.length].pack("V")
      writes.each do |w|
        SVE.put_str(out, w.collection)
        SVE.put_str(out, w.key)
        out << w.precondition.chr
        out << [w.version].pack("Q<") if w.precondition == 3
        out << w.kind.chr
        case w.kind
        when 1 then SVE.encode_map(w.fields || {}, out)
        when 2
          out << [w.ttl_ms].pack("Q<")
          SVE.encode_map(w.fields || {}, out)
        when 4
          out << [w.ops.length].pack("V")
          w.ops.each { |op| encode_op(op, out) }
        end
      end
      out
    end

    # `nearest`: nil or [field, vector, k, exact].
    def encode_query(collection, filter, order_by, limit, offset, after_key, nearest = nil)
      out = buf
      SVE.put_str(out, collection)
      if filter.nil?
        out << "\x00"
      else
        out << "\x01"
        encode_filter(filter, out)
      end
      out << [order_by.length].pack("V")
      order_by.each do |path, desc|
        SVE.put_str(out, path)
        out << (desc ? "\x01" : "\x00")
      end
      out << [limit, offset].pack("VV")
      if after_key.nil?
        out << "\x00"
      else
        out << "\x01"
        SVE.put_str(out, after_key)
      end
      if nearest
        # Optional trailing block: u8 1 | str field | u32 k | u8 exact | u32 dims | dims x f32 LE.
        field, vector, k, exact = nearest
        out << "\x01"
        SVE.put_str(out, field)
        out << [k, exact ? 1 : 0, vector.length].pack("VCV")
        out << vector.map { |x| Float(x) }.pack("e*")
      end
      out
    end

    # "price" / "-price" (descending) / ["a", "-b"] / [["a", :desc]].
    def parse_order_by(order_by)
      return [] if order_by.nil?

      items = order_by.is_a?(Array) && !(order_by.length == 2 && order_by[1].is_a?(Symbol)) ? order_by : [order_by]
      items.map do |it|
        case it
        when Array then [it[0].to_s, %w[desc descending -1].include?(it[1].to_s.downcase)]
        when String, Symbol
          s = it.to_s
          s.start_with?("-") ? [s[1..], true] : [s, false]
        else raise InvalidError, "order_by entries are 'field', '-field' or ['field', :desc]"
        end
      end
    end

    def decode_write_response(body)
      r = Reader.new(body)
      duplicate = r.u8 != 0
      versions = Array.new(r.count(9)) do
        exists = r.u8 != 0
        v = r.u64
        exists ? v : nil
      end
      [versions, duplicate]
    end

    def decode_page(body)
      r = Reader.new(body)
      total = r.u32
      next_key = r.u8.zero? ? nil : r.str
      docs = Array.new(r.count(16)) do
        key = r.str
        version = r.u64
        [key, version, r.map]
      end
      [total, next_key, docs]
    end

    def decode_conflict(body)
      return [nil, nil, body.dup.force_encoding(Encoding::UTF_8).scrub] if body.bytesize < 13

      index, exists, version = body.unpack("VCQ<")
      msg = body.byteslice(13..).force_encoding(Encoding::UTF_8).scrub
      [index == 0xFFFFFFFF ? nil : index, exists.zero? ? nil : version, msg]
    end

    def decode_info(r)
      name = r.str
      indexes = Array.new(r.count(4)) { r.str }
      building = Array.new(r.count(4)) { r.str }
      { name: name, indexes: indexes, building: building, metadata: r.map }
    end

    def decode_cluster_info(body)
      r = Reader.new(body)
      node_id = r.u64
      is_leader = r.u8 != 0
      leader_id = r.u64
      leader_addr = r.str
      term = r.u64
      commit = r.u64
      applied = r.u64
      members = {}
      r.count(12).times do
        id = r.u64
        members[id] = r.str
      end
      { node_id: node_id, is_leader: is_leader, leader_id: leader_id.zero? ? nil : leader_id,
        leader_addr: leader_addr.empty? ? nil : leader_addr, term: term, commit_index: commit,
        last_applied: applied, members: members }
    end
  end
end
