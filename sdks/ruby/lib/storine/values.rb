# frozen_string_literal: true

require "time"

module Storine
  # An integer stored as an unsigned 64-bit value (plain Integers are signed).
  class UInt64
    attr_reader :value

    def initialize(value)
      raise InvalidError, "UInt64 out of range" unless value.is_a?(Integer) && value.between?(0, (1 << 64) - 1)

      @value = value
    end

    def ==(other) = other.is_a?(UInt64) ? other.value == value : other == value
    def to_i = value
  end

  # An engine id value (24 raw bytes).
  class Id
    attr_reader :bytes

    def initialize(bytes)
      raise InvalidError, "an Id is exactly 24 bytes" unless bytes.bytesize == 24

      @bytes = bytes.b.freeze
    end

    def ==(other) = other.is_a?(Id) && other.bytes == bytes
    alias eql? ==
    def hash = bytes.hash
    def to_s = bytes.unpack1("H*")
  end

  # A UUID (16 bytes). Storine::Uuid.parse("123e4567-e89b-...") or Uuid.random.
  class Uuid
    attr_reader :bytes

    def initialize(bytes)
      raise InvalidError, "a UUID is exactly 16 bytes" unless bytes.bytesize == 16

      @bytes = bytes.b.freeze
    end

    def self.parse(text)
      hex = text.delete("-")
      raise InvalidError, "not a UUID: #{text}" unless hex.match?(/\A\h{32}\z/)

      new([hex].pack("H*"))
    end

    def self.random
      b = SecureRandom.random_bytes(16).bytes
      b[6] = (b[6] & 0x0f) | 0x40
      b[8] = (b[8] & 0x3f) | 0x80
      new(b.pack("C*"))
    end

    def ==(other) = other.is_a?(Uuid) && other.bytes == bytes
    alias eql? ==
    def hash = bytes.hash

    def to_s
      h = bytes.unpack1("H*")
      "#{h[0, 8]}-#{h[8, 4]}-#{h[12, 4]}-#{h[16, 4]}-#{h[20, 12]}"
    end
  end

  # Storine Value Encoding (SVE): Ruby values <-> wire bytes (protocol/PROTOCOL.md §4).
  #
  # nil, true/false, Integer (64- or 128-bit), Float, Storine::Decimal or BigDecimal (exact),
  # String (UTF-8 text; binary strings are bytes), Time (UTC, nanoseconds), Storine::Uuid,
  # Storine::Id, Storine::UInt64, Array, Hash (String or Symbol keys).
  module SVE
    NULL, BOOL, INT64, UINT64, FLOAT64, INT128, TIMESTAMP, UUID, ID, TEXT, BYTES, DECIMAL, LIST, MAP = (0..13).to_a
    MAX_DEPTH = 32
    I64 = (-(1 << 63))..((1 << 63) - 1)
    I128 = (-(1 << 127))..((1 << 127) - 1)

    module_function

    def int_bytes(value, size)
      v = value.negative? ? value + (1 << (8 * size)) : value
      Array.new(size) { |i| (v >> (8 * i)) & 0xff }.pack("C*")
    end

    def put_str(out, text)
      b = text.to_s.encode(Encoding::UTF_8).b
      out << [b.bytesize].pack("V") << b
    rescue EncodingError
      raise InvalidError, "text is not valid UTF-8"
    end

    def encode(value, out, depth = 0)
      raise InvalidError, "value nesting is too deep (max 32)" if depth > MAX_DEPTH

      case value
      when nil then out << NULL.chr
      when true then out << [BOOL, 1].pack("CC")
      when false then out << [BOOL, 0].pack("CC")
      when UInt64 then out << [UINT64, value.value].pack("CQ<")
      when Integer
        if I64.cover?(value)
          out << [INT64, value].pack("Cq<")
        elsif I128.cover?(value)
          out << INT128.chr << int_bytes(value, 16)
        else
          raise InvalidError, "integer does not fit in 128 bits"
        end
      when Float then out << [FLOAT64, value].pack("CE")
      when Decimal, BigDecimal
        d = Decimal.from(value)
        out << DECIMAL.chr << int_bytes(d.mantissa, 16) << d.scale.chr
      when String
        if value.encoding == Encoding::BINARY
          out << [BYTES, value.bytesize].pack("CV") << value
        else
          raise InvalidError, "text is not valid UTF-8" unless value.valid_encoding?

          out << TEXT.chr
          put_str(out, value)
        end
      when Symbol
        out << TEXT.chr
        put_str(out, value.to_s)
      when Id then out << ID.chr << value.bytes
      when Uuid then out << UUID.chr << value.bytes
      when Time then out << [TIMESTAMP, (value.to_i * 1_000_000_000) + value.nsec].pack("Cq<")
      when Array
        out << [LIST, value.length].pack("CV")
        value.each { |v| encode(v, out, depth + 1) }
      when Hash
        out << MAP.chr
        encode_map(value, out, depth + 1)
      else
        if defined?(Date) && value.is_a?(Date) && !value.is_a?(DateTime)
          raise InvalidError, "dates are not supported: use a Time (stored as a UTC timestamp)"
        end
        return encode(value.to_time, out, depth) if value.respond_to?(:to_time) && defined?(DateTime) && value.is_a?(DateTime)

        raise InvalidError, "cannot store a value of type #{value.class}"
      end
      out
    end

    # Appends `u32 count | (str key, value)*` with keys in ascending byte order.
    def encode_map(fields, out, depth = 0)
      items = fields.map do |k, v|
        raise InvalidError, "document field names must be strings" unless k.is_a?(String) || k.is_a?(Symbol)

        [k.to_s.encode(Encoding::UTF_8).b, v]
      end
      items.sort_by!(&:first)
      out << [items.length].pack("V")
      items.each do |raw, v|
        out << [raw.bytesize].pack("V") << raw
        encode(v, out, depth)
      end
      out
    end

    def encode_value(value)
      encode(value, String.new(encoding: Encoding::BINARY))
    end

    def decode(bytes)
      r = Reader.new(bytes)
      v = r.value
      raise InvalidError, "trailing bytes after value" if r.remaining.positive?

      v
    end
  end

  # Bounds-checked cursor over untrusted bytes.
  class Reader
    def initialize(buf, pos = 0)
      @buf = buf.b
      @pos = pos
    end

    def remaining = @buf.bytesize - @pos

    def take(n)
      raise InvalidError, "truncated response" if n.negative? || @pos + n > @buf.bytesize

      b = @buf.byteslice(@pos, n)
      @pos += n
      b
    end

    def u8 = take(1).getbyte(0)
    def u32 = take(4).unpack1("V")
    def u64 = take(8).unpack1("Q<")
    def i64 = take(8).unpack1("q<")

    def int128
      v = take(16).bytes.each_with_index.sum { |b, i| b << (8 * i) }
      v >= (1 << 127) ? v - (1 << 128) : v
    end

    def str
      s = take(u32).force_encoding(Encoding::UTF_8)
      raise InvalidError, "invalid UTF-8 in response" unless s.valid_encoding?

      s
    end

    def count(min_size)
      n = u32
      raise InvalidError, "truncated response" if n * [min_size, 1].max > remaining

      n
    end

    def value(depth = 0)
      tag = u8
      case tag
      when SVE::NULL then nil
      when SVE::BOOL
        b = u8
        raise InvalidError, "bool byte must be 0 or 1" if b > 1

        b == 1
      when SVE::INT64 then i64
      when SVE::UINT64 then u64
      when SVE::FLOAT64 then take(8).unpack1("E")
      when SVE::INT128 then int128
      when SVE::TIMESTAMP
        ns = i64
        Time.at(ns.div(1_000_000_000), ns % 1_000_000_000, :nsec, in: "UTC")
      when SVE::UUID then Uuid.new(take(16))
      when SVE::ID then Id.new(take(24))
      when SVE::TEXT then str
      when SVE::BYTES then take(u32)
      when SVE::DECIMAL
        mantissa = int128
        scale = u8
        raise InvalidError, "decimal scale above 38" if scale > Decimal::MAX_SCALE

        Decimal.new(mantissa, scale)
      when SVE::LIST
        raise InvalidError, "value nesting is too deep" if depth >= SVE::MAX_DEPTH

        Array.new(count(1)) { value(depth + 1) }
      when SVE::MAP
        raise InvalidError, "value nesting is too deep" if depth >= SVE::MAX_DEPTH

        map(depth + 1)
      else
        raise InvalidError, "unknown value tag #{tag}"
      end
    end

    def map(depth = 0)
      out = {}
      count(5).times do
        k = str
        raise InvalidError, "duplicate map key" if out.key?(k)

        out[k] = value(depth)
      end
      out
    end
  end
end
