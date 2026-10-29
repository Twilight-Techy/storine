# frozen_string_literal: true

require "bigdecimal"

module Storine
  # An exact decimal that keeps its scale (`1234.50` stays `1234.50`): a 128-bit mantissa and
  # up to 38 fractional digits. Use it for money. Converts to and from BigDecimal (which drops
  # trailing zeros) with #to_d / Decimal.from.
  class Decimal < Numeric
    MAX_SCALE = 38
    MAX_MANTISSA = (1 << 127) - 1
    MIN_MANTISSA = -(1 << 127)

    attr_reader :mantissa, :scale

    # Decimal.new("19.99"), Decimal.new(1999, 2) or Decimal.new(BigDecimal("1.5")).
    def initialize(value, scale = nil)
      super()
      if scale
        raise InvalidError, "a decimal mantissa is an Integer" unless value.is_a?(Integer)

        @mantissa = value
        @scale = scale
      else
        @mantissa, @scale = Decimal.parse(value)
      end
      normalize!
      freeze
    end

    # A Decimal from a String, Integer, BigDecimal or Decimal.
    def self.from(value)
      value.is_a?(Decimal) ? value : new(value)
    end

    def self.parse(value)
      case value
      when Decimal then [value.mantissa, value.scale]
      when Integer then [value, 0]
      when BigDecimal then parse_big(value)
      when String then parse_text(value)
      else raise InvalidError, "cannot make a decimal from #{value.class}"
      end
    end

    def self.parse_text(text)
      m = /\A\s*([+-]?)(\d*)(?:\.(\d*))?(?:[eE]([+-]?\d+))?\s*\z/.match(text)
      raise InvalidError, "not a decimal: #{text.inspect}" if m.nil? || (m[2].empty? && m[3].to_s.empty?)

      digits = m[2] + m[3].to_s
      mantissa = digits.to_i
      mantissa = -mantissa if m[1] == "-"
      [mantissa, m[3].to_s.length - m[4].to_i]
    end

    def self.parse_big(value)
      raise InvalidError, "NaN and infinity are not decimals" unless value.finite?

      sign, digits, _base, exponent = value.split
      mantissa = digits.to_i * (sign.negative? ? -1 : 1)
      [mantissa, digits.length - exponent]
    end

    # A Storine::Expr on the right builds an analytics expression (`Storine::Decimal("2.5") + field`).
    def +(other) = other.is_a?(Expr) ? Storine.lit(self) + other : combine(other) { |a, b| a + b }
    def -(other) = other.is_a?(Expr) ? Storine.lit(self) - other : combine(other) { |a, b| a - b }

    def *(other)
      return Storine.lit(self) * other if other.is_a?(Expr)

      o = Decimal.from(other)
      Decimal.new(mantissa * o.mantissa, scale + o.scale)
    end

    def -@ = Decimal.new(-mantissa, scale)
    def abs = Decimal.new(mantissa.abs, scale)
    def zero? = mantissa.zero?
    def negative? = mantissa.negative?

    def <=>(other)
      return nil unless other.is_a?(Numeric)
      return to_d <=> other if other.is_a?(Float)

      a, b = aligned(Decimal.from(other))
      a <=> b
    end

    def ==(other)
      other.is_a?(Numeric) && (self <=> other) == 0
    end
    alias eql? ==

    def hash = to_d.hash

    def coerce(other)
      [Decimal.from(other), self]
    end

    def to_d = BigDecimal(to_s)
    def to_r = Rational(mantissa, 10**scale)
    def to_f = to_d.to_f
    def to_i = to_r.to_i
    def inspect = "#<Storine::Decimal #{self}>"

    def to_s
      return mantissa.to_s if scale.zero?

      digits = mantissa.abs.to_s.rjust(scale + 1, "0")
      "#{'-' if mantissa.negative?}#{digits[0...-scale]}.#{digits[-scale..]}"
    end

    private

    # Negative scales fold into the mantissa; scales above 38 drop trailing zeros if they can.
    def normalize!
      if @scale.negative?
        @mantissa *= 10**-@scale
        @scale = 0
      end
      while @scale > MAX_SCALE && (@mantissa % 10).zero?
        @mantissa /= 10
        @scale -= 1
      end
      raise InvalidError, "decimals have at most 38 fractional digits" if @scale > MAX_SCALE
      raise InvalidError, "decimal does not fit in 38 digits" unless @mantissa.between?(MIN_MANTISSA, MAX_MANTISSA)
    end

    def aligned(other)
      s = [scale, other.scale].max
      [mantissa * 10**(s - scale), other.mantissa * 10**(s - other.scale), s]
    end

    def combine(other)
      a, b, s = aligned(Decimal.from(other))
      Decimal.new(yield(a, b), s)
    end
  end

  # Storine::Decimal("19.99")
  def self.Decimal(value) # rubocop:disable Naming/MethodName
    Decimal.from(value)
  end
end
