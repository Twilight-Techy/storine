# frozen_string_literal: true

module Storine
  # Base class of every Storine error. Each says what happened and what to do about it.
  class Error < StandardError
    attr_reader :status

    def initialize(message, status = nil)
      super(message)
      @status = status
    end
  end

  # The document does not exist (e.g. `update` of a missing key).
  class NotFoundError < Error
    attr_accessor :write_index
  end

  # A condition did not hold: the document already exists, does not exist, or changed.
  # `write_index` is the position of the failing write in the request and `current_version`
  # the document's version now (nil if it does not exist). Re-read and decide again.
  class ConflictError < Error
    attr_reader :write_index, :current_version

    def initialize(message, write_index = nil, current_version = nil)
      super(message, 0x04)
      @write_index = write_index
      @current_version = current_version
    end
  end

  # The request was rejected as invalid (bad value, type mismatch, bound violated). Do not retry.
  class InvalidError < Error; end

  # Authentication failed or the token's role does not allow this operation.
  class UnauthorizedError < Error; end

  # The server is temporarily overloaded. Retry later.
  class OverloadedError < Error; end

  # This connection exceeded its rate limit. Retry later.
  class RateLimitedError < OverloadedError; end

  # No server (or no cluster leader) could be reached in time. Retry later.
  class UnavailableError < Error; end

  # The server failed internally (I/O error...). The message has the details.
  class ServerError < Error; end
end
