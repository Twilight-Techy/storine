# frozen_string_literal: true

require "socket"
require "securerandom"
require "uri"

module Storine
  # A document: a Hash of its fields, plus #key and #version.
  class Document < Hash
    attr_reader :key, :version

    def initialize(key, version, fields)
      super()
      @key = key
      @version = version
      merge!(fields)
    end

    def inspect = "#<Storine::Document key=#{key.inspect} version=#{version} #{super}>"
  end

  # One TCP connection speaking the binary protocol.
  class Connection
    ZERO_KEY = ("\x00" * 24).b.freeze
    MAX_BODY = 256 * 1024 * 1024

    attr_reader :address

    def initialize(address, timeout, token, follower_reads)
      @address = address
      @sock = Socket.tcp(address[0], address[1], connect_timeout: timeout)
      @sock.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
      @sock.timeout = timeout
      if token
        status, _v, body = call(Wire::OP_AUTH, token.b)
        unless status == Wire::ST_OK
          close
          raise UnauthorizedError.new("authentication failed: #{body.force_encoding('UTF-8').scrub}", status)
        end
      end
      call(Wire::OP_SET_OPTIONS, "\x01".b) if follower_reads
    end

    def call(op, body)
      @sock.write([op].pack("C") + ZERO_KEY + [body.bytesize].pack("V") + body)
      status, version, n = read(13).unpack("CQ<V")
      raise ServerError, "response too large" if n > MAX_BODY

      [status, version, read(n)]
    end

    def close
      @sock.close
    rescue IOError, SystemCallError
      nil
    end

    private

    def read(n)
      out = String.new(capacity: n, encoding: Encoding::BINARY)
      while out.bytesize < n
        chunk = @sock.readpartial([n - out.bytesize, 1 << 20].min)
        out << chunk
      end
      out
    rescue EOFError
      raise IOError, "server closed the connection"
    end
  end

  # A connection pool to a Storine server or cluster (thread-safe). Use Storine.connect.
  # Writes are retried automatically with the same request id (applied at most once) across
  # timeouts, reconnects and leader changes.
  class Client
    TRANSIENT = [IOError, SystemCallError, IO::TimeoutError, SocketError].freeze

    def initialize(seeds, token: nil, timeout: 5.0, follower_reads: false, retries: 8)
      raise InvalidError, "no server address given" if seeds.empty?

      @seeds = seeds
      @token = token
      @timeout = timeout
      @follower_reads = follower_reads
      @retries = retries
      @target = seeds.first
      @pool = []
      @lock = Mutex.new
    end

    # A handle on the collection `name` (created on first write).
    def collection(name) = Collection.new(self, name)
    alias [] collection

    # An atomic batch of writes. With a block, commits when the block returns:
    #   db.transaction { |tx| tx.update("accounts", "a", Storine.add("balance", -10, min: 0)) }
    def transaction
      tx = Transaction.new(self)
      return tx unless block_given?

      yield tx
      tx.commit
    end

    # Applies `writes` atomically (exactly once, even across retries); returns new versions.
    def write(writes)
      body = Wire.encode_writes(writes, SecureRandom.random_bytes(16))
      status, _v, rbody = request(Wire::OP_DOC_WRITE, body, idempotent: true)
      if status == Wire::ST_NOT_FOUND
        # Body: u32 index of the write that patched a missing document, then the message.
        err = NotFoundError.new(text(rbody.byteslice(4..) || "").then { |m| m.empty? ? "document not found" : m }, status)
        err.write_index = rbody.unpack1("V") if rbody.bytesize >= 4
        raise err
      end
      raise_for(status, rbody) unless status == Wire::ST_OK
      Wire.decode_write_response(rbody)[0]
    end

    # Names of every declared collection.
    def collections
      status, _v, body = request(Wire::OP_DOC_INFO, "\x03".b, idempotent: true)
      raise_for(status, body) unless status == Wire::ST_OK
      r = Reader.new(body)
      Array.new(r.count(17)) { Wire.decode_info(r)[:name] }
    end

    # Node id, leader, term and commit indexes of the server currently used.
    def cluster_info
      status, _v, body = request(Wire::OP_CLUSTER_INFO, "".b, idempotent: true)
      raise_for(status, body) unless status == Wire::ST_OK
      Wire.decode_cluster_info(body)
    end

    # Closes every pooled connection.
    def close
      pool = @lock.synchronize { @pool.slice!(0..) }
      pool.each(&:close)
    end

    # Sends a request, following leader redirects and retrying transient failures.
    # `idempotent` requests (all reads, and writes that carry a request id) are resent after a
    # connection failure or an unknown commit outcome. Retries continue for up to three times
    # the timeout (enough to ride out a leader election).
    def request(op, body, idempotent:)
      deadline = now + [3 * @timeout, 1.0].max
      delay = 0.02
      last = nil
      failed = []
      attempts = 0
      while attempts < @retries || now < deadline
        attempts += 1
        break if attempts > 1000

        begin
          c = acquire
        rescue *TRANSIENT => e
          last = e
          failed << @target
          redirect(nil, failed)
          sleep(delay)
          delay = [delay * 2, 0.5].min
          next
        end
        begin
          status, version, rbody = c.call(op, body)
        rescue *TRANSIENT => e
          c.close
          last = e
          raise UnavailableError, "connection lost during a request: #{e.message}" unless idempotent

          failed << c.address
          redirect(nil, failed)
          sleep(delay)
          delay = [delay * 2, 0.5].min
          next
        end
        case status
        when Wire::ST_NOT_LEADER
          c.close
          hint = text(rbody)
          hint = nil if hint.empty? || failed.include?(Client.parse_address(hint)) # stale: old leader is down
          redirect(hint, failed)
          last = UnavailableError.new("no cluster leader is available yet")
          unless hint
            sleep(delay)
            delay = [delay * 2, 0.5].min
          end
          next
        when Wire::ST_COMMIT_UNKNOWN, Wire::ST_OVERLOADED
          if idempotent
            c.close
            last = if status == Wire::ST_OVERLOADED
                     OverloadedError.new(text(rbody).then { |m| m.empty? ? "server overloaded" : m }, status)
                   else
                     UnavailableError.new("write outcome unknown and retries exhausted")
                   end
            sleep(delay)
            delay = [delay * 2, 0.5].min
            next
          end
        end
        if [Wire::ST_UNAUTHORIZED, Wire::ST_RATE_LIMITED].include?(status)
          c.close # the server closed the connection
        else
          release(c)
        end
        return [status, version, rbody]
      end
      raise last if last.is_a?(Error)

      raise UnavailableError, "could not reach any server: #{last&.message}"
    end

    def raise_for(status, body)
      msg = text(body)
      case status
      when Wire::ST_CONFLICT
        index, version, m = Wire.decode_conflict(body)
        raise ConflictError.new(m, index, version)
      when Wire::ST_NOT_FOUND then raise NotFoundError.new(msg.empty? ? "document not found" : msg, status)
      when Wire::ST_INVALID then raise InvalidError.new(msg, status)
      when Wire::ST_UNAUTHORIZED then raise UnauthorizedError.new(msg.empty? ? "not authorized" : msg, status)
      when Wire::ST_RATE_LIMITED then raise RateLimitedError.new(msg.empty? ? "rate limited" : msg, status)
      when Wire::ST_OVERLOADED then raise OverloadedError.new(msg.empty? ? "server overloaded" : msg, status)
      when Wire::ST_COMMIT_UNKNOWN then raise UnavailableError.new("write outcome unknown; retry the same call", status)
      else raise ServerError.new(msg.empty? ? "server error (status #{status})" : msg, status)
      end
    end

    # "host:port" (port 7878 when omitted; "[::1]:7878" for IPv6).
    def self.parse_address(text)
      t = text.strip
      host, sep, port = t.rpartition(":")
      host, port = port, "7878" if sep.empty? || host.end_with?(":") && !t.start_with?("[")
      [host.delete("[]"), Integer(port)]
    end

    private

    def now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    def text(bytes) = bytes.dup.force_encoding(Encoding::UTF_8).scrub

    def acquire
      target = nil
      @lock.synchronize do
        target = @target
        while (c = @pool.pop)
          return c if c.address == target

          c.close
        end
      end
      Connection.new(target, @timeout, @token, @follower_reads)
    end

    def release(conn)
      @lock.synchronize do
        if conn.address == @target && @pool.length < 16
          @pool << conn
          return
        end
      end
      conn.close
    end

    def redirect(address, avoid = [])
      @lock.synchronize do
        if address
          @target = Client.parse_address(address)
          return
        end
        others = @seeds.reject { |s| s == @target || avoid.include?(s) }
        others = @seeds.reject { |s| s == @target } if others.empty?
        others = @seeds if others.empty?
        @target = others.sample
      end
    end
  end

  # Connects to a Storine server or cluster.
  # `url` is `storine://[token@]host:port[,host:port...]` (or just `host:port`); options may
  # also be query parameters: `?timeout_ms=5000&follower_reads=1`.
  def self.connect(url = "storine://127.0.0.1:7878", token: nil, timeout: 5.0, follower_reads: false)
    url = "storine://#{url}" unless url.include?("://")
    scheme, rest = url.split("://", 2)
    raise InvalidError, "connection strings start with storine://" unless scheme == "storine"

    netloc, query = rest.split("?", 2)
    netloc = netloc.chomp("/")
    if netloc.include?("@")
      cred, netloc = netloc.rpartition("@").values_at(0, 2)
      token ||= URI.decode_www_form_component(cred)
    end
    seeds = netloc.split(",").reject(&:empty?).map { |h| Client.parse_address(h) }
    URI.decode_www_form(query.to_s).each do |k, v|
      timeout = Integer(v) / 1000.0 if k == "timeout_ms"
      follower_reads = %w[1 true yes].include?(v) if k == "follower_reads"
    end
    Client.new(seeds, token: token, timeout: timeout, follower_reads: follower_reads)
  end

  # Shared write builders of Collection and Transaction.
  module Writes
    private

    def put_write(collection, key, fields, ttl, pre, version = 0)
      raise InvalidError, "a document is a Hash of fields" unless fields.is_a?(Hash)

      ttl_ms = ttl_ms(ttl)
      Wire::Write.new(collection, key, ttl_ms.zero? ? 1 : 2, pre, version, fields, ttl_ms)
    end

    def ttl_ms(ttl)
      return 0 if ttl.nil?
      raise InvalidError, "ttl must be positive (seconds)" unless ttl.positive?

      [1, (ttl * 1000).to_i].max
    end

    def ops(changes)
      out = changes.flat_map do |c|
        case c
        when FieldOp then [c]
        when Hash then c.map { |k, v| Storine.set(k.to_s, v) }
        else raise InvalidError, "update takes field operations (Storine.set, add, ...) or a { field => value } Hash"
        end
      end
      raise InvalidError, "update needs at least one change" if out.empty?

      out
    end

    def versioned(if_version, default_pre)
      if_version.nil? ? [default_pre, 0] : [3, if_version]
    end
  end

  # A named set of documents. Every method is one atomic, exactly-once operation.
  class Collection
    include Writes

    attr_reader :client, :name

    def initialize(client, name)
      @client = client
      @name = name
    end

    # The document `key`, or nil if it does not exist.
    def get(key)
      status, version, body = client.request(Wire::OP_DOC_GET, Wire.encode_get(name, key), idempotent: true)
      return nil if status == Wire::ST_NOT_FOUND

      client.raise_for(status, body) unless status == Wire::ST_OK
      Document.new(key, version, Reader.new(body).map)
    end

    # Creates or replaces the document; returns its new version. `ttl:` in seconds.
    def put(key, fields, ttl: nil) = client.write([put_write(name, key, fields, ttl, 0)])[0] || 0

    # Creates the document; raises ConflictError if it already exists.
    def insert(key, fields, ttl: nil) = client.write([put_write(name, key, fields, ttl, 1)])[0] || 0

    # Replaces an existing document (only at `if_version:` when given); ConflictError otherwise.
    def replace(key, fields, if_version: nil)
      pre, ver = versioned(if_version, 2)
      client.write([put_write(name, key, fields, nil, pre, ver)])[0] || 0
    end

    # Applies field changes atomically on the server; returns the new version.
    # `changes` are Storine.set / unset / add / append operations or { field => value } Hashes:
    #   accounts.update("alice", Storine.add("balance", Storine::Decimal("-25"), min: 0))
    # Raises NotFoundError if the document does not exist.
    def update(key, *changes, if_version: nil)
      pre, ver = versioned(if_version, 0)
      client.write([Wire::Write.new(name, key, 4, pre, ver, nil, 0, ops(changes))])[0] || 0
    end

    # Deletes the document; returns false if it did not exist.
    def delete(key, if_version: nil)
      pre, ver = versioned(if_version, 2)
      client.write([Wire::Write.new(name, key, 3, pre, ver)])
      true
    rescue ConflictError => e
      raise unless if_version.nil? && e.current_version.nil?

      false
    end

    # Documents matching `where` (a filter or { field => value }); at most `limit:` (default 1,000).
    def find(where = nil, order_by: nil, limit: nil, offset: 0)
      query(where, order_by, limit || 0, offset, nil)[2]
    end

    # The first matching document, or nil.
    def first(where = nil, order_by: nil) = find(where, order_by: order_by, limit: 1).first

    # Number of matching documents.
    def count(where = nil) = query(where, nil, 1, 0, nil)[0]

    # Every matching document in key order (any collection size). Returns an Enumerator
    # without a block.
    def scan(where = nil, batch: 1000)
      return enum_for(:scan, where, batch: batch) unless block_given?

      after = nil
      loop do
        _total, nxt, docs = query(where, nil, batch, 0, after)
        docs.each { |d| yield d }
        break if nxt.nil?

        after = nxt
      end
    end

    # Indexes a top-level field (at most 4 per collection); existing documents included.
    def create_index(field) = admin(Wire::OP_DOC_ADMIN, "\x01", field)
    def drop_index(field) = admin(Wire::OP_DOC_ADMIN, "\x02", field)

    # Deletes every document and the collection's metadata; returns the number deleted.
    def drop = admin(Wire::OP_DOC_ADMIN, "\x04")[0]

    # Indexes and metadata of the collection.
    def info = Wire.decode_info(Reader.new(admin(Wire::OP_DOC_INFO, "\x06")[1]))

    private

    def query(where, order_by, limit, offset, after)
      body = Wire.encode_query(name, Wire.where_to_filter(where), Wire.parse_order_by(order_by), limit, offset, after)
      status, _v, rbody = client.request(Wire::OP_DOC_QUERY, body, idempotent: true)
      client.raise_for(status, rbody) unless status == Wire::ST_OK
      total, nxt, docs = Wire.decode_page(rbody)
      [total, nxt, docs.map { |k, v, f| Document.new(k, v, f) }]
    end

    def admin(op, code, field = nil)
      out = code.b
      SVE.put_str(out, name)
      SVE.put_str(out, field) if field
      status, version, rbody = client.request(op, out, idempotent: true)
      client.raise_for(status, rbody) unless status == Wire::ST_OK
      [version, rbody]
    end
  end

  # Writes buffered locally and committed atomically (all or none) by #commit.
  class Transaction
    include Writes

    attr_reader :writes, :versions

    def initialize(client)
      @client = client
      @writes = []
      @versions = []
    end

    def put(collection, key, fields, ttl: nil) = add_write(put_write(collection, key, fields, ttl, 0))
    def insert(collection, key, fields) = add_write(put_write(collection, key, fields, nil, 1))

    def replace(collection, key, fields, if_version: nil)
      pre, ver = versioned(if_version, 2)
      add_write(put_write(collection, key, fields, nil, pre, ver))
    end

    def update(collection, key, *changes, if_version: nil)
      pre, ver = versioned(if_version, 0)
      add_write(Wire::Write.new(collection, key, 4, pre, ver, nil, 0, ops(changes)))
    end

    def delete(collection, key, if_version: nil)
      pre, ver = versioned(if_version, 0)
      add_write(Wire::Write.new(collection, key, 3, pre, ver))
    end

    # Commit only if the document is at `version:` (or simply exists / does not exist).
    def require(collection, key, version: nil, exists: true)
      if version
        add_write(Wire::Write.new(collection, key, 5, 3, version))
      else
        add_write(Wire::Write.new(collection, key, 5, exists ? 2 : 1))
      end
    end

    # Applies every buffered write atomically; returns the new versions in order.
    def commit
      return [] if @writes.empty?

      @versions = @client.write(@writes)
      @writes = []
      @versions
    end

    private

    def add_write(write)
      @writes << write
      self
    end
  end
end
