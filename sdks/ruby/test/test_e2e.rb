# frozen_string_literal: true

# End-to-end tests against a real storage-server (single node and a 3-node cluster).
# The server binary is $STORINE_SERVER or <repo>/target/release/storage-server
# (build it with `cargo build --release -p storage-server`); without it the tests are skipped.

require "minitest/autorun"
require "tmpdir"
require "fileutils"
require "socket"
require_relative "../lib/storine"

REPO = File.expand_path("../../..", __dir__)
SERVER = ENV.fetch("STORINE_SERVER", File.join(REPO, "target", "release", "storage-server"))

module ServerHelpers
  def free_port
    s = TCPServer.new("127.0.0.1", 0)
    s.addr[1]
  ensure
    s&.close
  end

  def wait_port(port, timeout = 10)
    deadline = Time.now + timeout
    loop do
      Socket.tcp("127.0.0.1", port, connect_timeout: 0.2).close
      return
    rescue SystemCallError, IO::TimeoutError
      raise "server on port #{port} did not start" if Time.now > deadline

      sleep 0.05
    end
  end

  def start(dir, port, *extra)
    pid = Process.spawn(SERVER, dir, "127.0.0.1:#{port}", *extra, out: File::NULL, err: File::NULL)
    wait_port(port)
    pid
  end

  def stop(pid)
    Process.kill("TERM", pid)
    Process.wait(pid)
  rescue Errno::ESRCH, Errno::ECHILD
    nil
  end
end

class SingleNodeTest < Minitest::Test
  include ServerHelpers
  D = Storine.method(:Decimal)

  def setup
    skip "storage-server binary not built" unless File.executable?(SERVER)
    @dir = Dir.mktmpdir("storine_rb_")
    port = free_port
    @pid = start(@dir, port)
    @db = Storine.connect("storine://127.0.0.1:#{port}")
  end

  def teardown
    return unless @pid

    @db.close
    stop(@pid)
    FileUtils.rm_rf(@dir)
  end

  def test_crud_types_and_versions
    c = @db.collection("people")
    when_ = Time.utc(2024, 5, 1, 12, 30)
    uid = Storine::Uuid.random
    doc = { "name" => "Ann", "age" => 37, "big" => 1 << 100, "score" => 9.5, "price" => D.("19.99"),
            "joined" => when_, "id" => uid, "raw" => "\x00\xFF".b, "tags" => %w[a b], "address" => { "city" => "Lagos" },
            "nothing" => nil, "ok" => true }
    v1 = c.insert("ann", doc)
    got = c.get("ann")
    assert_equal v1, got.version
    assert_equal doc, got.to_h
    assert_equal "19.99", got["price"].to_s
    assert_raises(Storine::ConflictError) { c.insert("ann", doc) }
    v2 = c.update("ann", Storine.set("address.city", "Abuja"), Storine.unset("nothing"), Storine.append("tags", "c"))
    assert_operator v2, :>, v1
    got = c.get("ann")
    assert_equal({ "city" => "Abuja" }, got["address"])
    refute got.key?("nothing")
    assert_equal %w[a b c], got["tags"]
    assert_raises(Storine::ConflictError) { c.replace("ann", { "name" => "x" }, if_version: v1) }
    c.replace("ann", { "name" => "Ann B" }, if_version: v2)
    assert_equal({ "name" => "Ann B" }, c.get("ann").to_h)
    assert_raises(Storine::NotFoundError) { c.update("nobody", Storine.set("a", 1)) }
    assert c.delete("ann")
    refute c.delete("ann")
    assert_nil c.get("ann")
  end

  def test_money_is_exact_and_never_lost
    acct = @db.collection("accounts")
    acct.put("a", { "balance" => D.("0.00") })
    errors = Queue.new
    threads = Array.new(8) do
      Thread.new do
        100.times { acct.update("a", Storine.add("balance", D.("0.01"))) }
      rescue StandardError => e
        errors << e
      end
    end
    threads.each(&:join)
    assert errors.empty?
    assert_equal "8.00", acct.get("a")["balance"].to_s
    assert_raises(Storine::InvalidError) { acct.update("a", Storine.add("balance", D.("-8.01"), min: 0)) }
    assert_equal D.("8.00"), acct.get("a")["balance"]
  end

  def test_transaction_is_atomic
    acct = @db.collection("bank")
    acct.put("alice", { "balance" => D.("50") })
    acct.put("bob", { "balance" => D.("0") })
    @db.transaction do |tx|
      tx.update("bank", "alice", Storine.add("balance", D.("-20"), min: 0))
      tx.update("bank", "bob", Storine.add("balance", D.("20")))
    end
    assert_raises(Storine::InvalidError) do
      @db.transaction do |tx|
        tx.update("bank", "alice", Storine.add("balance", D.("-31"), min: 0))
        tx.update("bank", "bob", Storine.add("balance", D.("31")))
      end
    end
    assert_equal D.("30"), acct.get("alice")["balance"]
    assert_equal D.("20"), acct.get("bob")["balance"]
  end

  def test_queries_indexes_and_pagination
    items = @db.collection("items")
    120.times do |i|
      items.put(format("i%03d", i), { "n" => i, "kind" => i.even? ? "even" : "odd", "price" => D.(i) * D.("0.25") })
    end
    q = Storine.between("n", 10, 19) & Storine.eq("kind", "odd")
    before = items.find(q, order_by: "-n").map(&:key)
    assert_equal [19, 17, 15, 13, 11].map { |i| format("i%03d", i) }, before
    items.create_index("n")
    items.create_index("kind")
    assert_equal before, items.find(q, order_by: "-n").map(&:key)
    assert_includes items.info[:indexes], "n"
    assert_equal 19, items.count(Storine.gt("price", D.("25")))
    assert_equal 60, items.count({ "kind" => "even" })
    assert_equal 120, items.scan(batch: 7).count
    assert_equal "i119", items.first(order_by: "-price").key
    assert_includes @db.collections, "items"
  end

  def test_ttl_and_errors
    s = @db.collection("sessions")
    s.put("short", { "u" => 1 }, ttl: 0.2)
    refute_nil s.get("short")
    sleep 0.4
    assert_nil s.get("short")
    assert_raises(Storine::InvalidError) { s.put("bad", { "a.b" => 1 }) }
    assert_raises(Storine::InvalidError) { @db.collection("bad/name").put("k", {}) }
  end
end

class ClusterTest < Minitest::Test
  include ServerHelpers

  def test_three_nodes_redirect_and_failover
    skip "storage-server binary not built" unless File.executable?(SERVER)
    base = Dir.mktmpdir("storine_rb_cluster_")
    ports = Array.new(3) { free_port }
    peers = ports.each_with_index.map { |p, i| "#{i + 1}=127.0.0.1:#{p}" }.join(",")
    pids = ports.each_with_index.map do |p, i|
      Process.spawn(SERVER, File.join(base, "n#{i + 1}"), "127.0.0.1:#{p}", "--node-id", (i + 1).to_s, "--peers", peers,
                    out: File::NULL, err: File::NULL)
    end
    begin
      ports.each { |p| wait_port(p) }
      # Any seed works: the client follows redirects to the leader.
      db = Storine.connect("storine://" + ports.map { |p| "127.0.0.1:#{p}" }.join(","), timeout: 2.0)
      acct = db.collection("acct")
      deadline = Time.now + 15
      begin
        acct.put("a", { "bal" => Storine::Decimal("100") })
      rescue Storine::UnavailableError
        raise if Time.now > deadline

        sleep 0.1
        retry
      end
      10.times { acct.update("a", Storine.add("bal", Storine::Decimal("1"))) }
      assert_equal Storine::Decimal("110"), acct.get("a")["bal"]
      leader = db.cluster_info[:node_id]
      Process.kill("KILL", pids[leader - 1])
      Process.wait(pids[leader - 1])
      # The client fails over to the new leader; every deposit applies exactly once.
      5.times { acct.update("a", Storine.add("bal", Storine::Decimal("1"))) }
      assert_equal Storine::Decimal("115"), acct.get("a")["bal"]
      db.close
    ensure
      pids.each { |pid| stop(pid) }
      FileUtils.rm_rf(base)
    end
  end
end
