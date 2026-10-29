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

  def test_vector_search
    c = @db.collection("rb_docs")
    c.create_vector_index("emb", 3)
    assert_equal [{ field: "emb", dims: 3, ready: true }], c.vector_indexes
    [["a", [1, 0, 0], "x"], ["b", [0, 1, 0], "y"], ["c", [0.7, 0.7, 0], "x"]].each do |key, vec, kind|
      c.put(key, { "emb" => vec, "kind" => kind })
    end
    hits = c.nearest("emb", [1, 0.1, 0], k: 2)
    assert_equal %w[a c], hits.map(&:key)
    assert_operator hits[0].score, :>, hits[1].score
    assert_equal %w[a c], c.nearest("emb", [1, 0.1, 0], k: 5, where: { "kind" => "x" }, exact: true).map(&:key)
    assert_raises(Storine::InvalidError) { c.put("bad", { "emb" => [1, 2] }) } # wrong dimensions
    assert_raises(Storine::InvalidError) { c.nearest("emb", [1, 2], k: 1) }
    c.drop_vector_index("emb")
    assert_equal [], c.vector_indexes
  end

  def test_large_values
    c = @db.collection("rb_files")
    blob = Random.bytes(3 * 1024 * 1024 + 17)
    meta = { "size" => blob.bytesize, "type" => "image/png" }
    v = c.put("f1", { "data" => blob, "name" => "cat.png", "meta" => meta })
    d = c.get("f1", fields: ["name", "meta.size", "absent"])
    assert_equal [{ "name" => "cat.png", "meta" => { "size" => blob.bytesize } }, v], [d.to_h, d.version]
    assert_nil c.get("nope", fields: ["name"])
    r = c.read_bytes("f1", "data", 1000, 16)
    assert_equal [v, blob.bytesize, blob.byteslice(1000, 16)], [r.version, r.total, r.data]
    assert_equal "".b, c.read_bytes("f1", "data", blob.bytesize + 5, 10).data
    chunks = c.stream_bytes("f1", "data", chunk_size: 1 << 20).to_a
    assert_equal 4, chunks.length
    assert chunks.join == blob, "stream reassembles the value"
    assert_equal "cat".b, c.read_bytes("f1", "name", 0, 3).data
    assert_nil c.read_bytes("nope", "data")
    assert_raises(Storine::NotFoundError) { c.stream_bytes("nope", "data") { nil } }
    assert_raises(Storine::InvalidError) { c.read_bytes("f1", "meta") } # not bytes
    assert_raises(Storine::InvalidError) { c.read_bytes("f1", "data", 0, 17 * 1024 * 1024) }
    stream = c.stream_bytes("f1", "data", chunk_size: 1 << 20)
    stream.next
    c.update("f1", Storine.set("name", "dog.png"))
    assert_raises(Storine::ConflictError) { stream.next }
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

  def test_analytics
    sales = @db.collection("rb_sales")
    regions = %w[eu us apac]
    docs = {}
    3000.times do |i|
      d = { "region" => regions[i % 3], "qty" => (i % 17) + 1, "price" => Storine::Decimal.new((i % 997) + 1, 2),
            "day" => i % 30 }
      d["coupon"] = "C#{i % 5}" if (i % 7).zero?
      docs[format("s%05d", i)] = d
    end
    keys = docs.keys.sort
    keys.each_slice(250) do |slice|
      @db.transaction { |tx| slice.each { |k| tx.put("rb_sales", k, docs[k]) } }
    end

    avg6 = lambda do |total, n|
      r = Rational(total, n).round(6, half: :even)
      Storine::Decimal.new((r * 1_000_000).to_i, 6)
    end
    expect_by_region = lambda do |rows|
      regions.filter_map do |reg|
        ds = rows.select { |d| d["region"] == reg }
        next if ds.empty?

        { "region" => reg, "n" => ds.length, "revenue" => ds.sum(D.("0")) { |d| d["price"] * d["qty"] },
          "avg_qty" => avg6.(ds.sum { |d| d["qty"] }, ds.length), "cheapest" => ds.map { |d| d["price"] }.min,
          "dearest" => ds.map { |d| d["price"] }.max, "qtys" => ds.map { |d| d["qty"] }.uniq.size,
          "coupons" => ds.count { |d| d.key?("coupon") } }
      end.sort_by { |x| -x["revenue"] }
    end

    aggs = { "n" => Storine.count, "revenue" => Storine.sum(Storine.field("price") * "qty"), "avg_qty" => Storine.avg("qty"),
             "cheapest" => Storine.min("price"), "dearest" => Storine.max("price"), "qtys" => Storine.count_distinct("qty"),
             "coupons" => Storine.count("coupon") }
    expected = expect_by_region.(docs.values)
    check_main = lambda do
      res = sales.aggregate(aggs, group_by: ["region"], order_by: "-revenue", limit: 2)
      assert_equal 3000, res.matched
      assert_equal expected.first(2), res.rows
      assert_equal ["region", *aggs.keys], res.first.keys # column order
      full = sales.aggregate(aggs, group_by: "region", order_by: "region")
      assert_equal expected.sort_by { |x| x["region"] }, full.to_a
    end

    check_main.()
    assert_raises(Storine::InvalidError) { sales.aggregate(aggs, group_by: ["region"], order_by: "-nope") }
    total = sales.aggregate({ n: Storine.count, q: Storine.sum("qty"), neg: Storine.sum(-Storine.field("qty") + 1) })
    assert_equal [{ "n" => 3000, "q" => docs.values.sum { |d| d["qty"] }, "neg" => docs.values.sum { |d| 1 - d["qty"] } }],
                 total.rows

    sales.enable_columnar
    check_main.() # segments are built in the background: exact either way
    deadline = Time.now + 15
    while sales.aggregate({ "n" => Storine.count }, Storine.gt("qty", 1000)).pruned_row_groups.zero? # zone maps skip it
      flunk "column segments were never built" if Time.now > deadline
      sleep 0.05
    end
    check_main.()
    # Weekly buckets of day (offset 2): key = 2 + floor((day - 2) / 7) * 7.
    weeks = docs.values.group_by { |d| 2 + ((d["day"] - 2).div(7) * 7) }.sort
    res = sales.aggregate({ "n" => Storine.count, "q" => Storine.sum("qty") }, group_by: [Storine.bucket("day", 7, 2)],
                                                                             order_by: "day")
    assert_equal(weeks.map { |b, ds| [b, ds.length, ds.sum { |d| d["qty"] }] }, res.map { |r| [r["day"], r["n"], r["q"]] })
    late = docs.values.select { |d| d["day"] >= 20 }
    res = sales.aggregate(aggs, Storine.ge("day", 20), group_by: ["region"], order_by: "-revenue")
    assert_equal late.length, res.matched
    assert_equal expect_by_region.(late), res.rows

    eu_keys = keys.select { |k| docs[k]["region"] == "eu" }
    got = sales.scan_columns(%w[qty price], Storine.eq("region", "eu"), page_size: 300).to_a
    assert_equal eu_keys, got.map(&:first)
    assert_equal(eu_keys.map { |k| { "qty" => docs[k]["qty"], "price" => docs[k]["price"] } }, got.map(&:last))
    seen = 0
    sales.scan_columns(%w[qty absent], page_size: 500) do |key, values|
      assert_equal [docs[key]["qty"], nil], values.values_at("qty", "absent")
      seen += 1
    end
    assert_equal 3000, seen

    sales.disable_columnar
    check_main.()
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
