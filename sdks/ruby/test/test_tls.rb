# frozen_string_literal: true

# TLS on the native port, with certificates from `storage-server gen-ca` / `gen-cert`.

require_relative "test_e2e"

class TlsTest < Minitest::Test
  include ServerHelpers

  def test_tls_round_trip_and_untrusted_server
    skip "storage-server binary not built" unless File.executable?(SERVER)
    Dir.mktmpdir("storine_rb_tls_") do |d|
      [["gen-ca", "#{d}/ca"], ["gen-cert", "#{d}/ca", "#{d}/node", "127.0.0.1,localhost"], ["gen-ca", "#{d}/other"]].each do |args|
        assert system(SERVER, *args, out: File::NULL), args.join(" ")
      end
      port = free_port
      env = { "STORAGE_TLS_CERT" => "#{d}/node/node.crt", "STORAGE_TLS_KEY" => "#{d}/node/node.key" }
      pid = Process.spawn(env, SERVER, "#{d}/data", "127.0.0.1:#{port}", out: File::NULL, err: File::NULL)
      begin
        wait_port(port)
        big = "x" * 100_000
        db = Storine.connect("storines://127.0.0.1:#{port}", tls_ca: "#{d}/ca/ca.crt")
        db.collection("t").put("k", { "v" => big })
        assert_equal big, db.collection("t").get("k")["v"]
        named = Storine.connect("storines://127.0.0.1:#{port}?tls_ca=#{d}/ca/ca.crt&tls_server_name=localhost")
        refute_nil named.collection("t").get("k")
        other = Storine.connect("storines://127.0.0.1:#{port}", tls_ca: "#{d}/other/ca.crt", timeout: 1.0)
        assert_raises(StandardError) { other.collection("t").get("k") }
      ensure
        stop(pid)
      end
    end
  end
end
