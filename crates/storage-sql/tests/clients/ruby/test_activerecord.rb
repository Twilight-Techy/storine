# ActiveRecord (Rails) against storine-sql: the postgresql adapter and the trilogy (MySQL)
# adapter, with migrations, associations, aggregates, locking and transactions.
#
#   storine-sql --data /tmp/db --pg 127.0.0.1:55432 --mysql 127.0.0.1:53306 &
#   gem install activerecord pg trilogy
#   ruby test_activerecord.rb [pg_port] [mysql_port]

require "active_record"
require "bigdecimal"

PG_PORT = (ARGV[0] || 55432).to_i
MY_PORT = (ARGV[1] || 53306).to_i

def check(ok, what)
  return if ok
  warn "FAILED: #{what}"
  exit 1
end

class Account < ActiveRecord::Base
  self.table_name = "ar_accounts"
  has_many :entries, dependent: :destroy
  validates :email, presence: true
end

class Entry < ActiveRecord::Base
  self.table_name = "ar_entries"
  belongs_to :account
end

class CreateLedger < ActiveRecord::Migration[8.0]
  def change
    create_table :ar_accounts do |t|
      t.string :email, null: false, index: { unique: true }
      t.decimal :balance, precision: 12, scale: 2, default: 0, null: false
      t.integer :lock_version, default: 0, null: false
      t.json :prefs
      t.timestamps
    end
    create_table :ar_entries do |t|
      t.references :account, null: false, foreign_key: { to_table: :ar_accounts }
      t.decimal :amount, precision: 12, scale: 2, null: false
      t.string :memo
      t.timestamps
    end
    add_column :ar_entries, :posted, :boolean, default: false
    add_index :ar_entries, [:account_id, :posted]
  end
end

[
  ["postgresql", { adapter: "postgresql", host: "127.0.0.1", port: PG_PORT, database: "shop", username: "app", password: "x" }],
  ["trilogy", { adapter: "trilogy", host: "127.0.0.1", port: MY_PORT, database: "shop", username: "root", password: "", ssl: false }],
].each do |label, config|
  ActiveRecord::Base.establish_connection(config)
  conn = ActiveRecord::Base.connection
  ActiveRecord::Migration.verbose = false
  conn.drop_table(:ar_entries, if_exists: true)
  conn.drop_table(:ar_accounts, if_exists: true)
  CreateLedger.migrate(:up)
  check(conn.table_exists?(:ar_entries) && conn.column_exists?(:ar_entries, :posted), "#{label}: schema")
  check(conn.index_exists?(:ar_accounts, :email, unique: true), "#{label}: unique index")
  [Account, Entry].each(&:reset_column_information)

  ada = Account.create!(email: "ada@x", balance: BigDecimal("100.00"), prefs: { "tier" => "gold" })
  bob = Account.create!(email: "bob@x")
  ada.entries.create!([{ amount: BigDecimal("1.10") }, { amount: BigDecimal("2.20"), posted: true }])

  a = Account.includes(:entries).find_by!(email: "ada@x")
  check(a.entries.size == 2, "#{label}: includes")
  check(a.balance == BigDecimal("100.00") && a.balance.is_a?(BigDecimal), "#{label}: decimal")
  check(a.prefs == { "tier" => "gold" }, "#{label}: json #{a.prefs.inspect}")
  sums = Account.left_joins(:entries).group(:email).order(:email).sum(:amount)
  check(sums == { "ada@x" => BigDecimal("3.30"), "bob@x" => 0 }, "#{label}: group sum #{sums.inspect}")
  check(Entry.where(posted: true).joins(:account).where(ar_accounts: { email: "ada@x" }).count == 1, "#{label}: joins")
  check(Account.where("email LIKE ?", "b%").pluck(:email) == ["bob@x"], "#{label}: pluck")

  # Atomic counters from several threads: no lost money.
  threads = 4.times.map do
    Thread.new do
      ActiveRecord::Base.connection_pool.with_connection do
        25.times { Account.update_counters(ada.id, balance: 1) }
      end
    end
  end
  threads.each(&:join)
  check(ada.reload.balance == BigDecimal("200.00"), "#{label}: update_counters #{ada.balance}")

  # Pessimistic locking inside transactions.
  threads = 3.times.map do
    Thread.new do
      ActiveRecord::Base.connection_pool.with_connection do
        10.times do
          Account.transaction do
            src = Account.lock.find(ada.id)
            dst = Account.lock.find(bob.id)
            src.update!(balance: src.balance - 1)
            dst.update!(balance: dst.balance + 1)
          end
        rescue ActiveRecord::StaleObjectError, ActiveRecord::SerializationFailure, ActiveRecord::Deadlocked
          retry
        end
      end
    end
  end
  threads.each(&:join)
  check(ada.reload.balance == BigDecimal("170.00") && bob.reload.balance == BigDecimal("30.00"), "#{label}: locked transfers #{ada.balance} #{bob.balance}")

  # Optimistic locking with lock_version.
  x1 = Account.find(ada.id)
  x2 = Account.find(ada.id)
  x1.update!(balance: 1)
  begin
    x2.update!(balance: 2)
    check(false, "#{label}: stale update accepted")
  rescue ActiveRecord::StaleObjectError
  end

  Account.transaction do
    bob.update!(balance: 0)
    raise ActiveRecord::Rollback
  end
  check(bob.reload.balance == BigDecimal("30.00"), "#{label}: rollback")
  begin
    Account.create!(email: "ada@x")
    check(false, "#{label}: duplicate accepted")
  rescue ActiveRecord::RecordNotUnique
  end
  Account.upsert_all([{ email: "bob@x", balance: 5 }, { email: "cy@x", balance: 7 }], unique_by: :email) if label == "postgresql"
  ada.reload.destroy!
  check(Entry.count == 0, "#{label}: dependent destroy")
  CreateLedger.migrate(:down)
  check(!conn.table_exists?(:ar_accounts), "#{label}: migrate down")
  ActiveRecord::Base.remove_connection
  puts "OK  ActiveRecord (#{label})"
end
puts "all activerecord tests passed"
