# frozen_string_literal: true

# Storine Ruby client.
#
#   require "storine"
#
#   db = Storine.connect("storine://127.0.0.1:7878")
#   accounts = db.collection("accounts")
#   accounts.insert("alice", { "balance" => Storine::Decimal("100.00") })
#   accounts.update("alice", Storine.add("balance", Storine::Decimal("-25.00"), min: 0)) # no overdraft, no lost update
#   accounts.get("alice")["balance"].to_s                                                # "75.00"
module Storine
  VERSION = "0.1.0"
end

require_relative "storine/errors"
require_relative "storine/decimal"
require_relative "storine/values"
require_relative "storine/wire"
require_relative "storine/client"
