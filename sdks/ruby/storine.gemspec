# frozen_string_literal: true

require_relative "lib/storine"

Gem::Specification.new do |s|
  s.name = "storine"
  s.version = Storine::VERSION
  s.summary = "Client for the Storine document database"
  s.description = "Standard-library-only client: exact decimals, atomic server-side updates, " \
                  "at-most-once retries and cluster failover."
  s.authors = ["Storine contributors"]
  s.license = "MIT"
  s.files = Dir["lib/**/*.rb", "README.md"]
  s.required_ruby_version = ">= 3.2"
  s.add_dependency "bigdecimal" # a default gem (bundled from Ruby 3.4); used only for conversions
end
