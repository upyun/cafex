# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
use Mix.Config

# This configuration is loaded before any dependency and is restricted
# to this project. If another project depends on this project, this
# file won't be loaded nor affect the parent project. For this reason,
# if you want to provide default values for your application for third-
# party users, it should be done in your mix.exs file.

# Sample configuration of consumers:
#
# config :cafex, :myconsumer,
#   client_id: "cafex",
#   topic: "a_topic",
#   brokers: [
#     {"192.168.99.100", 9902},
#     {"192.168.99.101", 9902},
#     {"192.168.99.102", 9902}
#   ],
#   offset_storage: :kafka, # :kafka or :zookeeper
#   group_manager: :kafka,  # :kafka or :zookeeper
#   lock: :consul,          # :consul or :zookeeper
#    group_session_timeout: 7000, # ms
#   auto_commit: true,
#   auto_commit_interval: 500,    # ms
#   auto_commit_max_buffers: 50,
#   fetch_wait_time: 100,         # ms
#   fetch_min_bytes: 32 * 1024,
#   fetch_max_bytes: 1024 * 1024,
#   handler: {Cafex.Consumer.LogConsumer, [level: :debug]}
#
# config :cafex, :myconsumer2,
#   client_id: "cafex",
#   group_manager: :zookeeper,
#   locke: :zookeeper,
#   zookeeper: [
#     timeout: 5000,              # ms
#     servers: [{"192.168.99.100", 2181}],
#     path: "/elena/cafex"
#   ],
#   handler: {Cafex.Consumer.LogConsumer, [level: :debug]}

config :consul,
  host: "localhost",
  port: 8500

config :logger,
  level: :debug

config :logger, :console,
  format: "$date $time $metadata[$level] $levelpad$message\n"

if File.exists? Path.join([__DIR__, "#{Mix.env}.exs"]) do
  import_config "#{Mix.env}.exs"
end
