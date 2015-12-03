# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
use Mix.Config

# This configuration is loaded before any dependency and is restricted
# to this project. If another project depends on this project, this
# file won't be loaded nor affect the parent project. For this reason,
# if you want to provide default values for your application for third-
# party users, it should be done in your mix.exs file.

# Sample configuration:
#
#     config :logger, :console,
#       level: :info,
#       format: "$date $time [$level] $metadata$message\n",
#       metadata: [:user_id]

# It is also possible to import configuration files, relative to this
# directory. For example, you can emulate configuration per environment
# by uncommenting the line below and defining dev.exs, test.exs and such.
# Configuration from the imported file will override the ones defined
# here (which is why it is important to import them last).
#
#     import_config "#{Mix.env}.exs"

config :cafex, :myconsumer,
  client_id: "cafex",
  offset_storage: :kafka,
  auto_commit: true,
  auto_commit_interval: 500, # ms
  auto_commit_max_buffers: 50,
  zookeeper: [
    timeout: 5000,
    servers: [{"192.168.99.100", 2181}],
    path: "/elena/cafex"
  ],
  wait_time: 100,
  min_bytes: 32 * 1024,
  max_bytes: 1024 * 1024,
  handler: {Cafex.Consumer.LogConsumer, [level: :debug]}

config :cafex, :myconsumer2,
  client_id: "cafex",
  zookeeper: [
    servers: [{"192.168.99.100", 2181}],
    path: "/elena/cafex"
  ],
  handler: {Cafex.Consumer.LogConsumer, [level: :debug]}

config :logger,
  level: :debug

config :logger, :console,
  format: "$date $time $metadata[$level] $levelpad$message\n"

if File.exists? Path.join([__DIR__, "#{Mix.env}.exs"]) do
  import_config "#{Mix.env}.exs"
end
