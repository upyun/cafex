Cafex
=====

Cafex is a pure Elixir implementation of [Kafka][kafka] client with [ZooKeeper][zookeeper] and [Consul][consul.io] intergration.

Cafex support Kafka 0.8 and 0.9 group membership APIs.

Cafex provides all kafka APIs encapsulation, producer implementation and high-level consumer implementation.

## Producer

### Example

```elixir
iex> Application.start :cafex
iex> topic_name = "test_topic"
iex> brokers = [{"127.0.0.1", 9092}]
iex> {:ok, producer} = Cafex.start_producer topic_name, client_id: "myproducer",
                                                        brokers: brokers,
                                                        partitioner: MyPartitioner,
                                                        acks: 1,
                                                        batch_num: 100,
                                                        linger_ms: 10
iex> Cafex.produce producer, "message", key: "key"
iex> Cafex.async_produce producer, "message", key: "key"
```

### Producer options

#### `partitioner`

The partitioner for partitioning messages amongst sub-topics.
The default partitioner is `Cafex.Partitioner.Random`.

#### `client_id`

The client id is a user-specified string sent in each request to help trace
calls.  It should logically identify the application making the request.

Default `cafex_producer`.

#### `acks`

The number of acknowledgments the producer requires the leader to have received
before considering a request complete. This controls the durability of records
that are sent.

Default value is `1`.

#### `batch_num`

The number of messages to send in one batch when `linger_ms` is not zero.
The producer will wait until either this number of messages are ready to send.

#### `linger_ms`
This setting is the same as `linger.ms` config in the new official producer configs.
This setting defaults to 0 (i.e. no delay).

> NOTE: If `linger_ms` is set to `0`, the `batch_num` will not take effect.

## Consumer

### Example

```elixir
defmodule MyConsumer do
  use Cafex.Consumer

  def consume(msg, state) do
    # handle the msg
    {:ok, state}
  end
end

iex> Application.start :cafex
iex> topic_name = "test_topic"
iex> brokers = [{"127.0.0.1", 9092}]
iex> options = [client_id: "myconsumer",
                topic: topic_name,
                brokers: brokers,
                offset_storage: :kafka,
                group_manager: :kafka,
                lock: :consul,
                group_session_timeout: 7000, # ms
                auto_commit: true,
                auto_commit_interval: 500,   # ms
                auto_commit_max_buffers: 50,
                fetch_wait_time: 100,        # ms
                fetch_min_bytes: 32 * 1024,
                fetch_max_bytes: 64 * 1024,
                handler: {MyConsumer, []}]
iex> {:ok, consumer} = Cafex.start_consumer :myconsumer, options
```

The `options` argument of the function `start_consumer` can be put in the
`config/config.exs`:

```elixir
config :cafex, :myconsumer,
  client_id: "cafex",
  topic: "test_topic",
  brokers: [
    {"192.168.99.100", 9092},
    {"192.168.99.101", 9092}
  ],
  offset_storage: :kafka,
  group_manager: :kafka,
  lock: :consul,
  group_session_timeout: 7000, # ms
  auto_commit: true,
  auto_commit_interval: 500,   # ms
  auto_commit_max_buffers: 50,
  fetch_wait_time: 100,        # ms
  fetch_min_bytes: 32 * 1024,
  fetch_max_bytes: 64 * 1024,
  handler: {MyConsumer, []}
```

By default, cafex will use `:kafka` as the offset storage, use the new kafka
group membership API, which was added in the 0.9.x, as the group manager,
and use the `:consul` as the worker lock. Make suer your Kafka server is 0.9.x
or above.

But `:zookeeper` is another option for these. If you use zookeeper, the starting
options of `:erlzk` must be specified under the `:zookeeper` key:

```elixir
config :cafex, :myconsumer,
  client_id: "cafex",
  topic: "test_topic",
  brokers: [...],
  offset_storage: :zookeeper,
  group_manager: :zookeeper,
  lock: :zookeeper,
  zookeeper: [
    timeout: 5000,
    servers: [{"192.168.99.100", 2181}],
    chroot: "/cafex"
  ],
  ...
```

## TODO

* Support kafka 0.10.x.x
* Add tests

[kafka]: http://kafka.apache.org
[zookeeper]: http://zookeeper.apache.org
[consul.io]: https://consul.io
