Cafex
=====

### Producer

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

### Consumer

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
iex> {:ok, consumer} = Cafex.start_consumer :myconsumer, topic_name, client_id: "myconsumer",
                                                                     brokers: brokers,
                                                                     zookeeper: [servers: [{"192.168.99.100", 2181}],
                                                                                 path: "/cafex"],
                                                                     handler: {MyConsumer, []}
```

`start_consumer` 的 `options` 可以放在 `config/config.exs` 中：

```elixir
config :cafex, :myconsumer,
  client_id: "cafex",
  brokers: [{"192.168.99.100", 9092}, {"192.168.99.101", 9092}]
  zookeeper: [
    servers: [{"192.168.99.100", 2181}],
    path: "/elena/cafex"
  ],
  handler: {MyConsumer, []}
```

Consumer 启动后会在 zookeeper 上建立下面这样的建构

```
  /cafex
   |-- topic
   |  |-- group_name
   |  |  |-- leader
   |  |  |-- consumers
   |  |  |  |-- balance
   |  |  |  |  |-- cafex@192.168.0.1       - [0,1,2,3]     # persistent
   |  |  |  |  |-- cafex@192.168.0.2       - [4,5,6,7]     # persistent
   |  |  |  |  |-- cafex@192.168.0.3       - [8,9,10,11]   # persistent
   |  |  |  |-- online
   |  |  |  |  |-- cafex@192.168.0.1                       # ephemeral
   |  |  |  |  |-- cafex@192.168.0.2                       # ephemeral
   |  |  |  |-- offline
   |  |  |  |  |-- cafex@192.168.0.3                       # persistent
   |  |  |-- locks
```

首先，每个 Consumer 启动后会在 `consumers/online` 节点下面注册自己（目前是用 erlang node name 作为 consumer 的 name, 所以启动时务必指定 `-name` 参数）。
所有 Consumer 进程会选举出一个 Leader，只有这个 Leader 负责负载均衡。
Leader 获取 `consumers/online` 和 `consumers/offline` 下面的所有节点，然后作负载均衡，并将结果（也就是每个 consumer 负责的 partition 列表）写入 `consumers/balance` 下的各 consumer 节点。
每个 Consumer 都监听着 `consumers/balance` 下自己的相应节点的数据变化，发生变化时启动，或者关闭相关的 partition worker。

### TODO

* Simple Consumer
* Add tests
