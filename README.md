Cafex
=====

### Producer

```elixir
iex> Application.start :cafex
iex> {:ok, topic} = Cafex.start_topic "test", [{"127.0.0.1", 9092}]
iex> {:ok, producer} = Cafex.start_producer topic, partitioner: MyPartitioner,
                                                     client_id: "myproducer"
iex> Cafex.produce producer, "message", key: "key"
```

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
iex> {:ok, pid} = Cafex.start_topic "test", [{"127.0.0.1", 9092}]
iex> {:ok, consumer} = Cafex.start_consumer pid, :myconsumer, client_id: "myconsumer",
                                                              zookeeper: [servers: [{"192.168.99.100", 2181}],
                                                                          path: "/cafex"],
                                                              handler: {MyConsumer, []}                                         
```

`start_consumer` 的 `options` 可以放在 `config/config.exs` 中：

```elixir
config :cafex, :myconsumer,
  client_id: "cafex",
  zookeeper: [
    servers: [{"192.168.99.100", 2181}],
    path: "/elena/cafex"
  ],
  handler: {MyConsumer, []}
```

### TODO

* Simple Consumer
* 目前有很多参数写死在代码里的, 增加更多参数的配置选项
