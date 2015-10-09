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

  def consume(msg) do
    # handle the msg
    :ok
  end
end

iex> Application.start :cafex
iex> {:ok, topic} = Cafex.start_topic "test", [{"127.0.0.1", 9092}]
iex> Cafex.start_consumer "test", "group_name", "127.0.0.1:2191/cafex", module: MyConsumer,
                                                                     client_id: "myconsumer"
```
