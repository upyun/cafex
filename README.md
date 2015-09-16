Cafex
=====


```elixir
iex> Application.start :cafex
iex> {:ok, topic} = Cafex.start_topic "test"
iex> {:ok, producer} = Cafex.start_producer topic, brokers: [{"127.0.0.1", 9092}],
                                               partitioner: MyPartitioner
iex> Cafex.produce producer, "message", key: "key"
```

TODO: remove KafkaEx
