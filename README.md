Cafex
=====


```elixir
iex> Application.start :cafex
iex> {:ok, topic} = Cafex.start_topic "test", [{"127.0.0.1", 9092}]
iex> {:ok, producer} = Cafex.start_producer topic, partitioner: MyPartitioner,
                                                     client_id: "myproducer"
iex> Cafex.produce producer, "message", key: "key"
```

TODO: remove KafkaEx
