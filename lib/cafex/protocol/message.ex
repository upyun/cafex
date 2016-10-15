defmodule Cafex.Protocol.Message do
  alias __MODULE__, as: Message
  defstruct topic: nil,
            partition: nil,
            value: nil,
            key: nil,
            offset: 0,
            magic_byte: 0,
            attributes: 0,
            timestamp_type: nil,
            compression: nil,
            metadata: nil,
            timestamp: 0

  @type t :: %Message{ topic: binary,
                       partition: integer,
                       value: binary,
                       key: binary,
                       offset: integer,
                       magic_byte: integer,
                       attributes: integer,
                       compression: compression,
                       metadata: term,
                       timestamp: integer,
                       timestamp_type: atom }

  @type compression :: nil | :gzip | :snappy

  @type tuple_message :: {topic :: String.t, partition :: integer, value :: binary} |
                         {topic :: String.t, partition :: integer, value :: binary, key :: binary | nil}

  @spec from_tuple(tuple_message) :: Message.t
  def from_tuple({topic, partition, value}), do: from_tuple({topic, partition, value, nil})
  def from_tuple({topic, partition, value, key}) do
    %Message{ topic: topic,
              partition: partition,
              value: value,
              key: key }
  end
end
