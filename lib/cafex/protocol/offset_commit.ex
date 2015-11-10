defmodule Cafex.Protocol.OffsetCommit do
  @behaviour Cafex.Protocol.Decoder

  @moduledoc """
  The offset commit request support version 0, 1 and 2.
  To read more details, visit the [A Guide to The Kafka Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommitRequest).
  """

  @default_consumer_group_generation_id -1
  @default_timestamp -1

  defmodule Request do
    defstruct api_version: 0,
              consumer_group: "cafex",
              consumer_group_generation_id: nil,
              consumer_id: nil,
              retention_time: nil,
              topics: []

    @type t :: %Request{api_version: 0 | 1 | 2,
                        consumer_group: String.t,
                        consumer_group_generation_id: integer | nil,
                        consumer_id: String.t | nil,
                        retention_time: integer | nil,
                        topics: [{topic_name :: String.t,
                                  partitions :: [{partition :: integer,
                                                  offset :: integer,
                                                  timestamp :: integer,
                                                  metadata :: binary}
                                                |{partition :: integer,
                                                  offset :: integer,
                                                  metadata:: binary}]}]}
  end

  defmodule Response do
    defstruct topics: []

    @type t :: %Response{ topics: [{topic_name :: String.t,
                                    partitions :: [{partition :: integer,
                                                        error :: Cafex.Protocol.Errors.t}]}] }
  end

  defimpl Cafex.Protocol.Request, for: Request do
    def api_key(_), do: 8
    def api_version(_), do: 0
    def encode(request) do
      Cafex.Protocol.OffsetCommit.encode(request)
    end
  end

  def encode(request) do
    request |> fill_default |> do_encode
  end

  defp do_encode(%{api_version: 0} = request), do: encode_0(request)
  defp do_encode(%{api_version: 1} = request), do: encode_1(request)
  defp do_encode(%{api_version: 2} = request), do: encode_2(request)

  defp fill_default(%{api_version: version,
                      consumer_group_generation_id: id,
                      retention_time: time,
                      topics: topics} = request) do
    id = case id do
      nil -> @default_consumer_group_generation_id
      other -> other
    end

    time = case time do
      nil -> @default_timestamp
      other -> other
    end

    topics = case version do
      1 ->
        Enum.map(topics, fn {topic_name, partitions} ->
          partitions = Enum.map(partitions, fn
            {p, o, m} -> {p, o, @default_timestamp, m}
            {_, _, _, _} = partition -> partition
          end)
          {topic_name, partitions}
        end)
      _ -> topics
    end
    %{request | consumer_group_generation_id: id, retention_time: time, topics: topics}
  end

  defp encode_0(%{consumer_group: consumer_group, topics: topics}) do
    [Cafex.Protocol.encode_string(consumer_group),
     Cafex.Protocol.encode_array(topics, &encode_topic_0/1)]
    |> IO.iodata_to_binary
  end

  defp encode_1(%{consumer_group: consumer_group,
                consumer_group_generation_id: consumer_group_generation_id,
                consumer_id: consumer_id,
                topics: topics}) do
    [Cafex.Protocol.encode_string(consumer_group),
     <<consumer_group_generation_id :: 32-signed>>,
     Cafex.Protocol.encode_string(consumer_id),
     Cafex.Protocol.encode_array(topics, &encode_topic_1/1)]
    |> IO.iodata_to_binary
  end

  defp encode_2(%{consumer_group: consumer_group,
                consumer_group_generation_id: consumer_group_generation_id,
                consumer_id: consumer_id,
                retention_time: retention_time,
                topics: topics}) do
    [Cafex.Protocol.encode_string(consumer_group),
     <<consumer_group_generation_id :: 32-signed>>,
     Cafex.Protocol.encode_string(consumer_id),
     <<retention_time :: 64>>,
     Cafex.Protocol.encode_array(topics, &encode_topic_2/1)]
    |> IO.iodata_to_binary
  end

  defp encode_topic_0(data), do: encode_topic(data, &encode_partition_0/1)
  defp encode_topic_1(data), do: encode_topic(data, &encode_partition_1/1)
  defp encode_topic_2(data), do: encode_topic(data, &encode_partition_2/1)

  defp encode_topic({topic, partitions}, func) do
    [Cafex.Protocol.encode_string(topic),
     Cafex.Protocol.encode_array(partitions, func)]
  end

  defp encode_partition_0({partition, offset, metadata}) do
    [<< partition :: 32-signed, offset :: 64 >>, Cafex.Protocol.encode_string(metadata)]
  end
  defp encode_partition_1({partition, offset, timestamp, metadata}) do
    [<< partition :: 32-signed, offset :: 64, timestamp :: 64 >>, Cafex.Protocol.encode_string(metadata)]
  end
  defp encode_partition_2(data), do: encode_partition_0(data)

  def decode(data) when is_binary(data) do
    {topics, _} = Cafex.Protocol.decode_array(data, &decode_topic/1)
    %Response{topics: topics}
  end

  defp decode_topic(<< size :: 16-signed, topic :: size(size)-binary, rest :: binary >>) do
    {partitions, rest} = Cafex.Protocol.decode_array(rest, &decode_partition/1)
    {{topic, partitions}, rest}
  end

  defp decode_partition(<< partition :: 32-signed, error_code :: 16-signed, rest :: binary >>) do
    {{partition, Cafex.Protocol.Errors.error(error_code)}, rest}
  end
end
