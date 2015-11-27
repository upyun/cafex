defmodule Cafex.Protocol.OffsetCommit do
  @moduledoc """
  This api saves out the consumer's position in the stream for one or more partitions.

  The offset commit request support version 0, 1 and 2.
  To read more details, visit the [A Guide to The Kafka Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommitRequest).
  """

  use Cafex.Protocol, api_key: 8

  @default_consumer_group_generation_id -1
  @default_timestamp -1

  defrequest do
    field :api_version, [default: 0], api_version
    field :consumer_group, [default: "cafex"], String.t
    field :consumer_group_generation_id, integer | nil
    field :consumer_id, String.t | nil
    field :retention_time, integer | nil
    field :topics, [topic]

    @type api_version :: 0 | 1 | 2
    @type topic :: {topic_name :: String.t, partitions :: [partition]}
    @type partition :: partition_v0 | partition_v1 | partition_v2
    @type partition_v0 :: {partition :: integer, offset :: integer, metadata:: binary}
    @type partition_v1 :: {partition :: integer, offset :: integer, timestamp :: integer, metadata :: binary}
    @type partition_v2 :: {partition :: integer, offset :: integer, metadata:: binary}
  end

  defresponse do
    field :topics, [topic]
    @type topic :: {topic_name :: String.t, partitions :: [partition]}
    @type partition :: {partition :: integer, error :: Cafex.Protocol.error}
  end

  def api_version(%Request{api_version: api_version}), do: api_version

  def encode(request) do
    request |> fill_default |> do_encode
  end

  defp do_encode(%{api_version: 0} = request), do: encode_0(request)
  defp do_encode(%{api_version: 1} = request), do: encode_1(request)
  defp do_encode(%{api_version: 2} = request), do: encode_2(request)

  defp fill_default(%{api_version: version,
                      consumer_group_generation_id: id,
                      consumer_id: consumer_id,
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

    consumer_id = case consumer_id do
      nil -> ""
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
    %{request | consumer_group_generation_id: id, consumer_id: consumer_id, retention_time: time, topics: topics}
  end

  defp encode_0(%{consumer_group: consumer_group, topics: topics}) do
    [encode_string(consumer_group),
     encode_array(topics, &encode_topic_0/1)]
    |> IO.iodata_to_binary
  end

  defp encode_1(%{consumer_group: consumer_group,
                consumer_group_generation_id: consumer_group_generation_id,
                consumer_id: consumer_id,
                topics: topics}) do
    [encode_string(consumer_group),
     <<consumer_group_generation_id :: 32-signed>>,
     encode_string(consumer_id),
     encode_array(topics, &encode_topic_1/1)]
    |> IO.iodata_to_binary
  end

  defp encode_2(%{consumer_group: consumer_group,
                consumer_group_generation_id: consumer_group_generation_id,
                consumer_id: consumer_id,
                retention_time: retention_time,
                topics: topics}) do
    [encode_string(consumer_group),
     <<consumer_group_generation_id :: 32-signed>>,
     encode_string(consumer_id),
     <<retention_time :: 64>>,
     encode_array(topics, &encode_topic_2/1)]
    |> IO.iodata_to_binary
  end

  defp encode_topic_0(data), do: encode_topic(data, &encode_partition_0/1)
  defp encode_topic_1(data), do: encode_topic(data, &encode_partition_1/1)
  defp encode_topic_2(data), do: encode_topic(data, &encode_partition_2/1)

  defp encode_topic({topic, partitions}, func) do
    [encode_string(topic),
     encode_array(partitions, func)]
  end

  defp encode_partition_0({partition, offset, metadata}) do
    [<< partition :: 32-signed, offset :: 64 >>, encode_string(metadata)]
  end
  defp encode_partition_1({partition, offset, timestamp, metadata}) do
    [<< partition :: 32-signed, offset :: 64, timestamp :: 64 >>, encode_string(metadata)]
  end
  defp encode_partition_2(data), do: encode_partition_0(data)

  @spec decode(binary) :: Response.t
  def decode(data) when is_binary(data) do
    {topics, _} = decode_array(data, &decode_topic/1)
    %Response{topics: topics}
  end

  defp decode_topic(<< size :: 16-signed, topic :: size(size)-binary, rest :: binary >>) do
    {partitions, rest} = decode_array(rest, &decode_partition/1)
    {{topic, partitions}, rest}
  end

  defp decode_partition(<< partition :: 32-signed, error_code :: 16-signed, rest :: binary >>) do
    {{partition, decode_error(error_code)}, rest}
  end
end
