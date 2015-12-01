defmodule Cafex.Protocol.OffsetFetch do
  @moduledoc """
  This api reads back a consumer position previously written using the OffsetCommit api.

  The offset fetch request support version 0, 1 and 2.
  To read more details, visit the [A Guide to The Kafka Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetFetchRequest).
  """

  use Cafex.Protocol, api_key: 9

  defrequest do
    field :api_version, [default: 0], api_version
    field :consumer_group, binary
    field :topics, [topic]
    @type api_version :: 0 | 1
    @type topic :: {topic_name :: String.t, partitions :: [partition]}
    @type partition :: integer
  end

  defresponse do
    field :topics, [topic]
    @type topic :: {partition :: integer,
                       offset :: integer,
                     metadata :: String.t,
                       error  :: Cafex.Protocol.error}
  end

  def api_version(%{api_version: api_version}), do: api_version

  def encode(%{consumer_group: consumer_group, topics: topics}) do
    [encode_string(consumer_group),
     encode_array(topics, &encode_topic/1)]
    |> IO.iodata_to_binary
  end

  defp encode_topic({topic, partitions}) do
    [encode_string(topic),
     encode_array(partitions, &encode_partition/1)]
  end
  defp encode_partition(partition), do: << partition :: 32-signed >>

  @spec decode(binary) :: Response.t
  def decode(data) when is_binary(data) do
    {topics, _} = decode_array(data, &decode_topic/1)
    %Response{topics: topics}
  end

  defp decode_topic(<< size :: 16-signed, topic :: size(size)-binary, rest :: binary >>) do
    {partitions, rest} = decode_array(rest, &decode_partition/1)
    {{topic, partitions}, rest}
  end

  defp decode_partition(<< partition :: 32-signed, offset :: 64-signed,
                           -1 :: 16-signed, error_code :: 16-signed, rest :: binary >>) do
    {{partition, offset, "", decode_error(error_code)}, rest}
  end
  defp decode_partition(<< partition :: 32-signed, offset :: 64-signed,
                           size :: 16-signed, metadata :: size(size)-binary,
                           error_code :: 16-signed, rest :: binary >>) do
    {{partition, offset, metadata, decode_error(error_code)}, rest}
  end
end
