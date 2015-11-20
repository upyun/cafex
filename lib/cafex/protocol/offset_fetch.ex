defmodule Cafex.Protocol.OffsetFetch do
  @moduledoc """
  This api reads back a consumer position previously written using the OffsetCommit api.

  The offset fetch request support version 0, 1 and 2.
  To read more details, visit the [A Guide to The Kafka Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetFetchRequest).
  """

  @behaviour Cafex.Protocol.Decoder

  defmodule Request do
    use Cafex.Protocol

    @api_key 9

    defstruct api_version: 0,
              consumer_group: nil,
              topics: []

    @typedoc """
    OffsetFetch API now support v0 (supported in 0.8.1 or later), v1 (supported in 0.8.2 or later)

    To read more details, visit the [OffsetFetchRequest protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetFetchRequest).
    """
    @type api_version :: 0 | 1
    @type topic :: {topic_name :: String.t, partitions :: [partition]}
    @type partition :: integer
    @type t :: %Request{api_version: api_version,
                        consumer_group: binary,
                        topics: [topic]}

    def api_version(%{api_version: api_version}), do: api_version
    def encode(request) do
      Cafex.Protocol.OffsetFetch.encode(request)
    end
  end

  defmodule Response do
    defstruct topics: []

    @type topic :: {partition :: integer,
                       offset :: integer,
                     metadata :: String.t,
                       error  :: Cafex.Protocol.Errors.t}
    @type t :: %Response{topics: [topic]}
  end

  def encode(%{consumer_group: consumer_group, topics: topics}) do
    [Cafex.Protocol.encode_string(consumer_group),
     Cafex.Protocol.encode_array(topics, &encode_topic/1)]
    |> IO.iodata_to_binary
  end

  defp encode_topic({topic, partitions}) do
    [Cafex.Protocol.encode_string(topic),
     Cafex.Protocol.encode_array(partitions, &encode_partition/1)]
  end
  defp encode_partition(partition), do: << partition :: 32-signed >>

  @spec decode(binary) :: Response.t
  def decode(data) when is_binary(data) do
    {topics, _} = Cafex.Protocol.decode_array(data, &decode_topic/1)
    %Response{topics: topics}
  end

  defp decode_topic(<< size :: 16-signed, topic :: size(size)-binary, rest :: binary >>) do
    {partitions, rest} = Cafex.Protocol.decode_array(rest, &decode_partition/1)
    {{topic, partitions}, rest}
  end

  defp decode_partition(<< partition :: 32-signed, offset :: 64-signed,
                           -1 :: 16-signed, error_code :: 16-signed, rest :: binary >>) do
    {{partition, offset, "", Cafex.Protocol.Errors.error(error_code)}, rest}
  end
  defp decode_partition(<< partition :: 32-signed, offset :: 64-signed,
                           size :: 16-signed, metadata :: size(size)-binary,
                           error_code :: 16-signed, rest :: binary >>) do
    {{partition, offset, metadata, Cafex.Protocol.Errors.error(error_code)}, rest}
  end
end
