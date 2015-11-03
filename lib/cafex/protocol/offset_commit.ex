defmodule Cafex.Protocol.OffsetCommit do
  @behaviour Cafex.Protocol.Decoder

  @moduledoc """
  The offset commit request uses v0 (supported in 0.8.1 or later).
  To read more details, visit the [A Guide to The Kafka Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommitRequest).
  """

  defmodule Request do
    defstruct consumer_group: "cafex",
              topics: []

    @type t :: %Request{consumer_group: String.t,
                        topics: [{topic_name :: String.t,
                                  partitions :: [{partition :: integer,
                                                  offset :: integer,
                                                  metadata :: binary}]}]}
  end

  defmodule Response do
    defstruct topics: []

    @type t :: %Response{ topics: [{topic_name :: String.t,
                                    partitions :: [{partition :: integer,
                                                        error :: Cafex.Protocol.Errors.t}]}] }
  end

  defimpl Cafex.Protocol.Request, for: Request do
    def api_key(_), do: 8
    def encode(request) do
      Cafex.Protocol.OffsetCommit.encode(request)
    end
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
  defp encode_partition({partition, offset, metadata}) do
    [<< partition :: 32-signed, offset :: 64 >>, Cafex.Protocol.encode_string(metadata)]
  end

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
