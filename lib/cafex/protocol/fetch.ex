defmodule Cafex.Protocol.Fetch do
  use Cafex.Protocol, api_key: 1

  alias Cafex.Protocol.Message

  defrequest do
    field :replica_id, [default: -1], integer
    field :max_wait_time, [default: -1], integer
    field :min_bytes, [default: 0], integer
    field :topics, [default: []], [topic]

    @type topic :: {topic :: String.t, partitions :: [partition]}
    @type partition :: {partition :: integer,
                        offset :: integer,
                        max_bytes :: integer}
  end

  defresponse do
    field :topics, [topic]
    @type topic :: {topic :: String.t, partitions :: [partition]}
    @type partition :: %{partition: integer,
                         error: Cafex.Protocol.error,
                         hwm_offset: integer,
                         messages: [Message.t]}
  end

  def encode(%{replica_id: replica_id, max_wait_time: max_wait_time,
               min_bytes: min_bytes, topics: topics}) do
    [<< replica_id :: 32-signed, max_wait_time :: 32-signed, min_bytes :: 32-signed >>,
     encode_array(topics, &encode_topic/1)]
    |> IO.iodata_to_binary
  end

  defp encode_topic({topic, partitions}) do
    [encode_string(topic),
     encode_array(partitions, &encode_partition/1)]
  end
  defp encode_partition({partition, offset, max_bytes}) do
    << partition :: 32-signed, offset :: 64-signed, max_bytes :: 32-signed >>
  end

  @spec decode(binary) :: Response.t
  def decode(data) when is_binary(data) do
    {topics, _} = decode_array(data, &decode_topic/1)
    %Response{topics: topics}
  end

  defp decode_topic(<< size :: 16-signed, topic :: size(size)-binary, rest :: binary >>) do
    {partitions, rest} = decode_array(rest, &decode_partition/1)
    {{topic, partitions}, rest}
  end

  defp decode_partition(<< partition :: 32-signed,
                           error_code :: 16-signed,
                           hwm_offset :: 64-signed,
                           message_set_size :: 32-signed,
                           message_set :: size(message_set_size)-binary,
                           rest :: binary >>) do
    messages = decode_message_set(message_set)
    {%{partition: partition,
       error: decode_error(error_code),
       hwm_offset: hwm_offset,
       messages: messages}, rest}
  end
end
