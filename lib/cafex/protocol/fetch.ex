defmodule Cafex.Protocol.Fetch do
  @behaviour Cafex.Protocol.Decoder

  alias Cafex.Protocol.Message

  defmodule Request do
    defstruct replica_id: -1,
              max_wait_time: -1,
              min_bytes: 0,
              topics: []

    @type t :: %Request{replica_id: integer,
                        max_wait_time: integer,
                        min_bytes: integer,
                        topics: [{topic :: String.t,
                                  partitions :: [{partition :: integer,
                                                  offset :: integer,
                                                  max_bytes :: integer}]}]}
  end

  defmodule Response do
    defstruct topics: []
    @type t :: %Response{topics: [{topic :: String.t,
                                   partitions :: [{partition :: integer,
                                                   error :: Cafex.Protocol.Errors.t,
                                                   hwm_offset :: integer,
                                                   messages :: [Message.t]}]}]}
  end

  defimpl Cafex.Protocol.Request, for: Request do
    def api_key(_), do: 1
    def encode(request) do
      Cafex.Protocol.Fetch.encode(request)
    end
  end

  def encode(%{replica_id: replica_id, max_wait_time: max_wait_time,
               min_bytes: min_bytes, topics: topics}) do
    [<< replica_id :: 32-signed, max_wait_time :: 32-signed, min_bytes :: 32-signed >>,
     Cafex.Protocol.encode_array(topics, &encode_topic/1)]
    |> IO.iodata_to_binary
  end

  defp encode_topic({topic, partitions}) do
    [Cafex.Protocol.encode_string(topic),
     Cafex.Protocol.encode_array(partitions, &encode_partition/1)]
  end
  defp encode_partition({partition, offset, max_bytes}) do
    << partition :: 32-signed, offset :: 64-signed, max_bytes :: 32-signed >>
  end

  def decode(data) when is_binary(data) do
    {topics, _} = Cafex.Protocol.decode_array(data, &decode_topic/1)
    %Response{topics: topics}
  end

  defp decode_topic(<< size :: 16-signed, topic :: size(size)-binary, rest :: binary >>) do
    {partitions, rest} = Cafex.Protocol.decode_array(rest, &decode_partition/1)
    {{topic, partitions}, rest}
  end

  defp decode_partition(<< partition :: 32-signed,
                           error_code :: 16-signed,
                           hwm_offset :: 64-signed,
                           message_set_size :: 32-signed,
                           message_set :: size(message_set_size)-binary,
                           rest :: binary >>) do
    messages = Cafex.Protocol.decode_message_set(message_set)
    {%{partition: partition,
       error: Cafex.Protocol.Errors.error(error_code),
       hwm_offset: hwm_offset,
       messages: messages}, rest}
  end
end
