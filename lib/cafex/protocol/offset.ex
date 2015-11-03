defmodule Cafex.Protocol.Offset do
  @behaviour Cafex.Protocol.Decoder

  defmodule Request do
    defstruct replica_id: -1,
              topics: []

    @type t :: %Request{replica_id: integer,
                        topics: [{topic :: String.t,
                                  partitions :: [{partition :: integer,
                                                  time :: integer,
                                                  max_number_of_offsets :: integer}]}]}
  end

  defmodule Response do
    defstruct offsets: []

    @type t :: %Response{ offsets: [{topic :: String.t,
                                     partitions :: [{partition :: integer,
                                                     error :: Cafex.Protocol.Errors.t,
                                                     offsets :: [integer]}]}]}
  end


  defimpl Cafex.Protocol.Request, for: Request do
    def api_key(_), do: 2

    def encode(request) do
      Cafex.Protocol.Offset.encode(request)
    end
  end

  def encode(%Request{replica_id: replica_id, topics: topics}) do
    [<< replica_id :: 32-signed >>, Cafex.Protocol.encode_array(topics, &encode_topic/1)]
    |> IO.iodata_to_binary
  end

  defp encode_topic({topic, partitions}) do
    [<< byte_size(topic) :: 16-signed, topic :: binary >>,
     Cafex.Protocol.encode_array(partitions, &encode_partition/1)]
  end

  defp encode_partition({partition, time, max_number_of_offsets}) do
    << partition :: 32-signed, parse_time(time) :: 64-signed, max_number_of_offsets :: 32-signed >>
  end

  def decode(data) when is_binary(data) do
    {offsets, _rest} = Cafex.Protocol.decode_array(data, &parse_topic/1)
    %Response{offsets: offsets}
  end

  defp parse_topic(<< topic_len :: 16-signed, topic :: size(topic_len)-binary, rest :: binary >>) do
    {partitions, rest} = Cafex.Protocol.decode_array(rest, &parse_partition/1)
    {{topic, partitions}, rest}
  end

  defp parse_partition(<< partition :: 32-signed, error_code :: 16-signed, rest :: binary >>) do
    {offsets, rest} = Cafex.Protocol.decode_array(rest, &parse_offset/1)
    {%{partition: partition, error: Cafex.Protocol.Errors.error(error_code), offsets: offsets}, rest}
  end

  defp parse_offset(<< offset :: 64-signed, rest :: binary >>), do: {offset, rest}

  defp parse_time(:latest), do: -1

  defp parse_time(:earliest), do: -2

  @spec parse_time(:calendar.datetime) :: integer
  defp parse_time(time) do
    current_time_in_seconds = time |> :calendar.datetime_to_gregorian_seconds
    unix_epoch_in_seconds = {{1970,1,1},{0,0,0}} |> :calendar.datetime_to_gregorian_seconds
    (current_time_in_seconds - unix_epoch_in_seconds) * 1000
  end
end
