defmodule Cafex.Protocol.Metadata do
  @behaviour Cafex.Protocol.Decoder

  defmodule Request do
    defstruct topics: []

    @type t :: %Request{topics: [binary]}
  end

  defmodule Response do
    defstruct brokers: [], topics: []
  end

  defimpl Cafex.Protocol.Request, for: Request do
    def api_key(_), do: 3

    def encode(%Request{topics: topics}) do
      topics
      |> Cafex.Protocol.encode_array(&Cafex.Protocol.encode_string/1)
      |> IO.iodata_to_binary
    end
  end

  def decode(data) when is_binary(data) do
    {brokers, rest} = Cafex.Protocol.decode_array(data, &parse_broker/1)
    {topics,     _} = Cafex.Protocol.decode_array(rest, &parse_topic/1)
    %Response{brokers: brokers, topics: topics}
  end

  defp parse_broker(<< node_id :: 32-signed, host_len :: 16-signed,
                       host :: size(host_len)-binary, port :: 32-signed,
                       rest :: binary >>) do
    {%{node_id: node_id, host: host, port: port}, rest}
  end

  defp parse_topic(<< error_code :: 16-signed, topic_len :: 16-signed,
                      topic :: size(topic_len)-binary, rest :: binary >>) do
    {partitions, rest} = Cafex.Protocol.decode_array(rest, &parse_partition/1)
    {%{error_code: error_code, name: topic, partitions: partitions}, rest}
  end

  defp parse_partition(<< error_code :: 16-signed, partition_id :: 32-signed,
                          leader :: 32-signed, rest :: binary >>) do
    {replicas, rest} = Cafex.Protocol.decode_array(rest, &parse_int32/1)
    {isrs,     rest} = Cafex.Protocol.decode_array(rest, &parse_int32/1)
    {%{error_code: error_code,
       partition_id: partition_id,
       leader: leader,
       replicas: replicas,
       isrs: isrs}, rest}
  end

  defp parse_int32(<< value :: 32-signed, rest :: binary >>), do: {value, rest}
end
