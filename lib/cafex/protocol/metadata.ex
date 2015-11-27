defmodule Cafex.Protocol.Metadata do
  use Cafex.Protocol, api_key: 3

  defrequest do
    field :topics, [default: []], [binary]
  end

  defresponse do
    field :brokers, [broker]
    field :topics, [topic]

    @type broker :: %{node_id: integer,
                      host: binary,
                      port: 0..65535}
    @type topic :: %{error: Cafex.Protocol.error,
                     name: binary,
                     partitions: [partition]}
    @type partition :: %{error: Cafex.Protocol.error,
                         partition_id: integer,
                         leader: integer,
                         replicas: [integer],
                         isrs: [integer]}
  end

  def encode(%Request{topics: topics}) do
    topics
    |> encode_array(&Cafex.Protocol.encode_string/1)
    |> IO.iodata_to_binary
  end

  @spec decode(binary) :: Response.t
  def decode(data) when is_binary(data) do
    {brokers, rest} = decode_array(data, &parse_broker/1)
    {topics,     _} = decode_array(rest, &parse_topic/1)
    %Response{brokers: brokers, topics: topics}
  end

  defp parse_broker(<< node_id :: 32-signed, host_len :: 16-signed,
                       host :: size(host_len)-binary, port :: 32-signed,
                       rest :: binary >>) do
    {%{node_id: node_id, host: host, port: port}, rest}
  end

  defp parse_topic(<< error_code :: 16-signed, topic_len :: 16-signed,
                      topic :: size(topic_len)-binary, rest :: binary >>) do
    {partitions, rest} = decode_array(rest, &parse_partition/1)
    {%{error: decode_error(error_code), name: topic, partitions: partitions}, rest}
  end

  defp parse_partition(<< error_code :: 16-signed, partition_id :: 32-signed,
                          leader :: 32-signed, rest :: binary >>) do
    {replicas, rest} = decode_array(rest, &parse_int32/1)
    {isrs,     rest} = decode_array(rest, &parse_int32/1)
    {%{error: decode_error(error_code),
       partition_id: partition_id,
       leader: leader,
       replicas: replicas,
       isrs: isrs}, rest}
  end

  defp parse_int32(<< value :: 32-signed, rest :: binary >>), do: {value, rest}
end
