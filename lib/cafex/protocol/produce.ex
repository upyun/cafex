defmodule Cafex.Protocol.Produce do
  use Cafex.Protocol, api: :produce

  alias Cafex.Protocol.Message

  defrequest do
    field :required_acks, [default: 0], binary
    field :timeout, integer
    field :compression, [default: nil], Message.compression
    field :messages, [Message.t]
  end

  defresponse do
    field :topics, [topic]
    @type topic :: {topic :: String.t, [partition]}
    @type partition :: %{partition: integer,
                         error: Cafex.Protocol.error,
                         offset: integer}
  end
  def has_response?(%Request{required_acks: 0}), do: false
  def has_response?(%Request{required_acks: _}), do: true

  def encode(%Request{required_acks: required_acks,
                      timeout: timeout,
                      compression: compression_type,
                      messages: messages}) do
    message_bytes = encode_messages(messages, compression_type)

    << required_acks :: 16-signed, timeout :: 32-signed,
        message_bytes :: binary >>
  end

  defp encode_messages(messages, compression_type) do
    messages
    |> group_by_topic
    |> encode_array(fn {topic, partitions} ->
      [encode_string(topic),
       encode_array(partitions, fn {partition, messages} ->
         case compression_type do
           nil ->
             msg_bin = encode_message_set(messages)
             << partition :: 32-signed, byte_size(msg_bin) :: 32-signed, msg_bin :: binary >>
           type ->
             msg_bin = messages
               |> Enum.with_index
               |> Enum.map(fn {message, index} ->
                 %{message | offset: index}
               end)
               |> encode_message_set
               |> Cafex.Protocol.Compression.compress(type)
             bin = encode_message_set([%Message{topic: topic, partition: partition, value: msg_bin, compression: type}])
             << partition :: 32-signed, byte_size(bin) :: 32-signed, bin :: binary >>
         end
       end)]
    end)
    |> IO.iodata_to_binary
  end

  defp group_by_topic(messages) do
    messages |> Enum.group_by(fn %{topic: topic} -> topic end)
             |> Enum.map(fn {topic, msgs} -> {topic, group_by_partition(msgs)} end)
  end

  defp group_by_partition(messages) do
    messages |> Enum.group_by(fn %{partition: partition} -> partition end)
             |> Map.to_list
  end

  @spec decode(binary) :: Response.t
  def decode(data) when is_binary(data) do
    # TODO
    {response, _} = decode_array(data, &parse_response/1)
    %Response{topics: response}
  end

  defp parse_response(<< topic_size :: 16-signed, topic :: size(topic_size)-binary, rest :: binary >>) do
    {partitions, rest} = decode_array(rest, &parse_partition/1)
    {{topic, partitions}, rest}
  end

  defp parse_partition(<< partition :: 32-signed, error_code :: 16-signed, offset :: 64, rest :: binary >>) do
    {%{ partition: partition,
        error: decode_error(error_code),
        offset: offset}, rest}
  end
end
