defmodule Cafex.Protocol.Produce do
  @behaviour Cafex.Protocol.Decoder

  defmodule Request do
    defstruct required_acks: 0,
              timeout: 0,
              messages: []

    @type t :: %Request{ required_acks: binary,
                         timeout: integer,
                         messages: [Cafex.Protocol.Message.t] }
  end

  defimpl Cafex.Protocol.Request, for: Request do
    def api_key(_), do: 0

    def encode(request) do
      Cafex.Protocol.Produce.encode(request)
    end
  end

  def encode(%Request{required_acks: required_acks,
                      timeout: timeout,
                      messages: messages}) do
    message_bytes = encode_messages(messages)

    << required_acks :: 16-signed, timeout :: 32-signed,
        message_bytes :: binary >>
  end

  def encode_messages(messages) do
    messages
    |> group_by_topic
    |> Cafex.Protocol.encode_array(fn {topic, partitions} ->
      [Cafex.Protocol.encode_string(topic),
       Cafex.Protocol.encode_array(partitions, fn {partition, messages} ->
         msg_bin = Cafex.Protocol.encode_message_set(messages)
         << partition :: 32-signed, byte_size(msg_bin) :: 32-signed, msg_bin :: binary >>
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

  def decode(data) when is_binary(data) do
    {response, _} = Cafex.Protocol.decode_array(data, &parse_response/1)
    response
  end

  defp parse_response(<< topic_size :: 16-signed, topic :: size(topic_size)-binary, rest :: binary >>) do
    {partitions, rest} = Cafex.Protocol.decode_array(rest, &parse_partition/1)
    {{topic, partitions}, rest}
  end

  defp parse_partition(<< partition :: 32-signed, error_code :: 16-signed, offset :: 64, rest :: binary >>) do
    {%{ partition: partition,
        error_code: error_code,
        offset: offset}, rest}
  end
end
