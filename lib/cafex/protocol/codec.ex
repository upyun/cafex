defmodule Cafex.Protocol.Codec do
  @moduledoc """
  Kafka protocol request encoder and server response decoder implementation specification.
  """

  alias Cafex.Protocol.Request
  alias Cafex.Protocol.Message
  alias Cafex.Protocol.Compression

  @decoders [ Metadata,
              Produce,
              Fetch,
              Offset,
              GroupCoordinator,
              OffsetCommit,
              OffsetFetch,
              JoinGroup,
              SyncGroup,
              Heartbeat,
              LeaveGroup,
              ListGroups,
              DescribeGroups]

  @typedoc """
  """
  @type request :: Request.t

  @typedoc """
  The `decode` function in each decoder will return there own response struct

  See `Cafex.Protocol`
  """
  @type response :: unquote(Enum.map(@decoders, fn d ->
    quote do: Cafex.Protocol.unquote(d).Response.t
  end) |> List.foldr([], fn
    v, []  -> quote do: unquote(v)
    v, acc -> quote do: unquote(v) | unquote(acc)
  end))

  @typedoc """
  The modules which implement the `Decoder` interface

  See `Cafex.Protocol`
  """
  @type decoder :: unquote(Enum.map(@decoders, fn d ->
    quote do: Cafex.Protocol.unquote(d)
  end) |> List.foldr([], fn
    v, []  -> quote do: unquote(v)
    v, acc -> quote do: unquote(v) | unquote(acc)
  end))

  @doc """
  Decode the response message in the Kafka server response
  """
  @callback decode(binary) :: response

  @doc """
  Encode the request
  """
  @callback encode(request) :: binary

  @doc """
  Returen the api_key of a request.
  """
  @callback api_key(request) :: Cafex.Protocol.api_key

  @doc """
  Return the api_version the request will use.
  """
  @callback api_version(request) :: Cafex.Protocol.api_version

  @doc """
  Return whether the api request has a response.

  All request expecte server reply except the produce request with a `0` required_acks.
  """
  @callback has_response?(request) :: boolean

  def encode_request(client_id, correlation_id, request) do
    api_key = Request.api_key(request)
    api_version = Request.api_version(request)
    payload = Request.encode(request)
    << api_key :: 16, api_version :: 16, correlation_id :: 32,
       byte_size(client_id) :: 16, client_id :: binary,
       payload :: binary >>
  end

  def decode_response(decoder, << correlation_id :: 32, rest :: binary >>) do
    {correlation_id, decoder.decode(rest)}
  end

  def decode_error(error_code), do: Cafex.Protocol.Errors.error(error_code)

  @doc """
  Encode bytes

  ## Examples

      iex> encode_bytes(nil)
      <<255, 255, 255, 255>>

      iex> encode_bytes("")
      <<0, 0, 0, 0>>

      iex> encode_bytes("hey")
      <<0, 0, 0, 3, 104, 101, 121>>
  """
  @spec encode_bytes(nil | binary) :: binary
  def encode_bytes(nil), do: << -1 :: 32-signed >>
  def encode_bytes(data) when is_binary(data) do
    case byte_size(data) do
      0 -> << 0 :: 32-signed >>
      size -> << size :: 32-signed, data :: binary >>
    end
  end

  def decode_bytes(<< -1 :: 32-signed, rest :: binary >>) do
    {nil, rest}
  end
  def decode_bytes(<< size :: 32-signed, bytes :: size(size)-binary, rest :: binary >>) do
    {bytes, rest}
  end

  @doc """
  Encode string

  ## Examples

      iex> encode_string(nil)
      <<255, 255>>

      iex> encode_string("")
      <<0, 0>>

      iex> encode_string("hey")
      <<0, 3, 104, 101, 121>>
  """
  @spec encode_string(nil | binary) :: binary
  def encode_string(nil), do: << -1 :: 16-signed >>
  def encode_string(data) when is_binary(data) do
    case byte_size(data) do
      0 -> << 0 :: 16-signed >>
      size -> << size :: 16-signed, data :: binary >>
    end
  end

  @doc """
  Encode kafka array

  ## Examples

      iex> encode_array([], nil)
      <<0, 0, 0, 0>>

      iex> encode_array([1, 2, 3], fn x -> <<x :: 32-signed>> end)
      [<<0, 0, 0, 3>>, [<<0, 0, 0, 1>>, <<0, 0, 0, 2>>, <<0, 0, 0, 3>>]]
  """
  def encode_array([], _), do: << 0 :: 32-signed >>
  def encode_array(array, item_encoder) when is_list(array) do
    [<< length(array) :: 32-signed >>, Enum.map(array, item_encoder)]
  end

  @doc """
  Decode kafka array

  ## Examples

      iex> decode_array(<<0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2>>, fn <<x :: 32, rest :: binary>> -> {x, rest} end)
      {[1, 2], <<>>}
  """
  def decode_array(<< num_items :: 32-signed, rest :: binary >>, item_decoder) do
    decode_array_items(num_items, rest, item_decoder, [])
  end

  defp decode_array_items(0, rest, _, acc), do: {Enum.reverse(acc), rest}
  defp decode_array_items(num_items, data, item_decoder, acc) do
    {item, rest} = item_decoder.(data)
    decode_array_items(num_items - 1, rest, item_decoder, [item|acc])
  end

  @doc """
  Encode single kafka message

  ## Examples

      iex> encode_message(%Cafex.Protocol.Message{value: "hey", timestamp: nil})
      <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 17, 254, 46, 107, 157, 0, 0, 255, 255, 255, 255, 0, 0, 0, 3, 104, 101, 121>>

      iex> encode_message(%Cafex.Protocol.Message{value: "hey", timestamp: 1, magic_byte: 1, timestamp_type: :create_time})
      <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 25, 180, 33, 39, 101, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 255, 255, 255, 255, 0, 0, 0, 3, 104, 101, 121>>

      iex> encode_message(%Cafex.Protocol.Message{value: "hey", key: ""})
      <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 17, 106, 86, 37, 142, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 104, 101, 121>>

      iex> encode_message(%Cafex.Protocol.Message{value: "hey", key: "key"})
      <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20, 156, 151, 255, 143, 0, 0, 0, 0, 0, 3, 107, 101, 121, 0, 0, 0, 3, 104, 101, 121>>

      iex> encode_message(%Cafex.Protocol.Message{value: "hey", key: "key", magic_byte: 1, timestamp_type: :create_time})
      <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 28, 82, 200, 27, 221, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 107, 101, 121, 0, 0, 0, 3, 104, 101, 121>>
  """
  @spec encode_message(Message.t) :: binary
  def encode_message(%Message{magic_byte: 0,
                              compression: compression_type,
                              offset: offset,
                              key: key,
                              value: value} = msg) do
    sub = << 0 :: 8, encode_attributes(msg) :: 8,
             encode_bytes(key) :: binary, encode_bytes(value) :: binary >>
    crc = :erlang.crc32(sub)
    msg = << crc :: 32, sub :: binary >>
    << offset :: 64-signed, byte_size(msg) :: 32-signed, msg :: binary >>
  end

  def encode_message(%Message{magic_byte: 1,
                              compression: compression_type,
                              offset: offset,
                              timestamp_type: ts_type,
                              timestamp: ts,
                              key: key,
                              value: value} = msg) do
    sub = << 1 :: 8, encode_attributes(msg) :: 8, ts :: 64-signed,
             encode_bytes(key) :: binary, encode_bytes(value) :: binary >>
    crc = :erlang.crc32(sub)
    msg = << crc :: 32, sub :: binary >>
    << offset :: 64-signed, byte_size(msg) :: 32-signed, msg :: binary >>
  end

  @doc """
  Decode message

  ## Examples

      iex> decode_message(<<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 17, 254, 46, 107, 157, 0, 0, 255, 255, 255, 255, 0, 0, 0, 3, 104, 101, 121>>)
      {%Cafex.Protocol.Message{value: "hey", magic_byte: 0}, <<>>}

      iex> decode_message(<<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 17, 254, 46, 107, 157, 0, 0, 255, 255, 255, 255, 0, 0, 0, 3, 104, 101, 121>>)
      {%Cafex.Protocol.Message{value: "hey", key: nil, magic_byte: 0}, <<>>}

      iex> decode_message(<<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20, 156, 151, 255, 143, 0, 0, 0, 0, 0, 3, 107, 101, 121, 0, 0, 0, 3, 104, 101, 121>>)
      {%Cafex.Protocol.Message{value: "hey", key: "key", magic_byte: 0}, <<>>}
  """
  def decode_message(<< offset :: 64-signed,
                        msg_size :: 32-signed,
                        msg :: size(msg_size)-binary,
                        rest :: binary >>) do
    { _crc, magic, attributes, timestamp_type, timestamp, compression, data} =
    case msg do
      << crc :: 32, 0 :: 8, attributes :: 8, data :: binary >> ->
        {_, compression} = decode_attributes(<< attributes :: 8>>)
        { crc, 0, attributes, nil, nil, compression, data}
      << crc :: 32, 1 :: 8, attributes :: 8, timestamp :: 64, data :: binary >> ->
        {timestamp_type, compression} = decode_attributes(<< attributes :: 8>>)
        { crc, 1, attributes, timestamp_type, timestamp, compression, data}
    end
    {key, data} = decode_bytes(data)
    {value,  _} = decode_bytes(data)
    msgs = %Message{key: key,
                   value: value,
                   magic_byte: magic,
                   attributes: attributes,
                   timestamp_type: timestamp_type,
                   compression: compression,
                   offset: offset} |> decode_compressed_messages
    {msgs, rest}
  end
  def decode_message(rest) do
    {nil, rest}
  end

  @doc """
  Encode MessageSet
  """
  @spec encode_message_set([Message.t]) :: binary
  def encode_message_set(messages) do
    Enum.map(messages, &encode_message/1) |> IO.iodata_to_binary
  end

  @doc """
  Decode MessageSet
  """
  @spec decode_message_set(binary) :: [Message.t]
  def decode_message_set(data) do
    decode_message_set_item(data, [])
  end

  @doc """
  Encode group protocol metadata

  ## Examples

      iex> encode_group_protocol_metadata({0, ["topic_name"], nil})
      <<0, 0, 0, 22, 0, 0, 0, 0, 0, 1, 0, 10, 116, 111, 112, 105, 99, 95, 110, 97, 109, 101, 255, 255, 255, 255>>

      iex> encode_group_protocol_metadata({0, ["topic_name"], ""})
      <<0, 0, 0, 22, 0, 0, 0, 0, 0, 1, 0, 10, 116, 111, 112, 105, 99, 95, 110, 97, 109, 101, 0, 0, 0, 0>>
  """
  def encode_group_protocol_metadata({version, subscription, user_data}) do
    data = [<< version :: 16-signed >>,
               encode_array(subscription, &encode_string/1),
               encode_bytes(user_data)]
           |> IO.iodata_to_binary
   len = byte_size(data)
   << len :: 32-signed, data :: binary>>
  end

  @doc """
  Parse group protocol metadata

  ## Examples

      iex> parse_group_protocol_metadata(<<0, 0, 0, 0, 0, 1, 0, 10, 116, 111, 112, 105, 99, 95, 110, 97, 109, 101, 255, 255, 255, 255>>)
      {0, ["topic_name"], nil}

      iex> parse_group_protocol_metadata(<<0, 0, 0, 0, 0, 1, 0, 10, 116, 111, 112, 105, 99, 95, 110, 97, 109, 101, 0, 0, 0, 0>>)
      {0, ["topic_name"], ""}
  """
  def parse_group_protocol_metadata(<< version :: 16-signed,
                               rest :: binary >>) do
    {subscription, rest} = decode_array(rest, &parse_topic_name/1)

    {user_data, _} = decode_bytes(rest)

    {version, subscription, user_data}
  end

  @doc """
  Encode assignment

  ## Examples

      iex> encode_assignment({0, [{"topic", [0, 1, 2]}], ""})
      <<0, 0, 0, 33, 0, 0, 0, 0, 0, 1, 0, 5, 116, 111, 112, 105, 99, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0>>

      iex> encode_assignment({0, [{"topic", [3, 4]}], nil})
      <<0, 0, 0, 29, 0, 0, 0, 0, 0, 1, 0, 5, 116, 111, 112, 105, 99, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 255, 255, 255, 255>>
  """
  def encode_assignment({version, partition_assignment, user_data}) do
    data =
    [<< version :: 16-signed >>,
     encode_array(partition_assignment, &encode_partition_assignment/1),
     encode_bytes(user_data)] |> IO.iodata_to_binary

   len = byte_size(data)

   << len :: 32-signed, data :: binary >>
  end

  @doc """
  Parse assignment

  ## Examples

      iex> parse_assignment(<<0, 0, 0, 0, 0, 1, 0, 5, 116, 111, 112, 105, 99, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0>>)
      {0, [{"topic", [0, 1, 2]}], ""}

      iex> parse_assignment(<<0, 0, 0, 0, 0, 1, 0, 5, 116, 111, 112, 105, 99, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 255, 255, 255, 255>>)
      {0, [{"topic", [3, 4]}], nil}
  """
  def parse_assignment(<< version :: 16-signed, rest :: binary >>) do
    {partition_assignment, rest} = decode_array(rest, &parse_topic_partitions/1)

    {user_data, _} = decode_bytes(rest)

    {version, partition_assignment, user_data}
  end

  defp parse_topic_partitions(<< len :: 16-signed,
                                 topic :: size(len)-binary,
                                 rest :: binary >>) do
    {partitions, rest} = decode_array(rest, &parse_partition/1)
    {{topic, partitions}, rest}
  end

  defp parse_partition(<< partition :: 32-signed, rest :: binary >>), do: {partition, rest}

  defp encode_partition_assignment({topic, partitions}) do
    [encode_string(topic), encode_array(partitions, &(<< &1 :: 32-signed >>))]
  end

  defp parse_topic_name(<< len :: 16-signed,
                           topic :: size(len)-binary,
                           rest :: binary >>) do
    {topic, rest}
  end

  defp decode_message_set_item(<<>>, acc), do: Enum.reverse(acc)
  defp decode_message_set_item(data, acc) do
    {msg, rest} = decode_message(data)
    case msg do
      nil ->
        decode_message_set_item(<<>>, acc)
      msgs when is_list(msgs) ->
        decode_message_set_item(rest, Enum.reverse(msgs) ++ acc)
      msg ->
        decode_message_set_item(rest, [msg|acc])
    end
  end

  defp decode_compressed_messages(%Message{attributes: attributes, value: value}=msg) do
    <<_ :: 4, _ :: 1, compression_type :: 3>> = <<attributes :: 8>>
    case compression_type do
      0 -> msg
      type ->
        Compression.decompress(value, decode_compression(type)) |> decode_message_set
    end
  end

  defp encode_attributes(%Message{magic_byte: 0, compression: compression_type}) do
    << attr :: 8 >> = << 0 :: 4, 0 :: 1, encode_compression(compression_type) :: 3 >>
    attr
  end
  defp encode_attributes(%Message{magic_byte: 1, compression: compress_type, timestamp_type: ts_type}) do
    << attr :: 8 >> = << 0 :: 4, encode_timestamp_type(ts_type) :: 1, encode_compression(compress_type) :: 3 >>
    attr
  end

  @timestamp_types %{:create_time => 0, :log_append_time => 1}
  for {type, code} <- @timestamp_types do
    defp encode_timestamp_type(unquote(type)), do: unquote(code)
    defp decode_timestamp_type(unquote(code)), do: unquote(type)
  end

  defp decode_attributes(<< _ :: 4, timestamp_type :: 1, compression :: 3 >>) do
    {decode_timestamp_type(timestamp_type), decode_compression(compression)}
  end

  @compressions %{nil => 0, :gzip => 1, :snappy => 2}
  for {type, code} <- @compressions do
    defp encode_compression(unquote(type)), do: unquote(code)
    defp decode_compression(unquote(code)), do: unquote(type)
  end
end
