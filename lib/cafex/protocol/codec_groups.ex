defmodule Cafex.Protocol.CodecGroups do
  @moduledoc """
  Kafka consumer groups protocol encode/decode functions

  Our implementation follows the format defined in kafka official proposal.
  See [GroupMembershipAPI](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-GroupMembershipAPI) to read more.
  """
  import Cafex.Protocol.Codec

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

  defp parse_topic_name(<< len :: 16-signed,
                           topic :: size(len)-binary,
                           rest :: binary >>) do
    {topic, rest}
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

  defp encode_partition_assignment({topic, partitions}) do
    [encode_string(topic), encode_array(partitions, &(<< &1 :: 32-signed >>))]
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
end
