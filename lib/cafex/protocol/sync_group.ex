defmodule Cafex.Protocol.SyncGroup do
  use Cafex.Protocol, api_key: 14

  defrequest do
    field :group_id, binary
    field :generation_id, integer
    field :member_id, binary
    field :group_assignment, [{member_id, member_assignment}]
    @type member_id :: binary
    @type member_assignment :: {version :: integer, [partition_assignment], user_data :: binary}
    @type partition_assignment :: {topic, [partition]}
    @type topic :: binary
    @type partition :: integer
  end

  defresponse do
    field :error, Cafex.Protocol.error
    field :member_assignment, Request.member_assignment
  end

  def encode(%{group_id: group_id,
              generation_id: generation_id,
              member_id: member_id,
              group_assignment: assignment}) do
    [encode_string(group_id),
     << generation_id :: 32-signed >>,
     encode_string(member_id),
     encode_array(assignment, &encode_member_assignment/1)]
    |> IO.iodata_to_binary
  end

  def decode(<< error_code :: 16-signed,
                assignment_size :: 32-signed,
                assignment :: size(assignment_size)-binary>>) do

    assignment = parse_assignment(assignment)
    %Response{error: decode_error(error_code),
              member_assignment: assignment}
  end

  defp encode_member_assignment({member_id, assignment}) do
    [encode_string(member_id), encode_assignment(assignment)]
  end

  defp encode_assignment({version, partition_assignment, user_data}) do
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

  defp parse_assignment(<< version :: 16-signed, rest :: binary >>) do
    {partition_assignment, rest} = decode_array(rest, &parse_topic/1)

    {user_data, _} = decode_bytes(rest)

    {version, partition_assignment, user_data}
  end

  defp parse_topic(<< len :: 16-signed,
                      topic :: size(len)-binary,
                      rest :: binary >>) do
    {partitions, rest} = decode_array(rest, &parse_partition/1)
    {{topic, partitions}, rest}
  end

  defp parse_partition(<< partition :: 32-signed, rest :: binary >>), do: {partition, rest}
end
