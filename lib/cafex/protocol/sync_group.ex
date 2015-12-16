defmodule Cafex.Protocol.SyncGroup do
  use Cafex.Protocol, api_key: 14
  import Cafex.Protocol.CodecGroups

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

  def encode_member_assignment({member_id, assignment}) do
    [encode_string(member_id), encode_assignment(assignment)]
  end
end
