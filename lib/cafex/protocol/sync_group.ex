defmodule Cafex.Protocol.SyncGroup do
  use Cafex.Protocol, api_key: 14

  defrequest do
    field :group_id, binary
    field :generation_id, integer
    field :member_id, binary
    field :group_assignment, [member_assignment]
    @type member_assignment :: {member_id :: binary,
                                assignment :: binary}
  end

  defresponse do
    field :error, Cafex.Protocol.error
    field :member_assignment, binary
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
    %Response{error: decode_error(error_code),
              member_assignment: assignment}
  end

  defp encode_member_assignment({member_id, assignment}) do
    [encode_string(member_id),
     encode_bytes(assignment)]
  end
end
