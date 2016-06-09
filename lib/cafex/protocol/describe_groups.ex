defmodule Cafex.Protocol.DescribeGroups do
  use Cafex.Protocol, api: :describe_groups

  defrequest do
    field :groups, [group_id]
    @type group_id :: binary
  end

  defresponse do
    field :groups, [group]

    @type group :: %{error: Cafex.Protocol.error,
                     group_id: binary,
                     state: binary,
                     protocol_type: binary,
                     protocol: binary,
                     members: [member]}
    @type member :: %{member_id: binary,
                      client_id: binary,
                      client_host: binary,
                      member_metadata: Cafex.Protocol.JoinGroup.Request.protocol_metadata,
                      member_assignment: Cafex.Protocol.SyncGroup.Request.member_assignment}
  end

  def encode(%{groups: groups}) do
    groups
    |> encode_array(&Cafex.Protocol.encode_string/1)
    |> IO.iodata_to_binary
  end

  def decode(data) when is_binary(data) do
    {groups, _} = decode_array(data, &parse_group/1)
    %Response{groups: groups}
  end

  defp parse_group(<< error_code :: 16-signed,
                      group_id_len :: 16-signed,
                      group_id :: size(group_id_len)-binary,
                      state_len :: 16-signed,
                      state :: size(state_len)-binary,
                      protocol_type_len :: 16-signed,
                      protocol_type :: size(protocol_type_len)-binary,
                      protocol_len :: 16-signed,
                      protocol :: size(protocol_len)-binary,
                      rest :: binary >>) do
    {members, rest} = decode_array(rest, &parse_member/1)
    {%{error: decode_error(error_code),
      group_id: group_id,
      state: state,
      protocol_type: protocol_type,
      protocol: protocol,
      members: members}, rest}
  end

  defp parse_member(<< member_id_len :: 16-signed,
                       member_id :: size(member_id_len)-binary,
                       client_id_len :: 16-signed,
                       client_id :: size(client_id_len)-binary,
                       client_host_len :: 16-signed,
                       client_host :: size(client_host_len)-binary,
                       member_metadata_len :: 32-signed,
                       member_metadata :: size(member_metadata_len)-binary,
                       member_assignment_len :: 32-signed,
                       member_assignment :: size(member_assignment_len)-binary,
                       rest :: binary>>) do
    member_metadata = parse_group_protocol_metadata(member_metadata)
    member_assignment = parse_assignment(member_assignment)
    {%{member_id: member_id,
      client_id: client_id,
      client_host: client_host,
      member_metadata: member_metadata,
      member_assignment: member_assignment}, rest}
  end

end
