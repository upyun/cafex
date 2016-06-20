defmodule Cafex.Protocol.JoinGroup do
  use Cafex.Protocol, api: :join_group

  defrequest do
    field :group_id, binary
    field :session_timeout, integer
    field :member_id, binary
    field :protocol_type, binary
    field :group_protocols, [group_protocol]

    @type group_protocol :: {name :: binary, protocol_metadata}
    @type protocol_metadata :: {version :: integer, subscription :: [topic], user_data :: binary}
    @type topic :: binary
  end

  defresponse do
    field :error, Cafex.Protocol.error
    field :generation_id, binary
    field :group_protocol, binary
    field :leader_id, binary
    field :member_id, binary
    field :members, [member]

    @type member :: {id :: binary, metadata :: Request.protocol_metadata}
  end

  def encode(%{group_id: group_id,
               session_timeout: timeout,
               member_id: member_id,
               protocol_type: protocol_type,
               group_protocols: protocols}) do
    [encode_string(group_id),
     << timeout :: 32-signed >>,
     encode_string(member_id),
     encode_string(protocol_type),
     encode_array(protocols, &encode_group_protocol/1)]
    |> IO.iodata_to_binary
  end

  def decode(<< error_code :: 16-signed,
                generation_id :: 32-signed,
                group_protocol_len :: 16-signed,
                group_protocol :: size(group_protocol_len)-binary,
                leader_id_len :: 16-signed,
                leader_id :: size(leader_id_len)-binary,
                member_id_len :: 16-signed,
                member_id :: size(member_id_len)-binary,
                rest :: binary >>) do
    {members, _} = decode_array(rest, &parse_member/1)
    %Response{error: decode_error(error_code),
              generation_id: generation_id,
              group_protocol: group_protocol,
              leader_id: leader_id,
              member_id: member_id,
              members: members}
  end

  defp encode_group_protocol({name, metadata}) do
    [encode_string(name),
     encode_group_protocol_metadata(metadata)]
  end

  defp parse_member(<< member_id_len :: 16-signed,
                       member_id :: size(member_id_len)-binary,
                       metadata_len :: 32-signed,
                       metadata :: size(metadata_len)-binary,
                       rest :: binary>>) do
    metadata = parse_group_protocol_metadata(metadata)
    {{member_id, metadata}, rest}
  end
end
