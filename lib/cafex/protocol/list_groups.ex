defmodule Cafex.Protocol.ListGroups do
  use Cafex.Protocol, api: :list_groups

  defresponse do
    field :groups, [group]
    field :error, binary

    @type group :: {group_id :: binary,
                    protocol_type :: binary}
  end

  def decode(<< error_code :: 16-signed, rest :: binary >>) do
    {groups, _} = decode_array(rest, &parse_group/1)
    %Response{error: decode_error(error_code), groups: groups}
  end

  defp parse_group(<< group_id_len:: 16-signed,
                      group_id :: size(group_id_len)-binary,
                      protocol_type_len :: 16-signed,
                      protocol_type :: size(protocol_type_len)-binary,
                      rest :: binary>>) do
    {{group_id, protocol_type}, rest}
  end
end
