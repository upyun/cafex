defmodule Cafex.Protocol.Heartbeat do
  use Cafex.Protocol, api_key: 12

  defrequest do
    field :group_id, binary
    field :generation_id, binary
    field :member_id, binary
  end

  defresponse do
    field :error, Cafex.Protocol.error
  end

  def encode(%{group_id: group_id,
               generation_id: generation_id,
               member_id: member_id}) do
    [encode_string(group_id),
      << generation_id :: 32-signed >>,
     encode_string(member_id)]
    |> IO.iodata_to_binary
  end

  def decode(<< error_code :: 16-signed >>) do
    %Response{error: decode_error(error_code)}
  end
end
