defmodule Cafex.Protocol.ConsumerMetadata do
  use Cafex.Protocol, api_key: 10

  defrequest do
    field :consumer_group, binary
  end

  defresponse do
    field :error, Cafex.Protocol.error
    field :coordinator_id, integer
    field :coordinator_host, binary
    field :coordinator_port, 0..65535
  end

  def encode(%{consumer_group: consumer_group}) do
    encode_string(consumer_group)
  end

  @spec decode(binary) :: Response.t
  def decode(<< error_code :: 16-signed,
                coordinator_id :: 32-signed,
                host_size :: 16-signed,
                coordinator_host :: size(host_size)-binary,
                coordinator_port :: 32-signed >>) do
    %Response{error: decode_error(error_code),
              coordinator_id: coordinator_id,
              coordinator_host: coordinator_host,
              coordinator_port: coordinator_port}
  end
end
