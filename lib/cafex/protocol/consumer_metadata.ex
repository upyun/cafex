defmodule Cafex.Protocol.ConsumerMetadata do
  @behaviour Cafex.Protocol.Decoder

  defmodule Request do
    defstruct consumer_group: nil

    @type t :: %Request{consumer_group: binary}
  end

  defmodule Response do
    defstruct coordinator_id: 0,
              coordinator_host: "",
              coordinator_port: 0,
              error: :no_error

    @type t :: %Response{ coordinator_id: integer,
                          coordinator_host: binary,
                          coordinator_port: 0..65535,
                          error: Cafex.Protocol.Errors.t }
  end

  defimpl Cafex.Protocol.Request, for: Request do
    def api_key(_), do: 10
    def api_version(_), do: 0
    def encode(request) do
      Cafex.Protocol.ConsumerMetadata.encode(request)
    end
  end

  def encode(%{consumer_group: consumer_group}) do
    Cafex.Protocol.encode_string(consumer_group)
  end

  def decode(<< error_code :: 16-signed,
                coordinator_id :: 32-signed,
                host_size :: 16-signed, coordinator_host :: size(host_size)-binary,
                coordinator_port :: 32-signed >>) do
    %Response{error: Cafex.Protocol.Errors.error(error_code),
              coordinator_id: coordinator_id,
              coordinator_host: coordinator_host,
              coordinator_port: coordinator_port}
  end
end
