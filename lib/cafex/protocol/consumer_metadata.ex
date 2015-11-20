defmodule Cafex.Protocol.ConsumerMetadata do
  @behaviour Cafex.Protocol.Decoder

  defmodule Request do
    use Cafex.Protocol

    @api_key 10

    defstruct consumer_group: nil

    @type t :: %Request{consumer_group: binary}

    def encode(request) do
      Cafex.Protocol.ConsumerMetadata.encode(request)
    end
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
