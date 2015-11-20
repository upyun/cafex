defmodule Cafex.Protocol.RequestBehaviour do
  @moduledoc """
  The Kafka request specification.

  Since the Kafka protocol is designed to enable incremental evolution in a
  backward compatible fashion. Its versioning is on a per-api basis, each
  version consisting of a request and response pair. Each request contains
  an API key that identifies the API being invoked and a version number that
  indicates the format of the request and the expected format of the response.

  The Kafka server will always reply to a request except one case. If the
  produce request with a `0` required_acks, the server will not send any response.


  ## Protocols

  RequestBehaviour are required to implement the `Cafex.Protocol.Request` protocol.
  We use `Cafex.Protocol` to implement a API Request.
  """

  use Behaviour

  alias Cafex.Protocol.Request

  @doc """
  Returen the api_key of a request.
  """
  defcallback api_key(req :: Request.t) :: Cafex.Protocol.api_key

  @doc """
  Return the api_version the request will use.
  """
  defcallback api_version(req :: Request.t) :: Cafex.Protocol.api_version

  @doc """
  Return whether the api request has a response.

  All request expecte server reply except the produce request with a `0` required_acks.
  """
  defcallback has_response(req :: Request.t) :: boolean

  @doc """
  Encode the request data into binary.
  """
  defcallback encode(req :: Request.t) :: binary
end
