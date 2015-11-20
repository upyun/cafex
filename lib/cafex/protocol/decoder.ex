defmodule Cafex.Protocol.Decoder do
  @moduledoc """
  Kafka server response decoder implementation specification.
  """

  use Behaviour

  alias Cafex.Protocol, as: P

  @typedoc """
  The `decode` function in each decoder will return there own response struct

  See `Cafex.Protocol`
  """
  @type response :: P.Metadata.Response.t |
                    P.Produce.Response.t |
                    P.Fetch.Response.t |
                    P.Offset.Response.t |
                    P.ConsumerMetadata.Response.t |
                    P.OffsetCommit.Response.t |
                    P.OffsetFetch.Response.t

  @typedoc """
  The modules which implement the `Decoder` interface

  See `Cafex.Protocol`
  """
  @type decoder  :: P.Metadata |
                    P.Produce |
                    P.Fetch |
                    P.Offset |
                    P.ConsumerMetadata |
                    P.OffsetCommit |
                    P.OffsetFetch

  @doc """
  Decode the response message in the Kafka server response
  """
  defcallback decode(binary) :: response

end
