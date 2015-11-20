defmodule Cafex.Protocol.Decoder do
  @moduledoc """
  Kafka server response decoder implementation specification.
  """

  use Behaviour

  defcallback decode(binary) :: term
end
