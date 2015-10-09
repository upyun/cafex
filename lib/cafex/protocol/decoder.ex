defmodule Cafex.Protocol.Decoder do
  use Behaviour

  defcallback decode(binary) :: term
end
