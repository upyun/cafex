defmodule Cafex.Partitioner do
  use Behaviour

  alias Cafex.Message

  @type state :: term

  defcallback init(partitions :: integer) :: {:ok, state} | {:error, term}

  defcallback partition(Message.t, state) :: {integer, state}
end
