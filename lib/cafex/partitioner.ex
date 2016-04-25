defmodule Cafex.Partitioner do
  @moduledoc """
  Partitioner implementation specification for Kafka produce request API.

  ## Callbacks

    * `init(partitions)`

    * `partition(message, state)`
  """

  use Behaviour

  @type state :: term

  defcallback init(partitions :: integer) :: {:ok, state} | {:error, term}

  defcallback partition(message :: Cafex.Protocol.Message.t, state) :: {integer, state}
end
