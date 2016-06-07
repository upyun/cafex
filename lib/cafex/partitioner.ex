defmodule Cafex.Partitioner do
  @moduledoc """
  Partitioner implementation specification for Kafka produce request API.

  ## Callbacks

    * `init(partitions)`

    * `partition(message, state)`
  """

  @type state :: term

  @callback init(partitions :: integer) :: {:ok, state} | {:error, term}

  @callback partition(message :: Cafex.Protocol.Message.t, state) :: {integer, state}
end
