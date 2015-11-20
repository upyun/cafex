defmodule Cafex.Partitioner do
  @moduledoc """
  Partitioner implementation specification for Kafka produce request API.

  ## Callbacks

    * `init(partitions)`

    * `partition(message, state)`
  """

  use Behaviour

  alias Cafex.Message

  @type state :: term

  defcallback init(partitions :: integer) :: {:ok, state} | {:error, term}

  defcallback partition(message :: Message.t, state) :: {integer, state}
end
