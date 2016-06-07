defmodule Cafex.Consumer do
  @moduledoc """
  Consumer worker implementation specification.

  ## Callbacks

    * `init(args)`

    * `consume(message, state)`

    * `terminate(state)`
  """

  @type state :: term
  @type done :: :ok | :nocommit

  @callback init(args :: term) :: {:ok, state} | {:error, reason :: term}

  @callback consume(message :: Cafex.Protocol.Message.t, state) :: {done, state}

  @callback terminate(state) :: :ok

  @doc false
  defmacro __using__(_) do
    quote do
      @behaviour unquote(__MODULE__)

      def init(args), do: {:ok, args}
      def consume(_msg, state), do: {:ok, state}
      def terminate(_state), do: :ok

      defoverridable [init: 1, consume: 2, terminate: 1]
    end
  end
end
