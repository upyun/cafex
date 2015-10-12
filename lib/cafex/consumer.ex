defmodule Cafex.Consumer do
  use Behaviour

  defcallback init(args :: term) :: {:ok, term} | {:error, term}

  defcallback consume(Message.t, term) :: {:ok, term}

  defcallback terminate(term) :: :ok

  defmacro __using__(_) do
    quote do
      @behaviour Cafex.Consumer

      def init(_), do: {:ok, []}
      def consume(_msg, state), do: {:ok, state}
      def terminate(_state), do: :ok

      defoverridable [init: 1, consume: 2, terminate: 1]
    end
  end
end
