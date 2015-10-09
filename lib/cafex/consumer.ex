defmodule Cafex.Consumer do
  use Behaviour

  defcallback consume(Message.t) :: :ok

  defmacro __using__(_) do
    quote do
      @behaviour Cafex.Consumer
    end
  end
end
