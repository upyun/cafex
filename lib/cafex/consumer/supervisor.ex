defmodule Cafex.Consumer.Supervisor do
  @moduledoc """
  Manage consumers under the supervisor tree.
  """

  use Supervisor

  @doc false
  def start_link do
    Supervisor.start_link __MODULE__, [], name: __MODULE__
  end

  @doc """
  Start a consumer manager.

  Read `Cafex.Consumer.Manager` for more details.
  """
  @spec start_consumer(name :: atom, topic_name :: String.t, Cafex.Consumer.Manager.options) :: Supervisor.on_start_child
  def start_consumer(name, topic_name, opts) do
    Supervisor.start_child __MODULE__, [name, topic_name, opts]
  end

  @doc false
  def init([]) do
    children = [
      worker(Cafex.Consumer.Manager, [], restart: :transient,
                                        shutdown: 2000)
    ]
    supervise children, strategy: :simple_one_for_one,
                    max_restarts: 10,
                     max_seconds: 60
  end
end
