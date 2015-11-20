defmodule Cafex.Producer.Supervisor do
  @moduledoc """
  Manage producers under the supervisor tree.
  """

  use Supervisor

  @doc false
  def start_link do
    Supervisor.start_link __MODULE__, [], name: __MODULE__
  end

  @spec start_producer(topic :: String.t, Cafex.Producer.options) :: Supervisor.on_start_child
  def start_producer(topic, opts) do
    Supervisor.start_child __MODULE__, [topic, opts]
  end

  @doc false
  def init([]) do
    children = [
      worker(Cafex.Producer, [], restart: :permanent,
                                shutdown: 2000)
    ]
    supervise children, strategy: :simple_one_for_one,
                    max_restarts: 10,
                     max_seconds: 60
  end
end
