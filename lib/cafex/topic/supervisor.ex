defmodule Cafex.Topic.Supervisor do
  use Supervisor

  def start_link do
    Supervisor.start_link __MODULE__, [], name: __MODULE__
  end

  def start_topic(name, brokers) when is_binary(name) do
    Supervisor.start_child __MODULE__, [name, brokers]
  end

  def init([]) do
    children = [
      worker(Cafex.Topic.Server, [], restart: :permanent,
                                    shutdown: 2000)
    ]
    supervise children, strategy: :simple_one_for_one,
                    max_restarts: 10,
                     max_seconds: 60
  end
end
