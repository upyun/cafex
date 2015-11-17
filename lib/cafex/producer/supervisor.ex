defmodule Cafex.Producer.Supervisor do
  use Supervisor

  def start_link do
    Supervisor.start_link __MODULE__, [], name: __MODULE__
  end

  def start_producer(topic, brokers, opts) do
    Supervisor.start_child __MODULE__, [topic, brokers, opts]
  end

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
