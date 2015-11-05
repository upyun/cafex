defmodule Cafex.Consumer.Supervisor do
  use Supervisor

  def start_link do
    Supervisor.start_link __MODULE__, [], name: __MODULE__
  end

  def start_consumer(name, topic_pid, opts) do
    Supervisor.start_child __MODULE__, [name, topic_pid, opts]
  end

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
