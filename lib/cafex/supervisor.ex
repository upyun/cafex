defmodule Cafex.Supervisor do
  @moduledoc false

  use Supervisor

  def start_link do
    Supervisor.start_link __MODULE__, nil, [name: __MODULE__]
  end

  def init(_) do
    children = [
      supervisor(Cafex.Topic.Supervisor, []),
      supervisor(Cafex.Producer.Supervisor, [])
    ]
    supervise children, strategy: :one_for_one,
                    max_restarts: 10,
                     max_seconds: 60
  end
end
