defmodule Cafex.Application do
  @moduledoc false

  use Application

  def start(_type, _args) do
    Cafex.Supervisor.start_link
  end

  def stop(_state) do
  end
end
