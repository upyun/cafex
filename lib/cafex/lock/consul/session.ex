defmodule Cafex.Lock.Consul.Session do
  use GenServer

  alias Consul.Session
  alias Cafex.Lock.Consul.Heartbeat

  @default_behavior :release
  @default_ttl 10 * 1000

  defmodule State do
    @moduledoc false
    defstruct [:id, :heartbeat]
  end

  def start_link(opts \\ []) do
    GenServer.start_link __MODULE__, [opts]
  end

  def get(pid) do
    GenServer.call pid, :get
  end

  def stop(pid) do
    GenServer.call pid, :stop
  end

  def init([opts]) do
    behavior = Keyword.get(opts, :behavior, @default_behavior) |> to_string
    lock_delay_ms = Keyword.get opts, :lock_delay, 0
    ttl_ms = Keyword.get opts, :ttl, @default_ttl
    lock_delay = (lock_delay_ms |> div(1000) |> Integer.to_string) <> "s"
    ttl = (ttl_ms |> div(1000) |> Integer.to_string) <> "s"

    id =
    %{:Name      => Keyword.get(opts, :name),
      :Node      => Keyword.get(opts, :node_name),
      :LockDelay => lock_delay,
      :TTL       => ttl,
      :Behavior  => behavior}
    |> Enum.filter(fn {_k, v} -> v end)
    |> Enum.into(%{})
    |> Session.create!

    {:ok, pid} = Heartbeat.start_link(id, div(ttl_ms, 3) * 2)
    state = %State{id: id, heartbeat: pid}
    {:ok, state}
  end

  def handle_call(:get, _from, %{id: id} = state) do
    {:reply, id, state}
  end

  def handle_call(:stop, _from, %{id: id} = state) do
    {:stop, :normal, :ok, state}
  end

  def terminate(_reason, %{id: id, heartbeat: pid} = _state) do
    Heartbeat.stop pid
    do_destroy(id)
    :ok
  end

  defp do_destroy(id) do
    Session.destroy id
  end
end
