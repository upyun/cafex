defmodule Cafex.Kafka.Heartbeat do
  use GenServer

  require Logger

  defmodule State do
    @moduledoc false
    defstruct pid: nil,
              conn: nil,
              request: nil,
              interval: nil
  end

  def start_link(pid, conn, request, interval) do
    GenServer.start_link __MODULE__, [pid, conn, request, interval]
  end

  def stop(pid), do: GenServer.call pid, :stop

  def init([pid, conn, request, interval]) do
    {:ok, %State{pid: pid,
                 conn: conn,
                 request: request,
                 interval: interval}, 0}
  end

  def handle_call(:stop, _from, state) do
    {:stop, :normal, :ok, state}
  end

  def handle_info(:timeout, %{pid: pid,
                              conn: conn,
                              request: request,
                              interval: interval} = state) do
    case beat(conn, request) do
      :ok ->
        {:noreply, state, interval}
      {:error, reason} ->
        Logger.warn "heartbeat error #{inspect reason}"
        send pid, {:no_heartbeat, reason}
        {:stop, :normal, state}
      error ->
        {:stop, error, state}
    end
  end

  defp beat(conn, request) do
    case Cafex.Connection.request(conn, request) do
      {:ok, %{error: :no_error}} ->
        :ok
      {:ok, %{error: error}} ->
        {:error, error}
      error ->
        error
    end
  end
end
