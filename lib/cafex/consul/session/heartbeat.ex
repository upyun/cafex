defmodule Cafex.Consul.Session.Heartbeat do
  @moduledoc false
  use GenServer

  defmodule State do
    @moduledoc false
    defstruct [:timer, :session, :interval]
  end

  def start_link(session, interval) do
    GenServer.start_link __MODULE__, [session, interval]
  end

  def stop(pid) do
    GenServer.call pid, :stop
  end

  def init([session, interval]) do
    {:ok, %State{session: session, interval: interval}, 0}
  end

  def handle_call(:stop, _from, state) do
    {:stop, :normal, :ok, state}
  end

  def handle_info(:timeout, state) do
    handle_info(:beat, state)
  end

  def handle_info(:beat, %{timer: timer, session: session, interval: interval} = state) do
    unless is_nil(timer) do
      :erlang.cancel_timer(timer)
    end

    case Consul.Session.renew session do
      {:ok, _ret} ->
        new_timer = :erlang.send_after(interval, self, :beat)
        {:noreply, %{state | timer: new_timer}}
      _ ->
        {:stop, :consul_error, state}
    end
  end
end
