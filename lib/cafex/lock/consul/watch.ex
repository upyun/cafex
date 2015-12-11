defmodule Cafex.Lock.Consul.Watch do
  @moduledoc false
  use GenServer

  @wait "5m"
  @retry_ms 30 * 1000

  defmodule State do
    @moduledoc false
    defstruct [:path, :index, :from]
  end

  def start_link(path, index, from) do
    GenServer.start_link __MODULE__, [path, index, from]
  end

  def init([path, index, from]) do
    {:ok, %State{path: path, index: index, from: from}, 0}
  end

  def handle_info(:timeout, state) do
    do_wait(state)
  end

  defp do_wait(%{path: path, index: index, from: from} = state) do
    # blocking query
    case Consul.Kv.fetch(path, wait: @wait, index: index) do
      {:ok, %{body: _body} = response} ->
        case Consul.Response.consul_index(response) do
          ^index ->
            {:noreply, state, 0}
          _ ->
            send from, :lock_changed
            {:stop, :normal, state}
        end
      {:error, %{reason: :timeout}} ->
        {:noreply, state, 0}
      {:error, _} ->
        {:noreply, state, @retry_ms}
    end
  end
end
