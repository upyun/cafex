defmodule Cafex.Topic.Server do
  @moduledoc false

  use GenServer

  require Logger

  defmodule State do
    defstruct name: nil,
              metadata: nil,
              feed_brokers: [],
              dead_brokers: [],
              conn: nil,
              client_id: nil,
              timer: nil
  end

  alias Cafex.Connection
  alias Cafex.Protocol.Metadata
  alias Cafex.Protocol.Metadata.Request

  # ===================================================================
  # API
  # ===================================================================

  def start_link(name, brokers, opts \\ []) when is_binary(name) do
    GenServer.start_link __MODULE__, [name, brokers, opts]
  end

  def metadata(pid) do
    GenServer.call pid, :metadata
  end

  # ===================================================================
  #  GenServer callbacks
  # ===================================================================

  def init([name, brokers, opts]) do
    Logger.info fn -> "Starting topic server: #{name}" end

    Process.flag(:trap_exit, true)

    client_id = Keyword.get(opts, :client_id, "cafex")

    :random.seed(:os.timestamp)

    state = %State{name: name,
                   feed_brokers: Enum.shuffle(brokers),
                   client_id: client_id} |> fetch_metadata

    {:ok, state}
  end

  def handle_call(:metadata, _from, state) do
    %{metadata: metadata} = state = fetch_metadata(state)
    {:reply, metadata, state}
  end

  def handle_info({:timeout, tref, :close_conn}, %{timer: tref} = state) do
    state = close_conn(%{state | timer: nil})
    {:noreply, state}
  end

  def handle_info({:EXIT, _pid, :normal}, state) do
    {:noreply, %{state | conn: nil}}
  end
  def handle_info({:EXIT, conn, reason}, %{conn: conn} = state) do
    Logger.error "Connection closed unexpectedly: #{inspect reason}"
    {:noreply, %{state | conn: nil}}
  end

  def terminate(_reason, state) do
    close_conn(state)
    :ok
  end

  # ===================================================================
  #  Internal functions
  # ===================================================================

  defp schedule_close_conn(%{timer: nil} = state) do
    tref = :erlang.start_timer(5000, self, :close_conn)
    %{state | timer: tref}
  end
  defp schedule_close_conn(%{timer: tref} = state) do
    :erlang.cancel_timer(tref)
    schedule_close_conn(%{state | timer: nil})
  end

  defp close_conn(%{conn: nil} = state), do: state
  defp close_conn(%{conn: conn, feed_brokers: [broker|_], timer: timer} = state) do
    Logger.debug fn -> "Closing connection to: #{inspect broker}" end
    Connection.close(conn)
    if timer, do: :erlang.cancel_timer(timer)
    %{state | conn: nil, timer: nil}
  end

  defp open_conn(%{conn: nil, feed_brokers: [], dead_brokers: deads} = state) do
    :timer.sleep(1000)
    open_conn(%{state | feed_brokers: deads,
                        dead_brokers: []})
  end
  defp open_conn(%{conn: nil, feed_brokers: [{host, port}|rest],
                   dead_brokers: deads, client_id: client_id} = state) do
    case Connection.start_link(host, port, client_id: client_id) do
      {:ok, pid} ->
        Logger.debug fn -> "Connection opened: #{host}:#{port}" end
        schedule_close_conn(%{state | conn: pid})
      {:error, reason} ->
        Logger.warn "Failed to open connection to broker: #{host}:#{port}, reason: #{inspect reason}"
        open_conn(%{state | feed_brokers: rest,
                            dead_brokers: [{host, port}|deads]})
    end
  end
  defp open_conn(%{conn: conn} = state) when is_pid(conn) do
    schedule_close_conn(state)
  end

  defp fetch_metadata(state) do
    state = %{name: topic,
              feed_brokers: [broker|rest],
              dead_brokers: deads,
              conn: conn_pid} = open_conn(state)

    request = %Request{ topics: [topic] }

    case Connection.request(conn_pid, request, Metadata) do
      {:ok, metadata} ->
        metadata = extract_metadata(metadata)
        brokers  = metadata.brokers
                   |> Map.values
                   |> Enum.shuffle
        %{state | metadata: metadata, feed_brokers: brokers}
      {:error, reason} ->
        Logger.error "Failed to get metadata from: #{inspect broker}, reason: #{inspect reason}"
        %{state | feed_brokers: rest, dead_brokers: [broker|deads]}
        |> close_conn
        |> fetch_metadata
    end
  end

  defp extract_metadata(metadata) do
    brokers = metadata.brokers |> Enum.map(fn b -> {b.node_id, {b.host, b.port}} end)
                               |> Enum.into(HashDict.new)

    [%{name: name, partitions: partitions}] = metadata.topics

    leaders = partitions |> Enum.map(fn p -> {p.partition_id, p.leader} end)
                         |> Enum.into(HashDict.new)

    %{name: name, brokers: brokers, leaders: leaders, partitions: length(partitions)}
  end
end
