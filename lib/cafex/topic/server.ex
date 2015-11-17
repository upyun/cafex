defmodule Cafex.Topic.Server do
  @moduledoc false

  use GenServer

  require Logger

  defmodule State do
    @moduledoc false
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
  alias Cafex.Protocol.Fetch
  alias Cafex.Protocol.Offset
  alias Cafex.Protocol.Fetch.Request,    as: FetchRequest
  alias Cafex.Protocol.Offset.Request,   as: OffsetRequest
  alias Cafex.Protocol.Metadata.Request, as: MetadataRequest

  # ===================================================================
  # API
  # ===================================================================

  def start_link(name, brokers, opts \\ []) when is_binary(name) do
    GenServer.start_link __MODULE__, [name, brokers, opts]
  end

  def metadata(pid) do
    GenServer.call pid, :metadata
  end

  def fetch(pid, partition, offset) do
    GenServer.call pid, {:fetch, partition, offset}
  end

  def offset(pid, partition, time, max) do
    GenServer.call pid, {:offset, partition, time, max}
  end

  def stop(pid) do
    GenServer.call pid, :stop
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

  def handle_call({:fetch, partition, offset}, _from, state) do
    reply = fetch_messages(partition, offset, state)
    {:reply, reply, state}
  end

  def handle_call({:offset, partition, time, max}, _from, state) do
    reply = get_offset(partition, time, max, state)
    {:reply, reply, state}
  end

  def handle_call(:stop, _from, state) do
    {:stop, :normal, state}
  end

  def handle_info({:timeout, tref, :close_conn}, %{timer: tref} = state) do
    state = close_conn(%{state | timer: nil})
    {:noreply, state}
  end

  def handle_info({:EXIT, conn, reason}, %{conn: conn} = state) do
    Logger.error "Connection closed unexpectedly: #{inspect reason}"
    {:noreply, %{state | conn: nil}}
  end
  def handle_info({:EXIT, _pid, :normal}, state) do
    {:noreply, state}
  end
  def handle_info({:EXIT, _pid, reason}, state) do
    Logger.error "Connection closed unexpectedly: #{inspect reason}"
    {:noreply, state}
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
        open_conn(%{state | conn: nil, feed_brokers: rest,
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

    request = %MetadataRequest{ topics: [topic] }

    case Connection.request(conn_pid, request, Metadata) do
      {:ok, %{topics: [%{error: :unknown_topic_or_partition}]}} ->
        throw :unknown_topic_or_partition
      {:ok, %{topics: [%{error: :leader_not_available}]}} ->
        :timer.sleep(100)
        fetch_metadata(state)
      {:ok, metadata} ->
        metadata = extract_metadata(metadata)
        brokers  = metadata.brokers
                   |> Dict.values
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

  @wait_time 100
  @min_bytes 32 * 1024
  @max_bytes 1024 * 1024

  defp fetch_messages(partition, _offset, %{partitions: partitions}) when partition >= partitions do
    {:error, :unknown_partition}
  end
  defp fetch_messages(partition, offset, %{name: name, metadata: metadata}) do
    {host, port} = leader_for_partition(metadata, partition)
    request = %FetchRequest{max_wait_time: @wait_time,
                            min_bytes: @min_bytes,
                            topics: [{name, [{partition, offset, @max_bytes}]}]}
    {:ok, pid} = Connection.start_link(host, port)
    try do
      Connection.request(pid, request, Fetch)
    after
      Connection.close(pid)
    end
  end

  defp leader_for_partition(%{brokers: brokers, leaders: leaders}, partition) do
    leader = HashDict.get(leaders, partition)
    HashDict.get(brokers, leader)
  end

  defp get_offset(partition, _time, _max, %{partitions: partitions}) when partition >= partitions do
    {:error, :unknown_partition}
  end
  defp get_offset(partition, time, max, %{name: name, metadata: metadata}) do
    {host, port} = leader_for_partition(metadata, partition)
    request = %OffsetRequest{topics: [{name, [{partition, time, max}]}]}
    {:ok, pid} = Connection.start_link(host, port)
    try do
      Connection.request(pid, request, Offset)
    after
      Connection.close(pid)
    end
  end
end
