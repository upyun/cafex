defmodule Cafex.Consumer.Coordinator do
  use GenServer

  require Logger

  @max_buffers 50
  @interval 500
  @offset_storage :kafka

  defmodule State do
    @moduledoc false
    defstruct        conn: nil,  # coordinator connection
                     host: nil,
                     port: nil,
               partitions: nil,
               group_name: nil,
               topic_name: nil,
             to_be_commit: %{},
                 interval: nil,
                    timer: nil,
                    count: 0,
              max_buffers: 0,
              auto_commit: true,
           offset_storage: nil
  end

  alias Cafex.Connection
  alias Cafex.Protocol.Offset
  alias Cafex.Protocol.Offset.Request, as: OffsetRequest
  alias Cafex.Protocol.OffsetCommit
  alias Cafex.Protocol.OffsetFetch

  # ===================================================================
  # API
  # ===================================================================

  def start_link({host, port}, partitions, group_name, topic_name, opts \\ []) do
    GenServer.start_link __MODULE__, [{host, port}, partitions, group_name, topic_name, opts]
  end

  def stop(pid) do
    GenServer.call pid, :stop
  end

  def offset_commit(pid, partition, offset, metadata \\ "") do
    GenServer.call pid, {:offset_commit, partition, offset, metadata}
  end

  def offset_fetch(pid, partition, leader_conn) do
    GenServer.call pid, {:offset_fetch, partition, leader_conn}
  end

  # ===================================================================
  # GenServer callbacks
  # ===================================================================

  def init([{host, port}, partitions, group_name, topic_name, opts]) do
    state = %State{     host: host,
                        port: port,
                  partitions: partitions,
                  group_name: group_name,
                  topic_name: topic_name,
                 auto_commit: Keyword.get(opts, :auto_commit, false),
                    interval: Keyword.get(opts, :interval, @interval),
                 max_buffers: Keyword.get(opts, :max_buffers, @max_buffers),
              offset_storage: Keyword.get(opts, :offset_storage) || @offset_storage} |> start_conn
    {:ok, state}
  end

  def handle_call(:stop, _from, state) do
    {:stop, :normal, state}
  end

  def handle_call({:offset_commit, partition, _, _}, _from, %{partitions: partitions} = state) when partition >= partitions do
    {:reply, {:error, :unknown_partition}, state}
  end
  def handle_call({:offset_commit, partition, offset, metadata}, from, state) do
    state = schedule_offset_commit(from, partition, offset, metadata, state)
    {:noreply, state}
  end

  def handle_call({:offset_fetch, partition, _leader_conn}, _from, %{partitions: partitions} = state) when partition >= partitions do
    {:reply, {:error, :unknown_partition}, state}
  end
  def handle_call({:offset_fetch, partition, leader_conn}, _from, %{group_name: group,
                                                       topic_name: topic,
                                                       offset_storage: storage,
                                                       conn: conn} = state) do
    case offset_fetch(storage, conn, group, topic, partition) do
      {:ok, {-1, _}} ->
        {:reply, get_earliest_offset(topic, partition, leader_conn), state}
      {:ok, _} = reply ->
        {:reply, reply, state}
      {:error, :unknown_topic_or_partition} ->
        {:reply, get_earliest_offset(topic, partition, leader_conn), state}
      error ->
        {:reply, error, state}
    end
  end

  def handle_info({:timeout, timer, :do_commit}, %{group_name: group,
                                         topic_name: topic,
                                         offset_storage: storage,
                                         to_be_commit: to_be_commit,
                                         timer: timer,
                                         conn: conn} = state) do
    partitions = Enum.map(to_be_commit, fn {partition, {offset, metadata}} ->
      {partition, offset, metadata}
    end)
    offset_commit(storage, conn, group, topic, partitions)
    {:noreply, %{state | to_be_commit: %{}, timer: nil, count: 0}}
  end

  # ===================================================================
  # Internal functions
  # ===================================================================

  defp start_conn(%{host: host, port: port} = state) do
    {:ok, pid} = Connection.start_link(host, port)
    %{state | conn: pid}
  end

  defp offset_fetch(storage, conn, group, topic, partition) do
    request = %OffsetFetch.Request{consumer_group: group,
                                   topics: [{topic, [partition]}]}
    request = case storage do
      :zookeeper -> %{request | api_version: 0}
      :kafka     -> %{request | api_version: 1}
    end

    case Connection.request(conn, request, OffsetFetch) do
      {:ok, %{topics: [{^topic, [{^partition, offset, metadata, :no_error}]}]}} ->
        {:ok, {offset, metadata}}
      {:ok, %{topics: [{^topic, [{^partition, _, _, error}]}]}} ->
        {:error, error}
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp schedule_offset_commit(from, partition, offset, metadata, %{auto_commit: false,
                                                             offset_storage: storage,
                                                             conn: conn,
                                                             group_name: group,
                                                             topic_name: topic} = state) do
    reply = case offset_commit(storage, conn, group, topic, [{partition, offset, metadata}]) do
      {:ok, %{topics: [{^topic, [{^partition, :no_error}]}]}} -> :ok
      {:ok, %{topics: [{^topic, [{^partition, error}]}]}} -> {:error, error}
      {:error, _reason} = error -> error
    end

    GenServer.reply(from, reply)

    state
  end
  defp schedule_offset_commit(from, partition, offset, metadata, %{to_be_commit: to_be_commit,
                                                             timer: timer,
                                                             interval: interval,
                                                             count: count,
                                                             max_buffers: max_buffers} = state) do
    to_be_commit = Map.put(to_be_commit, partition, {offset, metadata})

    state = if count + 1 >= max_buffers do
      cancel_timer(timer)
      send self, {:timeout, nil, :do_commit}
      %{state | timer: nil, to_be_commit: to_be_commit, count: count + 1}
    else
      timer = start_timer(timer, interval, self, :do_commit)
      %{state | timer: timer, to_be_commit: to_be_commit, count: count + 1}
    end

    GenServer.reply(from, :ok)

    state
  end

  defp offset_commit(storage, conn, group, topic, partitions) do
    Logger.debug "Do offset commit: topic: #{inspect topic}, group: #{inspect group} partition offsets: #{inspect partitions},"
    request = %OffsetCommit.Request{consumer_group: group,
                                    topics: [{topic, partitions}]}
    request = case storage do
      :zookeeper -> %{request | api_version: 0}
      :kafka     -> %{request | api_version: 1}
    end

    # TODO
    # Handle every partition errors
    case Connection.request(conn, request, OffsetCommit) do
      {:ok, %{topics: [{^topic, partitions}]}} -> {:ok, partitions}
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp get_earliest_offset(topic, partition, conn) when is_integer(partition) do
    request = %OffsetRequest{topics: [{topic, [{partition, :earliest, 1}]}]}
    case Connection.request(conn, request, Offset) do
      {:ok, %{offsets: [{_, [%{error: :no_error, offsets: [offset]}]}]}} ->
        {:ok, {offset, ""}}
      {:ok, %{offsets: [{_, [%{error: :no_error, offsets: []}]}]}} ->
        {:ok, {0, ""}}
      {:ok, %{offsets: [{_, [%{error: error}]}]}} ->
        {:error, error}
      error ->
        error
    end
  end

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(ref), do: :erlang.cancel_timer(ref)

  defp start_timer(nil, time, dest, msg), do: :erlang.start_timer(time, dest, msg)
  defp start_timer(timer, _, _, _), do: timer
end
