defmodule Cafex.Consumer.OffsetManager do
  use GenServer

  require Logger

  @max_buffers 50
  @interval 500
  @offset_storage :kafka

  defmodule State do
    @moduledoc false
    defstruct        conn: nil,  # coordinator connection
               group_coordinator: nil,
               partitions: nil,
               generation_id: nil,
               consumer_id: nil,
               group: nil,
               topic: nil,
             to_be_commit: %{},
                 interval: nil,
                    timer: nil,
                    count: 0,
              max_buffers: 0,
              auto_commit: true,
           offset_storage: nil
  end

  alias Cafex.Connection
  alias Cafex.Protocol.Offset.Request, as: OffsetRequest
  alias Cafex.Protocol.OffsetCommit
  alias Cafex.Protocol.OffsetFetch

  # ===================================================================
  # API
  # ===================================================================

  def start_link(group_coordinator, partitions, group, topic, opts \\ []) do
    GenServer.start_link __MODULE__, [group_coordinator, partitions, group, topic, opts]
  end

  def stop(pid) do
    GenServer.call pid, :stop
  end

  def update_generation_id(pid, consumer_id, generation_id) do
    GenServer.call pid, {:update_generation_id, consumer_id, generation_id}
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

  def init([group_coordinator, partitions, group, topic, opts]) do
    state = %State{group_coordinator: group_coordinator,
                   topic: topic,
                   group: group,
                   partitions: partitions,
                   auto_commit: Keyword.get(opts, :auto_commit, false),
                   interval: Keyword.get(opts, :interval) || @interval,
                   max_buffers: Keyword.get(opts, :max_buffers) || @max_buffers,
                   offset_storage: Keyword.get(opts, :offset_storage) || @offset_storage}
                 |> start_conn
    {:ok, state}
  end

  def handle_call(:stop, _from, state) do
    {:stop, :normal, :ok, state}
  end

  def handle_call({:update_generation_id, consumer_id, generation_id}, _from, state) do
    {:reply, :ok, %{state | consumer_id: consumer_id, generation_id: generation_id}}
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
  def handle_call({:offset_fetch, partition, leader_conn}, _from, %{group: group,
                                                                    topic: topic,
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

  def handle_info({:timeout, timer, :do_commit}, %{group: group,
                                                   topic: topic,
                                                   offset_storage: storage,
                                                   to_be_commit: to_be_commit,
                                                   consumer_id: consumer_id,
                                                   generation_id: generation_id,
                                                   timer: timer,
                                                   conn: conn} = state) do
    partitions = Enum.map(to_be_commit, fn {partition, {offset, metadata}} ->
      {partition, offset, metadata}
    end)
    offset_commit(storage, conn, group, topic, partitions, consumer_id, generation_id)
    {:noreply, %{state | to_be_commit: %{}, timer: nil, count: 0}}
  end

  def terminate(_reason, state) do
    close_conn(state)
    :ok
  end

  # ===================================================================
  # Internal functions
  # ===================================================================

  defp start_conn(%{group_coordinator: {host, port}} = state) do
    {:ok, pid} = Connection.start_link(host, port)
    %{state | conn: pid}
  end

  defp close_conn(%{conn: nil} = state), do: state
  defp close_conn(%{conn: pid} = state) do
    if Process.alive?(pid) do
      Connection.close(pid)
    end
    %{state | conn: nil}
  end

  defp offset_fetch(storage, conn, group, topic, partition) do
    request = %OffsetFetch.Request{consumer_group: group,
                                   topics: [{topic, [partition]}]}
    request = case storage do
      :zookeeper -> %{request | api_version: 0}
      :kafka     -> %{request | api_version: 1}
    end

    case Connection.request(conn, request) do
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
                                                                   generation_id: generation_id,
                                                                   consumer_id: consumer_id,
                                                                   conn: conn,
                                                                   group: group,
                                                                   topic: topic} = state) do
    reply = case offset_commit(storage, conn, group, topic, [{partition, offset, metadata}], consumer_id, generation_id) do
      {:ok, [{^partition, :no_error}]} -> :ok
      {:ok, [{^partition, error}]} -> {:error, error}
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

  defp offset_commit(storage, conn, group, topic, partitions, consumer_id, generation_id) do
    Logger.debug "Do offset commit: topic: #{inspect topic}, group: #{inspect group} partition offsets: #{inspect partitions},"
    request = %OffsetCommit.Request{consumer_group: group,
                                    consumer_id: consumer_id,
                                    consumer_group_generation_id: generation_id,
                                    topics: [{topic, partitions}]}
    request = case storage do
      :zookeeper -> %{request | api_version: 0}
      :kafka     -> %{request | api_version: 1}
    end

    # TODO
    # Handle every partition errors
    case Connection.request(conn, request) do
      {:ok, %{topics: [{^topic, partitions}]}} -> {:ok, partitions}
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp get_earliest_offset(topic, partition, conn) when is_integer(partition) do
    request = %OffsetRequest{topics: [{topic, [{partition, :earliest, 1}]}]}
    case Connection.request(conn, request) do
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