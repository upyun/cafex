defmodule Cafex.Consumer.Coordinator do
  use GenServer

  require Logger

  defmodule State do
    defstruct        conn: nil,
                     host: nil,
                     port: nil,
                topic_pid: nil,
               partitions: nil,
               group_name: nil,
               topic_name: nil,
           offset_storage: nil
  end

  alias Cafex.Connection
  alias Cafex.Protocol.OffsetCommit
  alias Cafex.Protocol.OffsetFetch
  alias Cafex.Topic.Server, as: Topic

  # ===================================================================
  # API
  # ===================================================================

  def start_link({host, port}, topic_pid, partitions, group_name, topic_name, offset_storage) do
    GenServer.start_link __MODULE__, [{host, port}, topic_pid, partitions, group_name, topic_name, offset_storage]
  end

  def stop(pid) do
    GenServer.call pid, :stop
  end

  def offset_commit(pid, partition, offset, metadata \\ "") do
    GenServer.call pid, {:offset_commit, partition, offset, metadata}
  end

  def offset_fetch(pid, partition) do
    GenServer.call pid, {:offset_fetch, partition}
  end

  # ===================================================================
  # GenServer callbacks
  # ===================================================================

  def init([{host, port}, topic_pid, partitions, group_name, topic_name, offset_storage]) do
    state = %State{     host: host,
                        port: port,
                   topic_pid: topic_pid,
                  partitions: partitions,
                  group_name: group_name,
                  topic_name: topic_name,
              offset_storage: offset_storage} |> start_conn
    {:ok, state}
  end

  def handle_call(:stop, _from, state) do
    {:stop, :normal, state}
  end

  def handle_call({:offset_commit, partition, _, _}, _from, %{partitions: partitions} = state) when partition >= partitions do
    {:reply, {:error, :unknown_partition}, state}
  end
  def handle_call({:offset_commit, partition, offset, metadata}, _from, %{group_name: group,
                                                                          topic_name: topic,
                                                                          offset_storage: storage,
                                                                          conn: conn} = state) do
    reply = offset_commit(storage, conn, group, topic, partition, offset, metadata)
    {:reply, reply, state}
  end

  def handle_call({:offset_fetch, partition}, _from, %{partitions: partitions} = state) when partition >= partitions do
    {:reply, {:error, :unknown_partition}, state}
  end
  def handle_call({:offset_fetch, partition}, _from, %{group_name: group,
                                                       topic_name: topic,
                                                       topic_pid: topic_pid,
                                                       offset_storage: storage,
                                                       conn: conn} = state) do
    case offset_fetch(storage, conn, group, topic, partition) do
      {:ok, {-1, _}} ->
        {:reply, get_offset(topic_pid, partition), state}
      {:ok, _} = reply ->
        {:reply, reply, state}
      {:error, :unknown_topic_or_partition} ->
        {:reply, get_offset(topic_pid, partition), state}
      error ->
        {:reply, error, state}
    end
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

  defp offset_commit(storage, conn, group, topic, partition, offset, metadata) do
    request = %OffsetCommit.Request{consumer_group: group,
                                    topics: [{topic, [{partition, offset, metadata}]}]}
    request = case storage do
      :zookeeper -> %{request | api_version: 0}
      :kafka     -> %{request | api_version: 1}
    end
    case Connection.request(conn, request, OffsetCommit) do
      {:ok, %{topics: [{^topic, [{^partition, :no_error}]}]}} ->
        :ok
      {:ok, %{topics: [{^topic, [{^partition, error}]}]}} ->
        {:error, error}
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp get_offset(topic_pid, partition) when is_pid(topic_pid) and is_integer(partition) do
    case Topic.offset(topic_pid, partition, :earliest, 1) do
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
end
