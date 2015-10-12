defmodule Cafex.Consumer.Manager do
  @moduledoc """
  This depends on ZooKeeper to rebalancing consumers

  ## zookeeper structure

  /cafex
   |-- topic
   |  |-- group_name
   |  |  |-- leader
   |  |  |-- cosnumers
   |  |  |  |-- cafex@192.168.0.1       - [0,1,2,3]
   |  |  |  |-- cafex@192.168.0.2       - [4,5,6,7]
   |  |  |-- locks

  """

  use GenServer

  require Logger

  defmodule State do
    defstruct group_name: nil,
              topic_pid: nil,
              zk_servers: nil,
              zk_path: nil,
              zk_pid: nil,
              zk_node: nil,
              handler: nil,
              topic_name: nil,
              brokers: nil,
              leaders: nil,
              partitions: nil,
              workers: HashDict.new,
              coordinator: nil,
              leader: {false, nil}
  end

  alias Cafex.Util
  alias Cafex.Connection
  alias Cafex.Protocol.ConsumerMetadata
  alias Cafex.Protocol.OffsetFetch
  alias Cafex.Protocol.OffsetCommit
  alias Cafex.Consumer.Worker

  # ===================================================================
  # API
  # ===================================================================

  def start_link(name, topic_pid, opts \\ []) do
    GenServer.start_link __MODULE__, [name, topic_pid, opts], name: name
  end

  def offset_commit(pid, partition, offset, metadata \\ "") do
    GenServer.call pid, {:offset_commit, partition, offset, metadata}
  end

  def offset_fetch(pid, partition) do
    GenServer.call pid, {:offset_fetch, partition}
  end

  # ===================================================================
  #  GenServer callbacks
  # ===================================================================

  def init([name, topic_pid, opts]) do
    Process.flag(:trap_exit, true)

    cfg        = Application.get_env(:cafex, name, [])
    handler    = Util.get_config(opts, cfg, :handler)
    zk_config  = Util.get_config(opts, cfg, :zookeeper, [])
    zk_servers = Keyword.get(zk_config, :servers, [{"127.0.0.1", 2181}])
                  |> Enum.map(fn {h, p} -> {:erlang.bitstring_to_list(h), p} end)
    zk_path    = Keyword.get(zk_config, :path, "/cafex")

    group_name = Atom.to_string(name)
    Logger.info fn -> "Starting consumer: #{group_name} ..." end

    state = %State{ group_name: group_name,
                    topic_pid: topic_pid,
                    handler: handler,
                    zk_servers: zk_servers,
                    zk_path: zk_path }
            |> load_metadata
            |> zk_connect
            |> zk_register
            |> leader_election
            |> load_balance
            |> start_offset_coordinator
            |> restart_workers

    {:ok, state}
  end

  def handle_call({:offset_commit, partition, _, _}, _from, %{partitions: partitions} = state) when partition >= partitions do
    {:reply, {:error, :unknown_partition}, state}
  end
  def handle_call({:offset_commit, partition, offset, metadata}, _from, %{group_name: group,
                                                                          topic_name: topic,
                                                                          coordinator: coordinator} = state) do
    reply = offset_commit(coordinator, group, topic, partition, offset, metadata)
    {:reply, reply, state}
  end

  def handle_call({:offset_fetch, partition}, _from, %{partitions: partitions} = state) when partition >= partitions do
    {:reply, {:error, :unknown_partition}, state}
  end
  def handle_call({:offset_fetch, partition}, _from, %{group_name: group,
                                                       topic_name: topic,
                                                       topic_pid: topic_pid,
                                                       coordinator: coordinator} = state) do
    case offset_fetch(coordinator, group, topic, partition) do
      {:ok, _} = reply ->
        {:reply, reply, state}
      {:error, :unknown_topic_or_partition} ->
        {:reply, get_offset(topic_pid, partition), state}
      error ->
        {:reply, error, state}
    end
  end

  def handle_info({:leader_election, seq}, %{leader: {_, seq}} = state) do
    state = state |> leader_election
                  |> load_balance
                  |> restart_workers
    {:noreply, state}
  end

  def handle_info({_, path, :node_data_changed}, state) do
    Logger.info fn -> "#{path} partition layout changed, restart_workers" end
    state = restart_workers(state)
    {:noreply, state}
  end
  def handle_info({_, _path, :node_children_changed}, %{group_name: group} = state) do
    Logger.info fn -> "#{group} consumers changed, rebalancing ..." end
    state = load_balance(state)
    {:noreply, state}
  end

  def handle_info({:EXIT, pid, reason}, state) do
    IO.puts "process exit #{inspect pid}, reason: #{inspect reason}, #{inspect state.workers}"
    {:noreply, state}
  end

  # TODO handle zk messages
  # TODO handle linked process EXIT
  # TODO handle worker lock timeout

  def terminate(_reason, state) do
    state |> leader_resign
          |> zk_unregister
          |> zk_close
          |> stop_workers
          |> stop_offset_coordinator
    :ok
  end


  # ===================================================================
  #  Internal functions
  # ===================================================================

  defp load_metadata(%{topic_pid: topic_pid} = state) do
    metadata = Cafex.Topic.Server.metadata topic_pid
    %{state | topic_name: metadata.name,
              brokers: metadata.brokers,
              leaders: metadata.leaders,
              partitions: metadata.partitions}
  end

  defp zk_connect(%{group_name: group_name,
                    topic_name: topic_name,
                    zk_servers: zk_servers,
                    zk_path: zk_path} = state) do
    {:ok, pid} = :erlzk.connect(zk_servers, 5000, [])
    path = Path.join [zk_path, topic_name, group_name]
    %{state | zk_pid: pid, zk_path: path}
  end

  defp zk_close(%{zk_pid: nil} = state), do: state
  defp zk_close(%{zk_pid: pid} = state) do
    :erlzk.close(pid)
    %{state | zk_pid: nil}
  end

  defp zk_register(%{zk_pid: pid, zk_path: zk_path} = state) do
    path = Path.join(zk_path, "consumers")
    name = Path.join(path, Atom.to_string(node))
    :ok = Cafex.ZK.Util.create_nodes(pid, path)
    case :erlzk.create(pid, name, :ephemeral) do
      {:ok, _} ->
        %{state | zk_node: name}
      {:error, reason} ->
        Logger.error "Failed to register consumer: #{name}, reason: #{inspect reason}"
        throw reason
    end
  end

  defp zk_unregister(%{zk_node: nil} = state), do: state
  defp zk_unregister(%{zk_node: zk_node, zk_pid: pid} = state) do
    :erlzk.delete(pid, zk_node)
    %{state | zk_node: nil}
  end

  defp leader_election(%{leader: {true, _}} = state), do: state
  defp leader_election(%{zk_pid: pid, zk_path: zk_path, leader: {false, nil}} = state) do
    path = Path.join(zk_path, "leader")
    %{state | leader: Cafex.ZK.Leader.election(pid, path)}
  end
  defp leader_election(%{zk_pid: pid, zk_path: zk_path, leader: {false, seq}} = state) do
    path = Path.join(zk_path, "leader")
    %{state | leader: Cafex.ZK.Leader.election(pid, path, seq)}
  end

  defp leader_resign(%{leader: {true, seq}, zk_pid: pid} = state) do
    :erlzk.delete(pid, seq)
    %{state | leader: {false, nil}}
  end
  defp leader_resign(state), do: state

  defp load_balance(%{leader: {false, _}} = state), do: state
  defp load_balance(%{zk_pid: pid, zk_path: zk_path, partitions: partitions} = state) do
    consumers  = zk_get_consumers(pid, zk_path)
    rebalanced = Cafex.Consumer.LoadBalancer.rebalance(consumers, partitions)
    for {k, v} <- rebalanced do
      case Keyword.get(consumers, k) do
        ^v -> :ok # not changed
        _  -> zk_set_consumer_partitions(pid, zk_path, k, v)
      end
    end
    state
  end

  defp zk_get_consumers(zk, path) do
    path = Path.join(path, "consumers")
    {:ok, children} = Cafex.ZK.Util.get_children_with_data(zk, path, self)
    children |> Enum.map(fn {x, v} -> {String.to_atom(x), decode_partitions(v)} end)
             |> Enum.sort
  end

  defp zk_set_consumer_partitions(zk, path, consumer, partitions) do
    path = Path.join [path, "consumers", Atom.to_string(consumer)]
    :erlzk.set_data zk, path, encode_partitions(partitions)
  end

  defp decode_partitions(nil), do: []
  defp decode_partitions(""), do: []
  defp decode_partitions(value) do
    value |> String.lstrip(?[)
          |> String.rstrip(?])
          |> String.split(",")
          |> Enum.map(&String.to_integer/1)
  end

  defp encode_partitions(partitions) do
    "[#{ partitions |> Enum.map(&Integer.to_string/1) |> Enum.join(",") }]"
  end

  defp restart_workers(%{zk_pid: zk, zk_node: zk_node, workers: workers} = state) do
    {:ok, {data, _stat}} = :erlzk.get_data(zk, zk_node, self)
    partitions = decode_partitions(data)
    should_stop = HashDict.keys(workers) -- partitions

    state =
    Enum.reduce should_stop, state, fn partition, acc ->
      stop_worker(partition, acc)
    end

    Enum.reduce partitions, state, fn partition, acc ->
      start_worker(partition, acc)
    end
  end

  defp start_worker(partition, %{workers: workers} = state) do
    case HashDict.get(workers, partition) do
      nil ->
        {:ok, pid} = do_start_worker(partition, state)
        %{state | workers: HashDict.put(workers, partition, pid)}
      pid ->
        case Process.alive?(pid) do
          false ->
            start_worker(partition, %{state | workers: HashDict.delete(workers, partition)})
          true ->
            state
        end
    end
  end

  defp stop_worker(partition, %{workers: workers} = state) do
    case HashDict.get(workers, partition) do
      nil ->
        state
      pid ->
        Worker.stop(pid)
        %{state | workers: HashDict.delete(workers, partition)}
    end
  end

  defp do_start_worker(partition, %{group_name: group,
                                    topic_name: topic,
                                    brokers: brokers,
                                    leaders: leaders,
                                    handler: handler,
                                    zk_pid: zk_pid,
                                    zk_path: zk_path}) do
    Logger.info fn -> "Starting consumer worker: #{topic}:#{group}:#{partition}" end
    leader = HashDict.get(leaders, partition)
    broker = HashDict.get(brokers, leader)
    Worker.start_link(self, handler, topic, group, partition, broker, zk_pid, zk_path)
  end

  defp stop_workers(%{workers: workers} = state) do
    Enum.each workers, fn {_, pid} ->
      Worker.stop pid
    end
    %{state | workers: HashDict.new}
  end

  defp get_offset(topic_pid, partition) when is_pid(topic_pid) and is_integer(partition) do
    case Cafex.Topic.Server.offset(topic_pid, partition, :earliest, 1) do
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

  defp offset_fetch(coordinator_pid, group, topic, partition) do
    request = %OffsetFetch.Request{consumer_group: group,
                                   topics: [{topic, [partition]}]}
    case Connection.request(coordinator_pid, request, OffsetFetch) do
      {:ok, %{topics: [{^topic, [{^partition, offset, metadata, :no_error}]}]}} ->
        {:ok, {offset, metadata}}
      {:ok, %{topics: [{^topic, [{^partition, _, _, error}]}]}} ->
        {:error, error}
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp offset_commit(coordinator_pid, group, topic, partition, offset, metadata) do
    request = %OffsetCommit.Request{consumer_group: group,
                                    topics: [{topic, [{partition, offset, metadata}]}]}
    case Connection.request(coordinator_pid, request, OffsetCommit) do
      {:ok, %{topics: [{^topic, [{^partition, :no_error}]}]}} ->
        :ok
      {:ok, %{topics: [{^topic, [{^partition, error}]}]}} ->
        {:error, error}
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp start_offset_coordinator(%{group_name: group, brokers: brokers} = state) do
    {:ok, {host, port}} = get_coordinator(group, HashDict.values(brokers))
    {:ok, pid} = Connection.start_link(host, port)
    %{state | coordinator: pid}
  end

  defp stop_offset_coordinator(%{coordinator: nil} = state), do: state
  defp stop_offset_coordinator(%{coordinator: pid} = state) do
    Connection.close(pid)
    %{state | coordinator: nil}
  end

  defp get_coordinator(group, brokers) do
    {:ok, pid} = broker_connect(brokers)
    request = %ConsumerMetadata.Request{consumer_group: group}
    reply = case Connection.request(pid, request, ConsumerMetadata) do
      {:ok, %{error: :no_error, coordinator_host: host, coordinator_port: port}} ->
        {:ok, {host, port}}
      {:ok, %{error: code}} ->
        {:error, code}
      {:error, reason} ->
        {:error, reason}
    end
    Connection.close(pid)
    reply
  end

  defp broker_connect(brokers), do: broker_connect(brokers, :no_broker)

  defp broker_connect([], reason), do: {:error, reason}
  defp broker_connect([{host, port}|rest], _) do
    case Connection.start(host, port) do
      {:ok, pid} ->
        {:ok, pid}
      {:error, reason} ->
        broker_connect(rest, reason)
    end
  end
end
