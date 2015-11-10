defmodule Cafex.Consumer.Manager do
  @moduledoc """
  This depends on ZooKeeper to rebalancing consumers

  ## zookeeper structure


  ```
    /cafex
     |-- topic
     |  |-- group_name
     |  |  |-- leader
     |  |  |-- consumers
     |  |  |  |-- balance
     |  |  |  |  |-- cafex@192.168.0.1       - [0,1,2,3]     # persistent
     |  |  |  |  |-- cafex@192.168.0.2       - [4,5,6,7]     # persistent
     |  |  |  |  |-- cafex@192.168.0.3       - [8,9,10,11]   # persistent
     |  |  |  |-- online
     |  |  |  |  |-- cafex@192.168.0.1                       # ephemeral
     |  |  |  |  |-- cafex@192.168.0.2                       # ephemeral
     |  |  |  |-- offline
     |  |  |  |  |-- cafex@192.168.0.3                       # persistent
     |  |  |-- locks
  ```
  """

  use GenServer

  require Logger

  @zk_timeout 5000

  defmodule State do
    @moduledoc false
    defstruct group_name: nil,
              topic_pid: nil,
              zk_servers: nil,
              zk_path: nil,
              zk_timeout: nil,
              zk_online_path: nil,
              zk_offline_path: nil,
              zk_balance_path: nil,
              zk_pid: nil,
              zk_online_node: nil,
              zk_balance_node: nil,
              handler: nil,
              topic_name: nil,
              brokers: nil,
              leaders: nil,
              partitions: nil,
              worker_cfg: nil,
              workers: HashDict.new,
              r_workers: HashDict.new,
              trefs: HashDict.new,
              offset_storage: :kafka,
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

  def offline(pid) do
    GenServer.cast pid, :offline
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
    worker_cfg = Util.get_config(opts, cfg, :worker)
    zk_config  = Util.get_config(opts, cfg, :zookeeper, [])
    zk_servers = Keyword.get(zk_config, :servers, [{"127.0.0.1", 2181}])
                  |> Enum.map(fn {h, p} -> {:erlang.bitstring_to_list(h), p} end)
    zk_path    = Keyword.get(zk_config, :path, "/cafex")
    zk_timeout = Keyword.get(zk_config, :timeout, @zk_timeout)

    group_name = Atom.to_string(name)
    Logger.info fn -> "Starting consumer: #{group_name} ..." end

    state = %State{ group_name: group_name,
                    topic_pid: topic_pid,
                    handler: handler,
                    worker_cfg: worker_cfg,
                    zk_servers: zk_servers,
                    zk_timeout: zk_timeout,
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
                                                                          offset_storage: storage,
                                                                          coordinator: coordinator} = state) do
    reply = offset_commit(storage, coordinator, group, topic, partition, offset, metadata)
    {:reply, reply, state}
  end

  def handle_call({:offset_fetch, partition}, _from, %{partitions: partitions} = state) when partition >= partitions do
    {:reply, {:error, :unknown_partition}, state}
  end
  def handle_call({:offset_fetch, partition}, _from, %{group_name: group,
                                                       topic_name: topic,
                                                       topic_pid: topic_pid,
                                                       offset_storage: storage,
                                                       coordinator: coordinator} = state) do
    case offset_fetch(storage, coordinator, group, topic, partition) do
      {:ok, _} = reply ->
        {:reply, reply, state}
      {:error, :unknown_topic_or_partition} ->
        {:reply, get_offset(topic_pid, partition), state}
      error ->
        {:reply, error, state}
    end
  end

  def handle_cast(:offline, %{zk_pid: pid, zk_offline_path: offline_path} = state) do
    node_name = Atom.to_string(node)
    name = Path.join(offline_path, node_name)
    case :erlzk.create(pid, name) do
      {:ok, _} ->
        Logger.info "I am going to offline"
        {:stop, :normal, state}
      _ ->
        Logger.warn "Failed to offline"
        # TODO
        {:noreply, state}
    end
  end

  def handle_info({:leader_election, seq}, %{leader: {_, seq}} = state) do
    Logger.warn "leader_election"
    state = state |> leader_election
                  |> load_balance
    {:noreply, state}
  end

  def handle_info({:timeout, _tref, {:restart_worker, partition}}, %{trefs: trefs} = state) do
    state = %{state | trefs: HashDict.delete(trefs, partition)}
    state = start_worker(partition, state)
    {:noreply, state}
  end

  # handle zk messages
  # handle erlzk connection changes, as erlzk monitor
  def handle_info({:disconnected, _host, _port}, state) do
    Logger.warn "erlzk disconnected #{inspect _host}:#{inspect _port}"
    # TODO
    # erlzk disconnected from zk server, every erlzk command will failed until reconnected
    {:noreply, state}
  end
  def handle_info({:connected, _host, _port}, state) do
    Logger.warn "erlzk connected #{inspect _host}:#{inspect _port}"
    # TODO
    # Event maybe missed on zk server, need to check again for every watched znode
    {:noreply, state}
  end
  def handle_info({:expired, _host, _port}, state) do
    Logger.warn "erlzk expired #{inspect _host}:#{inspect _port}"
    # All ephemeral znodes were deleted by zk server, and it caused leader to execute rebalance.
    # So this consumer must stop, and restart by the supervisor.
    {:stop, :zk_session_expired, state}
  end

  # handle zk watcher events
  def handle_info({:node_children_changed, path}, %{zk_online_path: path} = state) do
    # Online nodes changed
    Logger.info fn -> "#{state.group_name} consumers changed, rebalancing ..." end
    state = load_balance(state)
    {:noreply, state}
  end
  def handle_info({:node_created, path}, %{zk_balance_node: path} = state) do
    # balance_node created, start workers
    Logger.info fn -> "#{path} partition node created, restart_workers" end
    state = restart_workers(state)
    {:noreply, state}
  end
  def handle_info({:node_data_changed, path}, %{zk_balance_node: path} = state) do
    # balance_node created, start workers
    Logger.info fn -> "#{path} partition layout changed, restart_workers" end
    state = restart_workers(state)
    {:noreply, state}
  end
  def handle_info({:node_deleted, path}, %{zk_online_node: path} = state) do
    # Should not happend here.
    # If it was deleted, the zookeeper session must be expired, but the watch event won't received
    Logger.warn fn -> "#{path} node deleted" end
    state = %{state|zk_online_node: nil} |> zk_register
    {:noreply, state}
  end
  def handle_info({:node_deleted, path}, %{zk_balance_node: path} = state) do
    # The zk_balance_node is not an ephemeral node, the only reason to be deleted
    # is the zk session of this node was expired, then the online node was deleted
    # by zk server, and the balance_node was deleted by leader after rebalance.
    #
    # Stop consumer itself and restart by supervisor can solve this.
    {:stop, :node_deleted, state}
  end

  # handle linked process EXIT
  def handle_info({:EXIT, pid, reason}, %{zk_pid: pid} = state) do
    Logger.error "ZooKeeper connection exit with reason: #{inspect reason}, stop cusumer."
    {:stop, reason, %{state|zk_pid: nil}}
  end
  def handle_info({:EXIT, pid, reason}, %{coordinator: pid} = state) do
    Logger.warn fn -> "Coordinator exit with the reason #{inspect reason}" end
    state = start_offset_coordinator(state)
    {:noreply, state}
  end
  def handle_info({:EXIT, pid, :not_leader_for_partition}, state) do
    Logger.warn fn -> "Worker stopped due to not_leader_for_partition, reload leader and restart it" end
    state = load_metadata(state)
    state = try_restart_worker(pid, state)
    {:noreply, state}
  end
  def handle_info({:EXIT, pid, :lock_timeout}, %{r_workers: r_workers, trefs: trefs} = state) do
    # worker lock_timeout, wait for sometimes and then restart it
    state = case HashDict.get(r_workers, pid) do
      nil -> state
      partition ->
        tref = :erlang.start_timer(5000, self, {:restart_worker, partition})
        %{state | trefs: HashDict.put(trefs, partition, tref)}
    end
    {:noreply, state}
  end
  def handle_info({:EXIT, _pid, :normal}, state) do
    {:noreply, state}
  end
  def handle_info({:EXIT, pid, reason}, %{r_workers: r_workers, trefs: trefs} = state) do
    state = case HashDict.get(r_workers, pid) do
      nil -> state
      partition ->
        Logger.info fn -> "Worker #{inspect pid} for partition #{inspect partition} stopped with the reason: #{inspect reason}, try to restart it" end
        state = load_metadata(state)
        tref = :erlang.start_timer(5000, self, {:restart_worker, partition})
        %{state | trefs: HashDict.put(trefs, partition, tref)}
    end
    {:noreply, state}
  end

  # TODO handle worker lock timeout

  def terminate(_reason, state) do
    state |> leader_resign
          |> zk_unregister
          |> stop_workers
          |> zk_close   # if zk close before wokers stopped, workers cannot release lock
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
                    zk_timeout: zk_timeout,
                    zk_path: zk_path} = state) do
    {:ok, pid} = :erlzk.connect(zk_servers, zk_timeout, [monitor: self])
    Process.link(pid)
    path = Path.join [zk_path, topic_name, group_name]
    online_path = Path.join [path, "consumers", "online"]
    offline_path = Path.join [path, "consumers", "offline"]
    balance_path = Path.join [path, "consumers", "balance"]
    %{state |   zk_pid: pid,
               zk_path: path,
       zk_balance_path: balance_path,
        zk_online_path: online_path,
       zk_offline_path: offline_path}
  end

  defp zk_close(%{zk_pid: nil} = state), do: state
  defp zk_close(%{zk_pid: pid} = state) do
    :erlzk.close(pid)
    %{state | zk_pid: nil}
  end

  defp zk_register(%{zk_pid: pid,
            zk_balance_path: balance_path,
             zk_online_path: online_path,
            zk_offline_path: offline_path} = state) do

    node_name = Atom.to_string(node)
    name = Path.join(online_path, node_name)
    balance_node = Path.join(balance_path, node_name)
    :ok = Cafex.ZK.Util.create_nodes(pid, [online_path, offline_path, balance_path])

    offline_node = Path.join(offline_path, node_name)
    case :erlzk.delete(pid, offline_node) do
      :ok ->
        Logger.info "I am back online"
      {:error, :no_node} ->
          :ok
      {:error, reason} ->
        throw reason
    end

    case :erlzk.create(pid, name, :ephemeral) do
      {:ok, _} ->
        %{state | zk_online_node: name, zk_balance_node: balance_node}
      {:error, reason} ->
        Logger.error "Failed to register consumer: #{name}, reason: #{inspect reason}"
        throw reason
    end
  end

  defp zk_unregister(%{zk_online_node: nil} = state), do: state
  defp zk_unregister(%{zk_online_node: zk_online_node, zk_pid: pid} = state) do
    :erlzk.delete(pid, zk_online_node)
    %{state | zk_online_node: nil}
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
    Logger.info "leader resign #{inspect seq}"
    :erlzk.delete(pid, seq)
    %{state | leader: {false, nil}}
  end
  defp leader_resign(state), do: state

  defp load_balance(%{leader: {false, _}} = state), do: state
  defp load_balance(%{zk_pid: pid,
             zk_balance_path: balance_path,
              zk_online_path: online_path,
             zk_offline_path: offline_path,
                  partitions: partitions} = state) do
    {consumers, should_delete} = zk_get_all_consumers(pid, balance_path, online_path, offline_path)

    Enum.each(should_delete, fn p ->
      :erlzk.delete(pid, Path.join(balance_path, p))
    end)

    rebalanced = Cafex.Consumer.LoadBalancer.rebalance(consumers, partitions)
    for {k, v} <- rebalanced do
      case Keyword.get(consumers, k) do
        ^v -> :ok # not changed
        _  ->
          zk_set_consumer_partitions(pid, balance_path, k, v)
      end
    end
    state
  end

  defp zk_get_all_consumers(zk, balance_path, online_path, offline_path) do
    {:ok, online_children} = Cafex.ZK.Util.get_children(zk, online_path, self)
    {:ok, offline_children} = Cafex.ZK.Util.get_children(zk, offline_path)
    consumers = Enum.concat(online_children, offline_children) |> Enum.uniq
    {:ok, balanced} = Cafex.ZK.Util.get_children_with_data(zk, balance_path)
    {consumers, should_delete} = Enum.map_reduce(consumers, balanced, fn p, acc ->
      data = decode_partitions(HashDict.get(acc, p))
      {{String.to_atom(p), data}, HashDict.delete(acc, p)}
    end)
    {Enum.sort(consumers), HashDict.keys(should_delete)}
  end

  defp zk_set_consumer_partitions(zk, path, consumer, partitions) do
    balance_node = Path.join [path, Atom.to_string(consumer)]
    case :erlzk.exists(zk, balance_node) do
      {:ok, _stat} ->
        :erlzk.set_data zk, balance_node, encode_partitions(partitions)
      {:error, :no_node} ->
        case :erlzk.create(zk, balance_node) do
          {:ok, _} ->
            :erlzk.set_data zk, balance_node, encode_partitions(partitions)
          {:error, :node_exists} ->
            :erlzk.set_data zk, balance_node, encode_partitions(partitions)
          {:error, :closed} ->
            # during reconnecting
            zk_set_consumer_partitions(zk, path, consumer, partitions)
          error ->
            Logger.error "Zookeeper error: #{inspect error}"
        end
      {:error, :closed} ->
        # during reconnecting
        zk_set_consumer_partitions(zk, path, consumer, partitions)
      error ->
        Logger.error "Zookeeper error: #{inspect error}"
    end
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

  defp restart_workers(%{zk_pid: zk, zk_balance_node: balance_node, workers: workers} = state) do
    case Cafex.ZK.Util.get_data(zk, balance_node, self) do
      {:ok, {data, _stat}} ->
        partitions = decode_partitions(data)
        should_stop = HashDict.keys(workers) -- partitions

        state =
        Enum.reduce should_stop, state, fn partition, acc ->
          stop_worker(partition, acc)
        end

        Enum.reduce partitions, state, fn partition, acc ->
          start_worker(partition, acc)
        end
      _error ->
        state
    end
  end

  defp try_restart_worker(pid, %{workers: workers, r_workers: r_workers} = state) do
    case HashDict.get(r_workers, pid) do
      nil -> state
      partition ->
        state = %{state | workers: HashDict.delete(workers, partition), r_workers: HashDict.delete(r_workers, pid)}
        start_worker partition, state
    end
  end

  defp start_worker(partition, %{workers: workers, r_workers: r_workers} = state) do
    case HashDict.get(workers, partition) do
      nil ->
        {:ok, pid} = do_start_worker(partition, state)
        %{state | workers: HashDict.put(workers, partition, pid), r_workers: HashDict.put(r_workers, pid, partition)}
      pid ->
        case Process.alive?(pid) do
          false ->
            start_worker(partition, %{state | workers: HashDict.delete(workers, partition), r_workers: HashDict.delete(r_workers, pid)})
          true ->
            state
        end
    end
  end

  defp stop_worker(partition, %{workers: workers, r_workers: r_workers} = state) do
    case HashDict.get(workers, partition) do
      nil ->
        state
      pid ->
        if Process.alive?(pid), do: Worker.stop(pid)
        %{state | workers: HashDict.delete(workers, partition), r_workers: HashDict.delete(r_workers, pid)}
    end
  end

  defp do_start_worker(partition, %{group_name: group,
                                    topic_name: topic,
                                    brokers: brokers,
                                    leaders: leaders,
                                    handler: handler,
                                    worker_cfg: worker_cfg,
                                    zk_pid: zk_pid,
                                    zk_path: zk_path}) do
    Logger.info fn -> "Starting consumer worker: #{topic}:#{group}:#{partition}" end
    leader = HashDict.get(leaders, partition)
    broker = HashDict.get(brokers, leader)
    Worker.start_link(self, handler, topic, group, partition, broker, zk_pid, zk_path, worker_cfg)
  end

  defp stop_workers(%{workers: workers} = state) do
    Enum.each workers, fn {_, pid} ->
      if Process.alive?(pid), do: Worker.stop(pid)
    end
    %{state | workers: HashDict.new, r_workers: HashDict.new}
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

  defp offset_fetch(storage, coordinator_pid, group, topic, partition) do
    request = %OffsetFetch.Request{consumer_group: group,
                                   topics: [{topic, [partition]}]}
    request = case storage do
      :zookeeper -> %{request | api_version: 0}
      :kafka     -> %{request | api_version: 1}
    end

    case Connection.request(coordinator_pid, request, OffsetFetch) do
      {:ok, %{topics: [{^topic, [{^partition, offset, metadata, :no_error}]}]}} ->
        {:ok, {offset, metadata}}
      {:ok, %{topics: [{^topic, [{^partition, _, _, error}]}]}} ->
        {:error, error}
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp offset_commit(storage, coordinator_pid, group, topic, partition, offset, metadata) do
    request = %OffsetCommit.Request{consumer_group: group,
                                    topics: [{topic, [{partition, offset, metadata}]}]}
    request = case storage do
      :zookeeper -> %{request | api_version: 0}
      :kafka     -> %{request | api_version: 1}
    end
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
    reply = try_fetch_consumer_metadata(pid, request)
    Connection.close(pid)
    reply
  end

  defp try_fetch_consumer_metadata(conn, request) do
    try_fetch_consumer_metadata(conn, request, 10)
  end
  defp try_fetch_consumer_metadata(_conn, _request, 0) do
    {:error, :consumer_coordinator_not_available}
  end
  defp try_fetch_consumer_metadata(conn, request, retries) when is_integer(retries) and retries > 0 do
    case Connection.request(conn, request, ConsumerMetadata) do
      {:ok, %{error: :no_error, coordinator_host: host, coordinator_port: port}} ->
        {:ok, {host, port}}
      {:ok, %{error: :consumer_coordinator_not_available}} ->
        # We have to send a ConsumerMetadataRequest and retry with back off if
        # you receive a ConsumerCoordinatorNotAvailableCode returned as an
        # error.
        # See
        # [Kafka Error Codes](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes)
        # [issue on StackOverflow](http://stackoverflow.com/questions/28513744/kafka-0-8-2-consumermetadatarequest-always-return-consumercoordinatornotavailabl)
        Logger.warn "Try fetch consumer metadata error: :consumer_coordinator_not_available, retry"
        :timer.sleep(2000)
        try_fetch_consumer_metadata(conn, request, retries - 1)
      {:ok, %{error: code}} ->
        Logger.error "Try fetch consumer metadata error: #{inspect code}"
        {:error, code}
      {:error, reason} ->
        Logger.error "Try fetch consumer metadata error: #{inspect reason}"
        {:error, reason}
    end
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
