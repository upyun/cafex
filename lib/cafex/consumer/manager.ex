defmodule Cafex.Consumer.Manager do
  @moduledoc """
  This module is the main manager for a high-level kafka consumer

  ## structure

  The manager works together with a offset manager and a group manager to
  manage the consumer workers.

  The group manager handles the client assignment via kafka(0.9) or zookeeper.
  All consumers in a group will elect a group leader, and the leader collects
  all the other consumers infomation in the group, performs the load balance
  of partitions.

  The offset manager is responsible for workers offset commit/fetch. It will
  buffer the offset commit requests to improve the throughput.

  ## Options

  All this options must not be ommitted, expect `:client_id`.

    * `:client_id`       Optional, default client_id is "cafex"
    * `:handler`         Worker handler module
    * `:brokers`         Kafka brokers list
    * `:lock`            Indicate which lock implementation will be use in the worker, default is `:consul`, another option is `:zookeeper`
    * `:group_manager`   Default group manager is `:kafka` which depends on the kafka server with 0.9.x or above.
    * `:offset_storage`  Indicate where to store the consumer's offset, default is `:kafka`, another option is `:zookeeper`
    * `:auto_commit`
    * `:auto_commit_interval`
    * `:auto_commit_max_buffers`
    * `:auto_offset_reset`
    * `:fetch_wait_time`
    * `:fetch_min_bytes`
    * `:fetch_max_bytes`
    * `:zookeeper`

  These options for `start_link/3` can be put under the `:cafex` key in the `config/config.exs` file:

    ```elixir
    config :cafex, :myconsumer,
      client_id: "cafex",
      brokers: [{"192.168.99.100", 9092}, {"192.168.99.101", 9092}]
      zookeeper: [
        servers: [{"192.168.99.100", 2181}],
        path: "/cafex"
      ],
      handler: {MyConsumer, []}
    ```

  And then start the manager or start it in your supervisor tree

    ```elixir
    Cafex.Consumer.Manager.start_link(:myconsumer, "interested_topic")
    ```
  """

  use GenServer

  require Logger

  alias Cafex.Util
  alias Cafex.Kafka.GroupCoordinator
  alias Cafex.Consumer.OffsetManager
  alias Cafex.Consumer.Worker
  alias Cafex.Consumer.WorkerPartition

  @default_client_id "cafex"
  @default_group_manager :kafka
  @default_lock :consul

  @typedoc "Options used by the `start_link/3` functions"
  @type options :: [option]

  @type server :: {host :: String.t, port :: 0..65535}
  @type broker :: server
  @type client_id :: String.t
  @type zookeeper :: [zookeeper_option]
  @type zookeeper_option :: {:servers, [server]} |
                            {:path, String.t} |
                            {:timeout, non_neg_integer}
  @type option :: {:client_id, client_id} |
                  {:topic, String.t} |
                  {:handler, Cafex.Consumer.Worker.handler} |
                  {:brokers, [Cafex.broker]} |
                  {:fetch_wait_time, integer} |
                  {:fetch_min_bytes, integer} |
                  {:fetch_max_bytes, integer} |
                  {:auto_commit, boolean} |
                  {:auto_commit_interval, integer} |
                  {:auto_commit_max_buffers, integer} |
                  {:auto_offset_reset, :earliest | :latest} |
                  {:lock, :consul | :zookeeper} |
                  {:group_manager, :kafka | :zookeeper} |
                  {:offset_storage, :kafka | :zookeeper} |
                  {:zooKeeper, zookeeper}
  defmodule State do
    @moduledoc false
    defstruct group: nil,
              topic: nil,
              client_id: nil,
              feed_brokers: [],
              handler: nil,
              brokers: nil,
              leaders: nil,
              partitions: nil,
              lock: nil,
              group_manager: {nil, nil},
              group_manager_cfg: [],
              group_coordinator: nil,
              offset_manager: nil,
              worker_cfg: nil,
              workers: WorkerPartition.new,
              trefs: %{},
              offset_manager_cfg: [
                auto_offset_reset: :latest,
                offset_storage: :kafka
              ]
  end

  # ===================================================================
  # API
  # ===================================================================

  @doc """
  Start a consumer manager.

  ## Arguments

    * `name` Consumer group name
    * `topic` The topic name which will be consumed
    * `options` Starting options

  ## Options

  Read above.
  """
  @spec start_link(name :: atom, options) :: GenServer.on_start
  def start_link(name, opts \\ []) do
    GenServer.start_link __MODULE__, [name, opts], name: name
  end

  def stop(pid) do
    GenServer.call pid, :stop
  end

  # ===================================================================
  #  GenServer callbacks
  # ===================================================================

  def init([name, opts]) do
    Process.flag(:trap_exit, true)

    cfg        = Application.get_env(:cafex, name, [])
    topic      = Util.get_config(opts, cfg, :topic)
    client_id  = Util.get_config(opts, cfg, :client_id, @default_client_id)
    handler    = Util.get_config(opts, cfg, :handler)
    brokers    = Util.get_config(opts, cfg, :brokers)
    lock       = Util.get_config(opts, cfg, :lock, @default_lock)
    fetch_wait_time = Util.get_config(opts, cfg, :fetch_wait_time)
    fetch_min_bytes = Util.get_config(opts, cfg, :fetch_min_bytes)
    fetch_max_bytes = Util.get_config(opts, cfg, :fetch_max_bytes)
    pre_fetch_size  = Util.get_config(opts, cfg, :pre_fetch_size)

    zk_config  = Util.get_config(opts, cfg, :zookeeper, [])
    zk_cfg = [
      servers: (Keyword.get(zk_config, :servers) || [])
            |> Enum.map(fn {h, p} -> {:erlang.bitstring_to_list(h), p} end),
      chroot:  Keyword.get(zk_config, :chroot),
      timeout: Keyword.get(zk_config, :timeout)
    ]

    kafka_group_cfg = [
      timeout: Util.get_config(opts, cfg, :group_session_timeout)
    ]

    lock_cfg = case lock do
      :consul -> {Cafex.Lock.Consul, []}
      :zookeeper -> {Cafex.Lock.ZK, zk_cfg}
    end

    {group_manager, group_manager_cfg} =
    case Util.get_config(opts, cfg, :group_manager, @default_group_manager) do
      :kafka ->
        {Cafex.Consumer.GroupManager.Kafka, kafka_group_cfg}
      :zookeeper ->
        {Cafex.Consumer.GroupManager.ZK, zk_cfg}
    end

    group = Atom.to_string(name)
    Logger.info "Starting consumer: #{group} ..."

    offset_manager_cfg = [
      offset_storage: Util.get_config(opts, cfg, :offset_storage),
      auto_commit:    Util.get_config(opts, cfg, :auto_commit, true),
      interval:       Util.get_config(opts, cfg, :auto_commit_interval),
      max_buffers:    Util.get_config(opts, cfg, :auto_commit_max_buffers),
      offset_reset:   Util.get_config(opts, cfg, :auto_offset_reset, :latest)
    ]

    state = %State{ group: group,
                    topic: topic,
                    client_id: client_id,
                    feed_brokers: brokers,
                    handler: handler,
                    lock: lock,
                    worker_cfg: [
                      pre_fetch_size: pre_fetch_size,
                      max_wait_time: fetch_wait_time,
                      min_bytes: fetch_min_bytes,
                      max_bytes: fetch_max_bytes,
                      lock_cfg: lock_cfg
                    ],
                    offset_manager_cfg: offset_manager_cfg,
                    group_manager: {group_manager, nil},
                    group_manager_cfg: group_manager_cfg}
            |> load_metadata
            |> find_group_coordinator
            |> start_offset_manager
            |> start_group_manager

    {:ok, state}
  end

  def handle_call(:stop, _from, state) do
    {:stop, :normal, :ok, state}
  end

  def handle_info({:timeout, _tref, {:restart_worker, partition}}, %{trefs: trefs} = state) do
    state = %{state | trefs: Map.delete(trefs, partition)}
    state = start_worker(partition, state)
    {:noreply, state}
  end

  def handle_info({:rebalanced, assignment}, state) do
    state = maybe_restart_workers(assignment, state)
    {:noreply, state}
  end

  # handle linked process EXIT
  def handle_info({:EXIT, pid, reason}, %{offset_manager: pid} = state) do
    Logger.warn "OffsetManager exit with the reason #{inspect reason}"
    state = start_offset_manager(state)
    {:noreply, state}
  end
  def handle_info({:EXIT, pid, reason}, %{group_manager: {manager, pid}} = state) do
    Logger.error "GroupManager exit with the reason #{inspect reason}"
    {:stop, reason, %{state | group_manager: {manager, nil}}}
  end
  def handle_info({:EXIT, pid, :not_leader_for_partition}, state) do
    Logger.warn "Worker stopped due to not_leader_for_partition, reload leader and restart it"
    state = load_metadata(state)
    state = try_restart_worker(pid, state)
    {:noreply, state}
  end
  def handle_info({:EXIT, pid, :lock_timeout}, %{workers: workers, trefs: trefs} = state) do
    # worker lock_timeout, wait for sometimes and then restart it
    state = case WorkerPartition.partition(workers, pid) do
      nil -> state
      partition ->
        tref = :erlang.start_timer(5000, self, {:restart_worker, partition})
        %{state | trefs: Map.put(trefs, partition, tref)}
    end
    {:noreply, state}
  end
  def handle_info({:EXIT, _pid, :normal}, state) do
    {:noreply, state}
  end
  def handle_info({:EXIT, pid, reason}, %{workers: workers, trefs: trefs} = state) do
    state = case WorkerPartition.partition(workers, pid) do
      nil -> state
      partition ->
        Logger.info "Worker #{inspect pid} for partition #{inspect partition} stopped with the reason: #{inspect reason}, try to restart it"
        state = load_metadata(state)
        tref = :erlang.start_timer(5000, self, {:restart_worker, partition})
        %{state | trefs: Map.put(trefs, partition, tref)}
    end
    {:noreply, state}
  end

  # TODO handle worker lock timeout

  def terminate(_reason, state) do
    state
    |> stop_workers
    |> stop_offset_manager
    |> stop_group_manager
    :ok
  end

  # ===================================================================
  #  Internal functions
  # ===================================================================

  defp load_metadata(%{feed_brokers: brokers, topic: topic} = state) do
    {:ok, metadata} = Cafex.Kafka.Metadata.request(brokers, topic)
    metadata = Cafex.Kafka.Metadata.extract_metadata(metadata)
    %{state | brokers: metadata.brokers,
              leaders: metadata.leaders,
              partitions: metadata.partitions}
  end

  defp find_group_coordinator(%{group: group, brokers: brokers} = state) do
    {:ok, {host, port}} = GroupCoordinator.request(Map.values(brokers), group)
    %{state | group_coordinator: {host, port}}
  end

  defp start_offset_manager(%{group: group,
                              topic: topic,
                              partitions: partitions,
                              group_coordinator: group_coordinator,
                              offset_manager: nil,
                              offset_manager_cfg: cfg,
                              client_id: client_id} = state) do
    cfg = Keyword.merge([client_id: client_id], cfg)
    {:ok, pid} = OffsetManager.start_link(group_coordinator, partitions, group, topic, cfg)
    %{state | offset_manager: pid}
  end

  defp stop_offset_manager(%{offset_manager: nil} = state), do: state
  defp stop_offset_manager(%{offset_manager: pid} = state) do
    if Process.alive?(pid) do
      OffsetManager.stop(pid)
    end
    %{state | offset_manager: nil}
  end

  defp start_group_manager(%{group_manager: {manager, nil},
                             group: group,
                             topic: topic,
                             partitions: partitions,
                             group_manager_cfg: cfg} = state) do
    opts = Map.take(state, [:offset_manager, :group_coordinator])
        |> Map.to_list
        |> Keyword.merge(cfg)

    {:ok, pid} = manager.start_link(self, topic, group, partitions, opts)
    %{state | group_manager: {manager, pid}}
  end

  defp stop_group_manager(%{group_manager: {_, nil}} = state), do: state
  defp stop_group_manager(%{group_manager: {manager, pid}} = state) do
    if Process.alive?(pid) do
      manager.stop(pid)
    end
    %{state | group_manager: {manager, nil}}
  end

  defp maybe_restart_workers(assignment, %{workers: workers} = state) do
    should_stop = WorkerPartition.partitions(workers) -- assignment

    state =
    Enum.reduce should_stop, state, fn partition, acc ->
      stop_worker(partition, acc)
    end

    Enum.reduce assignment, state, fn partition, acc ->
      start_worker(partition, acc)
    end
  end

  defp try_restart_worker(pid, %{workers: workers} = state) do
    case WorkerPartition.partition(workers, pid) do
      nil -> state
      partition ->
        state = %{state | workers: WorkerPartition.delete(workers, partition, pid)}
        start_worker partition, state
    end
  end

  defp start_worker(partition, %{workers: workers} = state) do
    case WorkerPartition.worker(workers, partition) do
      nil ->
        {:ok, pid} = do_start_worker(partition, state)
        %{state | workers: WorkerPartition.update(workers, partition, pid)}
      pid ->
        case Process.alive?(pid) do
          false ->
            start_worker(partition, %{state | workers: WorkerPartition.delete(workers, partition, pid)})
          true ->
            state
        end
    end
  end

  defp stop_worker(partition, %{group: group, topic: topic, workers: workers} = state) do
    case WorkerPartition.worker(workers, partition) do
      nil ->
        state
      pid ->
        if Process.alive?(pid) do
          Logger.info "Stopping consumer worker: #{topic}:#{group}:#{partition}"
          Worker.stop(pid)
        end
        %{state | workers: WorkerPartition.delete(workers, partition, pid)}
    end
  end

  defp do_start_worker(partition, %{group: group,
                                    topic: topic,
                                    brokers: brokers,
                                    leaders: leaders,
                                    handler: handler,
                                    client_id: client_id,
                                    worker_cfg: worker_cfg,
                                    offset_manager: offset_manager}) do
    Logger.info "Starting consumer worker: #{topic}:#{group}:#{partition}"
    leader = Map.get(leaders, partition)
    broker = Map.get(brokers, leader)
    worker_cfg = Keyword.merge([client_id: client_id], worker_cfg)
    Worker.start_link(offset_manager, handler, topic, group, partition, broker, worker_cfg)
  end

  defp stop_workers(%{workers: workers} = state) do
    Enum.each WorkerPartition.workers(workers), fn pid ->
      if Process.alive?(pid), do: Worker.stop(pid)
    end
    %{state | workers: WorkerPartition.new}
  end
end
