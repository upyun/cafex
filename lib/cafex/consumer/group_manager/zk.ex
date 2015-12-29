defmodule Cafex.Consumer.GroupManager.ZK do
  @moduledoc """
  This depends on ZooKeeper to rebalancing and managing consumer group

  ## zookeeper structure

  It will build the structure on zookeeper like below after consumer started:

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
     |  |  |  |  |-- cafex@192.168.0.3                       # persistent Deprecated
     |  |  |-- locks
  ```

  First of all, every consumer will register itself under the node `consumers/online`.
  All consumer will elected a leader, which will be responsible for balancing.
  Leader collects all sub node under the `consumers/online` and `consumers/offline`,
  and executes the balancing, writes the result to every consumer node under the
  `consumers/balance` node.
  All consumers is listening on the corresponding node under the `consumers/balance`
  for changes to adjust the partition workers.

  **NOTE**: For now, the consumer will use the erlang node name as its name to
  register on the zookeeper, so make sure starting with the `-name` argument.

  """
  @behaviour :gen_fsm

  require Logger

  import Cafex.Consumer.Util
  alias Cafex.ZK.Util, as: ZK

  @timeout 5000
  @chroot "/cafex"
  @zk_servers [{"127.0.0.1", 2181}]

  defmodule State do
    @moduledoc false
    defstruct [:manager,
               :topic,
               :group,
               :partitions,
               :leader?,
               :servers,
               :chroot,
               :timeout,
               :zk_pid,
               :balance_path,
               :online_path,
               :offline_path]
  end

  #
  # API
  #

  def start_link(manager, topic, group, partitions, opts \\ []) do
    :gen_fsm.start_link __MODULE__, [manager,topic, group, partitions, opts], []
  end

  def stop(pid) do
    :gen_fsm.sync_send_all_state_event pid, :stop
  end

  #
  # :gen_fsm callbacks
  #

  @doc false
  def init([manager, topic, group, partitions, opts]) do
    timeout = Keyword.get(opts, :timeout) || @timeout
    chroot  = Keyword.get(opts, :choot)   || @chroot
    servers = Keyword.get(opts, :servers) || @zk_servers

    state = %State{manager: manager,
                   group: group,
                   topic: topic,
                   partitions: partitions,
                   servers: servers,
                   chroot: chroot,
                   timeout: timeout}
                 |> zk_connect
                 |> zk_register
    {:ok, :election, state, 0}
  end

  @doc false
  def election(:timeout, state) do
    {:next_state, :rebalance, leader_election(state), 0}
  end

  @doc false
  def rebalance(:timeout, %{leader?: {false, _}} = state) do
    {:next_state, :idle, state}
  end
  def rebalance(:timeout, state) do
    {:next_state, :idle, load_balance(state)}
  end

  @doc false
  def handle_sync_event(:stop, _from, _state_name, state_data) do
    {:stop, :normal, :ok, state_data}
  end

  @doc false
  def handle_event(event, state_name, state_data) do
      {:stop, {:bad_event, state_name, event}, state_data}
  end

  @doc false
  def handle_info({:leader_election, seq}, :idle, %{leader?: {_, seq}} = state) do
    {:next_state, :election, state, 0}
  end

  def handle_info({:disconnected, host, port}, state_name, state_data) do
    Logger.warn "erlzk disconnected #{inspect host}:#{inspect port}"
    # TODO
    # erlzk disconnected from zk server, every erlzk command will failed until reconnected
    {:next_state, state_name, state_data}
  end

  def handle_info({:connected, host, port}, state_name, state_data) do
    Logger.info "erlzk connected #{inspect host}:#{inspect port}"
    # TODO
    # Event maybe missed on zk server, need to check again for every watched znode
    {:next_state, state_name, state_data}
  end

  def handle_info({:expired, host, port}, _state_name, state_data) do
    Logger.warn "erlzk expired #{inspect host}:#{inspect port}"
    # All ephemeral znodes were deleted by zk server, and it caused leader to execute rebalance.
    # So this consumer must stop, and restart by the supervisor.
    {:stop, :zk_session_expired, state_data}
  end

  # handle zk watcher events
  def handle_info({:node_children_changed, path}, _state_name, %{online_path: path} = state_data) do
    # Online nodes changed
    Logger.info "#{state_data.group} consumers changed, rebalancing ..."
    # state = load_balance(state)
    # {:noreply, state}
    {:next_state, :rebalance, state_data, 0}
  end
  def handle_info({:node_created, path}, _state_name, %{balance_node: path} = state_data) do
    # balance_node created, start workers
    Logger.info "#{path} partition node created, restart_workers"
    notify_manager(state_data)
    {:next_state, :idle, state_data}
  end
  def handle_info({:node_data_changed, path}, _state_name, %{balance_node: path} = state_data) do
    # balance_node created, start workers
    Logger.info "#{path} partition layout changed, restart_workers"
    notify_manager(state_data)
    {:next_state, :idle, state_data}
  end
  def handle_info({:node_deleted, path}, _state_name, %{online_node: path} = state_data) do
    # Should not happend here.
    # If it was deleted, the zookeeper session must be expired, but the watch event won't received
    Logger.error "#{path} node deleted"
    # state = %{state | online_node: nil} |> zk_register
    # {:noreply, state}
    {:stop, :node_deleted, state_data}
  end
  def handle_info({:node_deleted, path}, _state_name, %{balance_node: path} = state_data) do
    # The zk_balance_node is not an ephemeral node, the only reason to be deleted
    # is the zk session of this node was expired, then the online node was deleted
    # by zk server, and the balance_node was deleted by leader after rebalance.
    #
    # Stop consumer itself and restart by supervisor can solve this.
    {:stop, :node_deleted, state_data}
  end

  @doc false
  def code_change(_old, state_name, state_data, _extra) do
    {:ok, state_name, state_data}
  end

  @doc false
  def terminate(_reason, _state_name, state_data) do
    state_data |> leader_resign |> zk_unregister |> zk_close
    :ok
  end

  #
  # Internal functions
  #

  defp zk_connect(%{group: group,
                    topic: topic,
                    servers: servers,
                    timeout: timeout,
                    chroot: chroot} = state) do
    {:ok, pid} = :erlzk.connect(servers, timeout, [monitor: self])
    Process.link(pid)
    path = Path.join [chroot, topic, group]
    online_path = Path.join [path, "consumers", "online"]
    offline_path = Path.join [path, "consumers", "offline"]
    balance_path = Path.join [path, "consumers", "balance"]
    %{state |   zk_pid: pid,
                chroot: path,
          balance_path: balance_path,
           online_path: online_path,
          offline_path: offline_path}
  end

  defp zk_close(%{zk_pid: nil} = state), do: state
  defp zk_close(%{zk_pid: pid} = state) do
    :erlzk.close(pid)
    %{state | zk_pid: nil}
  end

  defp zk_register(%{zk_pid: pid,
               balance_path: balance_path,
                online_path: online_path,
               offline_path: offline_path} = state) do

    node_name = Atom.to_string(node)
    name = Path.join(online_path, node_name)
    balance_node = Path.join(balance_path, node_name)
    :ok = ZK.create_nodes(pid, [online_path, offline_path, balance_path])

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
        %{state | online_node: name, balance_node: balance_node}
      {:error, reason} ->
        Logger.error "Failed to register consumer: #{name}, reason: #{inspect reason}"
        throw reason
    end
  end

  defp zk_unregister(%{online_node: nil} = state), do: state
  defp zk_unregister(%{online_node: online_node, zk_pid: pid} = state) do
    :erlzk.delete(pid, online_node)
    %{state | online_node: nil}
  end

  defp leader_election(%{leader?: {true, _}} = state), do: state
  defp leader_election(%{zk_pid: pid, chroot: chroot, leader?: {false, nil}} = state) do
    path = Path.join(chroot, "leader")
    %{state | leader?: Cafex.ZK.Leader.election(pid, path)}
  end
  defp leader_election(%{zk_pid: pid, chroot: chroot, leader?: {false, seq}} = state) do
    path = Path.join(chroot, "leader")
    %{state | leader?: Cafex.ZK.Leader.election(pid, path, seq)}
  end

  defp leader_resign(%{leader?: {true, seq}, zk_pid: pid} = state) do
    Logger.info "leader resign #{inspect seq}"
    :erlzk.delete(pid, seq)
    %{state | leader?: {false, nil}}
  end
  defp leader_resign(state), do: state

  # defp load_balance(%{leader?: {false, _}} = state), do: state
  defp load_balance(%{zk_pid: pid,
                balance_path: balance_path,
                 online_path: online_path,
                offline_path: offline_path,
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
    {:ok, online_children} = ZK.get_children(zk, online_path, self)
    {:ok, offline_children} = ZK.get_children(zk, offline_path)
    consumers = Enum.concat(online_children, offline_children) |> Enum.uniq
    {:ok, balanced} = ZK.get_children_with_data(zk, balance_path)
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

  defp notify_manager(%{zk_pid: zk, balance_node: balance_node, manager: pid}) do
    {:ok, {data, _state}} = ZK.get_data(zk, balance_node, self)
    assignment = decode_partitions(data)
    send pid, {:rebalanced, assignment}
  end
end
