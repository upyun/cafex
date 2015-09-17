defmodule Cafex.Producer do
  @moduledoc false

  use GenServer

  require Logger

  alias Cafex.Message
  alias Cafex.Protocol

  defmodule State do
    defstruct topic: nil,
              topic_pid: nil,
              partitioner: nil,
              partitioner_state: nil,
              brokers: nil,
              leaders: nil,
              partitions: 0,
              workers: HashDict.new,
              client_id: nil,
              required_acks: 1,
              timeout: 60000
  end

  # ===================================================================
  # API
  # ===================================================================

  def start_link(topic_pid, opts) do
    GenServer.start_link __MODULE__, [topic_pid, opts]
  end

  def produce(pid, value, opts \\ []) do
    key = Keyword.get(opts, :key)
    message = %Message{key: key, value: value}
    worker_pid = GenServer.call pid, {:get_worker, message}
    Cafex.Producer.Worker.produce(worker_pid, message)
  end

  # ===================================================================
  #  GenServer callbacks
  # ===================================================================

  def init([topic_pid, opts]) do
    Process.flag(:trap_exit, true)

    client_id = Keyword.get(opts, :client_id, Protocol.default_client_id)

    state = %State{topic_pid: topic_pid,
                   client_id: client_id} |> load_metadata
                                         |> start_workers

    partitioner = Keyword.get(opts, :partitioner)
    {:ok, partitioner_state} = partitioner.init(state.partitions)

    {:ok, %{state | partitioner: partitioner,
                    partitioner_state: partitioner_state}}
  end

  def handle_call({:get_worker, message}, _from, state) do
    {worker, state} = dispatch(message, state)
    {:reply, worker, state}
  end

  def handle_info({:"EXIT", pid, reason}, %{workers: workers} = state) do
    Logger.error "Producer worker down: #{inspect reason}"
    state =
      case Enum.find(workers, fn {_k, v} -> v == pid end) do
        nil ->
          state
        {k, _} ->
          start_worker(k, %{state | workers: HashDict.delete(workers, k)})
      end
    {:noreply, state}
  end

  def terminate(_reason, %{workers: workers}) do
    for {_, pid} <- workers do
      Cafex.Producer.Worker.stop pid
    end
    :ok
  end

  # ===================================================================
  #  Internal functions
  # ===================================================================

  defp dispatch(message, %{partitioner: partitioner,
                           partitioner_state: partitioner_state,
                           workers: workers} = state) do
    {partition, new_state} = partitioner.partition(message, partitioner_state)
    worker_pid = HashDict.get(workers, partition)
    {worker_pid, %{state | partitioner_state: new_state}}
  end

  defp load_metadata(%{topic_pid: topic_pid} = state) do
    metadata = Cafex.Topic.Server.metadata topic_pid
    brokers = metadata.brokers |> Enum.map(fn b -> {b.node_id, {b.host, b.port}} end)
                               |> Enum.into(HashDict.new)

    [%{topic: name, partition_metadatas: partitions}] = metadata.topic_metadatas

    leaders = partitions |> Enum.map(fn p -> {p.partition_id, p.leader} end)
                         |> Enum.into(HashDict.new)

    %{state | topic: name, brokers: brokers, leaders: leaders, partitions: length(partitions)}
  end

  defp start_workers(%{partitions: partitions} = state) do
    Enum.reduce 0..(partitions - 1), state, fn partition, acc ->
      start_worker(partition, acc)
    end
  end

  defp start_worker(partition, %{topic: topic, brokers: brokers,
                                 leaders: leaders, workers: workers,
                                 client_id: client_id,
                                 required_acks: required_acks,
                                 timeout: timeout} = state) do
    leader = HashDict.get(leaders, partition)
    broker = HashDict.get(brokers, leader)
    Logger.debug fn -> "Starting producer worker { topic: #{topic}, partition: #{partition}, broker: #{inspect broker} } ..." end
    {:ok, pid} = Cafex.Producer.Worker.start_link(broker, topic, partition, client_id: client_id,
                                                                        required_acks: required_acks,
                                                                              timeout: timeout)
    %{state | workers: HashDict.put(workers, partition, pid)}
  end
end
