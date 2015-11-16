defmodule Cafex.Producer do
  @moduledoc false

  use GenServer

  @default_client_id "cafex_producer"
  @default_acks 1
  @default_batch_num 200
  # @default_max_request_size 1024 * 1024
  @default_linger_ms 0
  @default_timeout 60000

  require Logger

  alias Cafex.Protocol.Message

  defmodule State do
    defstruct topic: nil,
              topic_pid: nil,
              partitioner: nil,
              partitioner_state: nil,
              brokers: nil,
              leaders: nil,
              partitions: 0,
              workers: HashDict.new,
              worker_opts: nil
  end

  # ===================================================================
  # API
  # ===================================================================

  def start_link(topic_pid, opts) do
    GenServer.start_link __MODULE__, [topic_pid, opts]
  end

  def produce(pid, value, opts \\ []) do
    key        = Keyword.get(opts, :key)
    partition  = Keyword.get(opts, :partition)
    message    = %Message{key: key, value: value, partition: partition}
    worker_pid = GenServer.call pid, {:get_worker, message}
    Cafex.Producer.Worker.produce(worker_pid, message)
  end

  def async_produce(pid, value, opts \\ []) do
    key        = Keyword.get(opts, :key)
    partition  = Keyword.get(opts, :partition)

    message    = %Message{key: key, value: value, partition: partition}
    worker_pid = GenServer.call pid, {:get_worker, message}
    Cafex.Producer.Worker.async_produce(worker_pid, message)
  end

  # ===================================================================
  #  GenServer callbacks
  # ===================================================================

  def init([topic_pid, opts]) do
    Process.flag(:trap_exit, true)

    client_id        = Keyword.get(opts, :client_id, @default_client_id)
    acks             = Keyword.get(opts, :acks, @default_acks)
    batch_num        = Keyword.get(opts, :batch_num, @default_batch_num)
    # max_request_size = Keyword.get(opts, :max_request_size, @default_max_request_size)
    linger_ms        = Keyword.get(opts, :linger_ms, @default_linger_ms)

    state = %State{topic_pid: topic_pid,
                   worker_opts: [
                     client_id: client_id,
                     acks: acks,
                     batch_num: batch_num,
                     # max_request_size: max_request_size,
                     linger_ms: linger_ms,
                     timeout: @default_timeout
                   ]} |> load_metadata
                      |> start_workers

    partitioner = Keyword.get(opts, :partitioner, Cafex.Partitioner.Random)
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

  defp dispatch(%{partition: nil} = message, %{partitioner: partitioner,
                                               partitioner_state: partitioner_state,
                                               workers: workers} = state) do
    {partition, new_state} = partitioner.partition(message, partitioner_state)
    # TODO: check partition availability
    worker_pid = HashDict.get(workers, partition)
    {worker_pid, %{state | partitioner_state: new_state}}
  end
  defp dispatch(%{partition: partition}, %{workers: workers} = state) do
    # TODO: check partition availability
    worker_pid = HashDict.get(workers, partition)
    {worker_pid, state}
  end

  defp load_metadata(%{topic_pid: topic_pid} = state) do
    metadata = Cafex.Topic.Server.metadata topic_pid

    %{state | topic: metadata.name,
              brokers: metadata.brokers,
              leaders: metadata.leaders,
              partitions: metadata.partitions}
  end

  defp start_workers(%{partitions: partitions} = state) do
    Enum.reduce 0..(partitions - 1), state, fn partition, acc ->
      start_worker(partition, acc)
    end
  end

  defp start_worker(partition, %{topic: topic, brokers: brokers,
                                 leaders: leaders, workers: workers,
                                 worker_opts: worker_opts} = state) do
    leader = HashDict.get(leaders, partition)
    broker = HashDict.get(brokers, leader)
    Logger.debug fn -> "Starting producer worker { topic: #{topic}, partition: #{partition}, broker: #{inspect broker} } ..." end
    {:ok, pid} = Cafex.Producer.Worker.start_link(broker, topic, partition, worker_opts)
    %{state | workers: HashDict.put(workers, partition, pid)}
  end
end
