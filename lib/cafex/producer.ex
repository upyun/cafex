defmodule Cafex.Producer do
  @moduledoc """
  Kafka producer
  """

  use GenServer

  @default_client_id "cafex_producer"
  @default_acks 1
  @default_batch_num 200
  # @default_max_request_size 1024 * 1024
  @default_linger_ms 0
  @default_timeout 60000

  @typedoc "Options used by the `start_link/2` functions"
  @type options :: [option]
  @type option :: {:client_id, Cafex.client_id} |
                  {:brokers, [Cafex.broker]} |
                  {:acks, -1..32767} |
                  {:batch_num, pos_integer} |
                  {:linger_ms, non_neg_integer}

  require Logger

  alias Cafex.Protocol.Message

  defmodule State do
    @moduledoc false
    defstruct topic: nil,
              topic_name: nil,
              feed_brokers: [],
              partitioner: nil,
              partitioner_state: nil,
              brokers: nil,
              leaders: nil,
              partitions: 0,
              workers: %{},
              client_id: nil,
              worker_opts: nil
  end

  # ===================================================================
  # API
  # ===================================================================

  @spec start_link(topic_name :: String.t, opts :: options) :: GenServer.on_start
  def start_link(topic_name, opts) do
    GenServer.start_link __MODULE__, [topic_name, opts]
  end

  @doc """
  Produce message to kafka server in the synchronous way.

  ## Options

  * `:key` The key is an optional message key that was used for partition assignment. The key can be `nil`.
  * `:partition` The partition that data is being published to.
  * `:metadata` The metadata is used for partition in case of you wan't to use key to do that.
  """
  @spec produce(pid :: pid, value :: binary, opts :: [Keyword.t]) :: :ok | {:error, term}
  def produce(pid, value, opts \\ []) do
    key        = Keyword.get(opts, :key)
    partition  = Keyword.get(opts, :partition)
    metadata   = Keyword.get(opts, :metadata)
    message    = %Message{key: key, value: value, partition: partition, metadata: metadata}
    worker_pid = GenServer.call pid, {:get_worker, message}
    Cafex.Producer.Worker.produce(worker_pid, message)
  end

  @doc """
  Produce message to kafka server in the asynchronous way.

  ## Options

  See `produce/3`
  """
  @spec async_produce(pid :: pid, value :: binary, opts :: [Keyword.t]) :: :ok
  def async_produce(pid, value, opts \\ []) do
    key        = Keyword.get(opts, :key)
    partition  = Keyword.get(opts, :partition)

    message    = %Message{key: key, value: value, partition: partition}
    worker_pid = GenServer.call pid, {:get_worker, message}
    Cafex.Producer.Worker.async_produce(worker_pid, message)
  end

  @spec stop(pid :: pid) :: :ok
  def stop(pid) do
    GenServer.call pid, :stop
  end

  # ===================================================================
  #  GenServer callbacks
  # ===================================================================

  def init([topic_name, opts]) do
    Process.flag(:trap_exit, true)

    client_id        = Keyword.get(opts, :client_id, @default_client_id)
    brokers          = Keyword.get(opts, :brokers)
    acks             = Keyword.get(opts, :acks, @default_acks)
    batch_num        = Keyword.get(opts, :batch_num, @default_batch_num)
    # max_request_size = Keyword.get(opts, :max_request_size, @default_max_request_size)
    linger_ms        = Keyword.get(opts, :linger_ms, @default_linger_ms)
    compression      = Keyword.get(opts, :compression)

    state = %State{topic_name: topic_name,
                   feed_brokers: brokers,
                   client_id: client_id,
                   worker_opts: [
                     client_id: client_id,
                     acks: acks,
                     batch_num: batch_num,
                     # max_request_size: max_request_size,
                     linger_ms: linger_ms,
                     timeout: @default_timeout,
                     compression: compression
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

  def handle_call(:stop, _from, state) do
    {:stop, :normal, :ok, state}
  end

  def handle_info({:EXIT, pid, reason}, %{workers: workers} = state) do
    state =
      case Enum.find(workers, fn {_k, v} -> v == pid end) do
        nil ->
          state
        {k, _} ->
          Logger.error "Producer worker down: #{inspect reason}, restarting"
          start_worker(k, %{state | workers: Map.delete(workers, k)})
      end
    {:noreply, state}
  end

  def terminate(_reason, %{workers: workers}=_state) do
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
    worker_pid = Map.get(workers, partition)
    {worker_pid, %{state | partitioner_state: new_state}}
  end
  defp dispatch(%{partition: partition}, %{workers: workers} = state) do
    # TODO: check partition availability
    worker_pid = Map.get(workers, partition)
    {worker_pid, state}
  end

  defp load_metadata(%{feed_brokers: brokers, topic_name: topic} = state) do
    {:ok, metadata} = Cafex.Kafka.Metadata.request(brokers, topic)
    metadata = Cafex.Kafka.Metadata.extract_metadata(metadata)

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
    leader = Map.get(leaders, partition)
    broker = Map.get(brokers, leader)
    Logger.debug fn -> "Starting producer worker { topic: #{topic}, partition: #{partition}, broker: #{inspect broker} } ..." end
    {:ok, pid} = Cafex.Producer.Worker.start_link(broker, topic, partition, worker_opts)
    %{state | workers: Map.put(workers, partition, pid)}
  end

end
