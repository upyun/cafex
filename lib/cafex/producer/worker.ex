defmodule Cafex.Producer.Worker do
  use GenServer

  defmodule State do
    defstruct broker: nil,
              topic: nil,
              topic_server: nil,
              partition: nil,
              socket: nil,
              client_id: nil,
              required_acks: 1,
              timeout: 60000,
              correlation_id: 0
  end

  alias Cafex.Socket
  alias Cafex.Protocol
  alias Cafex.Protocol.Produce

  # ===================================================================
  # API
  # ===================================================================

  def start_link(broker, topic, partition, topic_server, opts \\ []) do
    GenServer.start_link __MODULE__, [broker, topic, partition, topic_server, opts]
  end

  def produce(pid, message) do
    GenServer.call pid, {:produce, message}
  end

  def stop(pid) do
    GenServer.call pid, :stop
  end

  # ===================================================================
  #  GenServer callbacks
  # ===================================================================

  def init([{host, port} = broker, topic, partition, topic_server, opts]) do
    required_acks = Keyword.get(opts, :required_acks, 1)
    timeout       = Keyword.get(opts, :timeout, 60000)
    client_id     = Keyword.get(opts, :client_id, Protocol.default_client_id)

    state = %State{ broker: broker,
                    topic: topic,
                    topic_server: topic_server,
                    partition: partition,
                    client_id: client_id,
                    required_acks: required_acks,
                    timeout: timeout }

    case Socket.open(host, port) do
      {:ok, socket} ->
        {:ok, %{state | socket: socket}}
      error ->
        error
    end
  end

  def handle_call({:produce, message}, _from, state) do
    do_produce(message, state)
  end
  def handle_call(:stop, _from, state) do
    {:stop, :normal, state}
  end

  def handle_info({:tcp_closed, socket}, %{socket: socket} = state) do
    {:stop, :socket_closed, %{state | socket: nil}}
  end

  def terminate(_reason, %{socket: socket}) do
    if socket, do: Socket.close(socket)
    :ok
  end

  # ===================================================================
  #  Internal functions
  # ===================================================================

  defp do_produce(message, %{topic: topic,
                             partition: partition,
                             client_id: client_id,
                             required_acks: required_acks,
                             timeout: timeout,
                             socket: socket,
                             correlation_id: correlation_id} = state) do
    request = Produce.create_request(correlation_id, client_id, topic, partition,
                                     message.value, message.key, required_acks, timeout)
    state = %{state | correlation_id: correlation_id + 1}
    case Socket.send_sync_request(socket, request) do
      {:ok, data} ->
        case Produce.parse_response(data) do
          [%{partitions: [%{error_code: 0, offset: offset, partition: partition}]}] ->
            {:reply, {:ok, partition, offset}, state}
          [%{partitions: [%{error_code: code}]}] ->
            {:reply, {:error, Protocol.error(code)}, state}
        end
      {:error, reason} = err ->
        {:stop, reason, err, state}
    end
  end
end
