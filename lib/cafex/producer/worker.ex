defmodule Cafex.Producer.Worker do
  use GenServer

  defmodule State do
    @moduledoc false
    defstruct broker: nil,
              topic: nil,
              topic_server: nil,
              partition: nil,
              client_id: nil,
              conn: nil,
              required_acks: 1,
              timeout: 60000
  end

  alias Cafex.Connection
  alias Cafex.Protocol.Produce
  alias Cafex.Protocol.Produce.Request

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
    client_id     = Keyword.get(opts, :client_id, "cafex")

    state = %State{ broker: broker,
                    topic: topic,
                    topic_server: topic_server,
                    partition: partition,
                    client_id: client_id,
                    required_acks: required_acks,
                    timeout: timeout }

    case Connection.start_link(host, port, client_id: client_id) do
      {:ok, pid} ->
        {:ok, %{state | conn: pid}}
      {:error, reason} ->
        {:stop, reason}
    end
  end

  def handle_call({:produce, message}, _from, state) do
    do_produce(message, state)
  end
  def handle_call(:stop, _from, state) do
    {:stop, :normal, state}
  end

  def terminate(_reason, %{conn: conn}) do
    if conn, do: Connection.close(conn)
    :ok
  end

  # ===================================================================
  #  Internal functions
  # ===================================================================

  defp do_produce(message, %{topic: topic,
                             partition: partition,
                             required_acks: required_acks,
                             timeout: timeout,
                             conn: conn} = state) do

    message = %{message | topic: topic, partition: partition}

    request = %Request{ required_acks: required_acks,
                        timeout: timeout,
                        messages: [message] }

    case Connection.request(conn, request, Produce) do
      {:ok, [{^topic, [%{error: :no_error, offset: offset, partition: partition}]}]} ->
        {:reply, {:ok, partition, offset}, state}
      {:ok, [{^topic, [%{error: error}]}]} ->
        {:reply, {:error, error}, state}
      {:error, reason} = err ->
        {:stop, reason, err, state}
    end
  end
end
