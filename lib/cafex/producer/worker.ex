defmodule Cafex.Producer.Worker do
  use GenServer

  defmodule State do
    @moduledoc false
    defstruct broker: nil,
              topic: nil,
              partition: nil,
              client_id: nil,
              conn: nil,
              acks: 1,
              batch_num: nil,
              batches: [],
              # max_request_size: nil,
              linger_ms: 0,
              timer: nil,
              timeout: 60000
  end

  alias Cafex.Connection
  alias Cafex.Protocol.Produce
  alias Cafex.Protocol.Produce.Request

  # ===================================================================
  # API
  # ===================================================================

  def start_link(broker, topic, partition, opts \\ []) do
    GenServer.start_link __MODULE__, [broker, topic, partition, opts]
  end

  def produce(pid, message) do
    GenServer.call pid, {:produce, message}
  end

  def async_produce(pid, message) do
    GenServer.cast pid, {:produce, message}
  end

  def stop(pid) do
    GenServer.call pid, :stop
  end

  # ===================================================================
  #  GenServer callbacks
  # ===================================================================

  def init([{host, port} = broker, topic, partition, opts]) do
    acks          = Keyword.get(opts, :acks, 1)
    timeout       = Keyword.get(opts, :timeout, 60000)
    client_id     = Keyword.get(opts, :client_id, "cafex")
    batch_num        = Keyword.get(opts, :batch_num)
    # max_request_size = Keyword.get(opts, :max_request_size)
    linger_ms        = Keyword.get(opts, :linger_ms)

    state = %State{ broker: broker,
                    topic: topic,
                    partition: partition,
                    client_id: client_id,
                    acks: acks,
                    batch_num: batch_num,
                    # max_request_size: max_request_size,
                    linger_ms: linger_ms,
                    timeout: timeout }

    case Connection.start_link(host, port, client_id: client_id) do
      {:ok, pid} ->
        {:ok, %{state | conn: pid}}
      {:error, reason} ->
        {:stop, reason}
    end
  end

  def handle_call({:produce, message}, from, state) do
    maybe_produce(message, from, state)
  end
  def handle_call(:stop, _from, state) do
    {:stop, :normal, state}
  end

  def handle_cast({:produce, message}, state) do
    maybe_produce(message, nil, state)
  end

  def handle_info({:timeout, timer, {:linger_timeout, from}}, %{timer: timer, batches: batches} = state) do
    result = batches |> Enum.reverse |> do_produce(state)
    state = %{state|timer: nil, batches: []}
    case result do
      :ok ->
        {:noreply, state}
      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  def terminate(_reason, %{conn: conn}) do
    if conn, do: Connection.close(conn)
    :ok
  end

  # ===================================================================
  #  Internal functions
  # ===================================================================

  defp maybe_produce(message, from, %{linger_ms: linger_ms} = state) when is_integer(linger_ms) and linger_ms <= 0 do
    case do_produce([{from, message}], state) do
      :ok -> {:noreply, state}
      {:error, reason} -> {:stop, reason, state}
    end
  end
  defp maybe_produce(message, from, %{batches: batches, batch_num: batch_num} = state) when length(batches) + 1 >= batch_num do
    result = [{from, message}|batches] |> Enum.reverse |> do_produce(state)
    %{state|batches: []}
    case result do
      :ok -> {:noreply, state}
      {:error, reason} -> {:stop, reason, state}
    end
  end
  defp maybe_produce(message, from, %{linger_ms: linger_ms, batches: batches, timer: timer} = state) do
    timer = case timer do
      nil ->
        timer = :erlang.start_timer(linger_ms, self, {:linger_timeout, from})
      timer ->
        timer
    end
    {:noreply, %{state|batches: [{from, message}|batches], timer: timer}}
  end

  defp do_produce(message_pairs, state) do
    case do_request(message_pairs, state) do
      {:ok, replies} ->
        Enum.each(replies, &do_reply/1)
        :ok
      {:error, reason} ->
        Enum.each(message_pairs, &(do_reply({&1, reason})))
        {:error, reason}
    end
  end

  defp do_request(message_pairs, %{topic: topic,
                             partition: partition,
                             acks: acks,
                             timeout: timeout,
                             conn: conn} = state) do

    messages = Enum.map(message_pairs, fn {_from, message} ->
      %{message | topic: topic, partition: partition}
    end)

    request = %Request{ required_acks: acks,
                        timeout: timeout,
                        messages: messages }

    case Connection.request(conn, request, Produce) do
      {:ok, [{^topic, [%{error: :no_error}=partition]}]} ->
        replies = Enum.map(message_pairs, fn {from, _} ->
          {from, :ok}
        end)
        {:ok, replies}
      {:ok, [{^topic, [%{error: reason}]}]} ->
        replies = Enum.map(message_pairs, fn {from, _} ->
          {from, {:error, reason}}
        end)
        {:ok, replies}
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp do_reply({nil, _reply}), do: :ok
  defp do_reply({from, reply}), do: GenServer.reply(from, reply)
end
