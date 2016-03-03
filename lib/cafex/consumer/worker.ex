defmodule Cafex.Consumer.Worker do
  @behaviour :gen_fsm

  require Logger

  @pre_fetch_size 50
  @max_wait_time 100
  @min_bytes 32 * 1024
  @max_bytes 1024 * 1024
  @client_id "cafex"

  @typedoc "Options used by the `start_link/9` functions"
  @type options :: [option]
  @type option :: {:max_wait_time, non_neg_integer} |
                  {:min_bytes, non_neg_integer} |
                  {:max_bytes, non_neg_integer}
  @type handler :: {module, args :: [Keyword.t]}

  defmodule State do
    @moduledoc false
    defstruct topic: nil,
              group: nil,
              client_id: nil,
              partition: nil,
              broker: nil,
              max_wait_time: nil,
              min_bytes: nil,
              max_bytes: nil,
              conn: nil, # partition leader connection
              lock: {false, nil},
              lock_cfg: nil,
              buffer: [],
              hwm_offset: 0,
              fetching: false,
              pre_fetch_size: 50,
              coordinator: nil,
              handler: nil,
              handler_data: nil
  end

  alias Cafex.Connection
  alias Cafex.Protocol.Fetch
  alias Cafex.Consumer.OffsetManager

  # ===================================================================
  # API
  # ===================================================================

  def start_link(coordinator, handler, topic, group, partition, broker, opts \\ []) do
    :gen_fsm.start_link __MODULE__, [coordinator, handler, topic, group, partition, broker, opts], []
  end

  def stop(pid) do
    :gen_fsm.sync_send_all_state_event pid, :stop, :infinity
  end

  # ===================================================================
  #  :gen_fsm callbacks
  # ===================================================================

  @doc false
  def init([coordinator, handler, topic, group, partition, broker, opts]) do
    opts = opts || []
    state = %State{topic: topic,
                   group: group,
                   client_id: Keyword.get(opts, :client_id) || @client_id,
                   partition: partition,
                   broker: broker,
                   coordinator: coordinator,
                   handler: handler,
                   lock_cfg:  Keyword.get(opts, :lock_cfg),
                   pre_fetch_size: Keyword.get(opts, :pre_fetch_size) || @pre_fetch_size,
                   max_wait_time: Keyword.get(opts, :max_wait_time) || @max_wait_time,
                   min_bytes: Keyword.get(opts, :min_bytes) || @min_bytes,
                   max_bytes: Keyword.get(opts, :max_bytes) || @max_bytes}
    {:ok, :acquire_lock, state, 0}
  end

  @lock_timeout 60000 * 5

  @doc false
  def acquire_lock(:timeout, %{partition: partition,
                              lock_cfg: {lock_mod, args},
                              group: group,
                              topic: topic} = state) do
    path = Path.join [group, topic, "partitions", Integer.to_string(partition)]
    lock_mod.acquire(path, args)
    |> case do
      {:wait, pid} ->
        {:next_state, :waiting_lock, %{state | lock: {false, pid}}, @lock_timeout}
      {:ok, lock} ->
        {:next_state, :prepare, %{state | lock: {true, lock}}, 0}
    end
  end

  @doc false
  def waiting_lock(:timeout, state) do
    {:stop, :lock_timeout, state}
  end

  @doc false
  def prepare(:timeout, %{partition: partition,
                          broker: {host, port},
                          handler: {handler, args},
                          client_id: client_id,
                          coordinator: coordinator} = state) do
    {:ok, conn} = Connection.start_link(host, port, client_id: client_id)
    {:ok, data} = handler.init(args)
    {:ok, {offset, _}} = OffsetManager.offset_fetch(coordinator, partition, conn)
    {:next_state, :consuming, %{state | conn: conn,
                                        hwm_offset: offset,
                                        handler: handler,
                                        handler_data: data}, 0}
  end

  @doc false
  def consuming(:timeout, state) do
    consume(state)
  end
  def consuming({:kafka_response, response}, state) do
    handle_fetch_response(response, state)
  end

  @doc false
  def waiting_messages(:timeout, state) do
    {:stop, :fetch_timeout, state}
  end
  def waiting_messages({:kafka_response, response}, state) do
    handle_fetch_response(response, state)
  end

  @doc false
  def handle_event(event, state_name, state_data) do
      {:stop, {:bad_event, state_name, event}, state_data}
  end

  @doc false
  def handle_sync_event(:stop, _from, _state_name, state) do
    {:stop, :normal, :ok, state}
  end

  @doc false
  def handle_info({:lock, :ok, lock}, :waiting_lock, %{lock: {false, lock}} = state_data) do
    {:next_state, :prepare, %{state_data | lock: {true, lock}}, 0}
  end

  @doc false
  def terminate(_reason, _state_name, %{handler: handler,
                                      handler_data: data} = state_data) do
    close_connection(state_data)
    release_lock(state_data)
    if data, do: handler.terminate(data)
    :ok
  end

  @doc false
  def code_change(_old, state_name, state_data, _extra) do
      {:ok, state_name, state_data}
  end

  # ===================================================================
  #  Internal functions
  # ===================================================================

  defp close_connection(%{conn: nil}), do: :ok
  defp close_connection(%{conn: pid}) do
    if Process.alive?(pid), do: Connection.close(pid)
  end

  defp release_lock(%{lock: {_, nil}}), do: :ok
  defp release_lock(%{lock: {_, lock}, lock_cfg: {mod, _}}) do
    mod.release(lock)
  end

  defp fetch_messages(%{fetching: true} = state), do: state
  defp fetch_messages(%{topic: topic,
                        partition: partition,
                        hwm_offset: offset,
                        max_bytes: max_bytes,
                        conn: conn} = state) do
    # Logger.debug fn -> "Consumer[#{group}:#{topic}:#{partition}] fetching messages: offset = #{offset}" end
    request =
    Map.take(state, [:max_wait_time, :min_bytes])
    |> Map.put(:topics, [{topic, [{partition, offset, max_bytes}]}])
    |> (&(struct(Fetch.Request, &1))).()

    Connection.async_request(conn, request, {:fsm, self})
    %{state | fetching: true}
  end

  defp handle_fetch_response(response, %{topic: topic,
                                         partition: partition,
                                         buffer: buffer,
                                         hwm_offset: offset} = state) do
    state = %{state | fetching: false}
    case response do
      {:ok, %{topics: [{^topic, [%{error: :no_error, messages: messages, hwm_offset: hwm_offset}]}]}} ->
        buffer = buffer ++ messages
        hwm_offset = case List.last(buffer) do
          nil -> hwm_offset
          msg -> msg.offset + 1
        end
        {:ok, %{state | buffer: buffer, hwm_offset: hwm_offset}}
      {:ok, %{topics: [{^topic, [%{error: :not_leader_for_partition = reason}]}]}} ->
        Logger.error "Failed to fetch new messages: #{inspect reason}, topic: #{topic}, partition: #{partition}, offset: #{offset}"
        {:error, :not_leader_for_partition, state}
      {:ok, %{topics: [{^topic, [%{error: :offset_out_of_range = reason}]}]}} ->
        Logger.error "Failed to fetch new messages: #{inspect reason}, topic: #{topic}, partition: #{partition}, offset: #{offset}"
        # {:ok, state}
        {:error, :offset_out_of_range, state}
      {:error, reason} ->
        Logger.error "Failed to fetch new messages: #{inspect reason}, topic: #{topic}, partition: #{partition}, offset: #{offset}"
        {:ok, state}
    end
    |> case do
      {:ok, %{buffer: []} = state} ->
        {:next_state, :consuming, state, 1000}
      {:ok, state} ->
        {:next_state, :consuming, state, 0}
      {:error, :offset_out_of_range, state} ->
        state = offset_reset(state)
        {:next_state, :consuming, state, 0}
      {:error, reason, state} ->
        {:stop, reason, state}
    end
  end

  defp consume(%{pre_fetch_size: pre_fetch_size} = state) do
    state = %{buffer: buffer} = do_consume(pre_fetch_size, state)
    buffer_length = length(buffer)
    cond do
      buffer_length == 0->
        {:next_state, :waiting_messages, fetch_messages(state)}
      buffer_length <= pre_fetch_size ->
        {:next_state, :consuming, fetch_messages(state), 0}
      true ->
        {:next_state, :consuming, state, 0}
    end
  end

  defp do_consume(0, state), do: state
  defp do_consume(_, %{buffer: []} = state), do: state
  defp do_consume(c, %{buffer: [first|rest]} = state) do
    state = handle_message(first, state)
    do_consume(c - 1, %{state | buffer: rest})
  end

  defp handle_message(%{offset: offset} = message, %{coordinator: coordinator,
                                                     topic: topic,
                                                     partition: partition,
                                                     handler: handler,
                                                     handler_data: handler_data} = state) do
    message = %{message | topic: topic, partition: partition}
    data = case handler.consume(message, handler_data) do
      {:ok, data} ->
        OffsetManager.offset_commit(coordinator, partition, offset + 1)
        data
      {:nocommit, data} ->
        data
    end

    %{state | handler_data: data}
  end

  defp offset_reset(%{coordinator: coordinator, partition: partition, conn: conn} = state) do
    {:ok, {offset, _}} = OffsetManager.offset_reset(coordinator, partition, conn)
    %{state | hwm_offset: offset}
  end
end
