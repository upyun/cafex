defmodule Cafex.Consumer.WorkerTest do
  use ExUnit.Case, async: true

  alias Cafex.Consumer.Worker
  alias Cafex.Consumer.Worker.State
  alias Cafex.Protocol.Message

  defmodule GenericHandler do
    use Cafex.Consumer

    def consume(_msg, state) do
      {:ok, state}
    end
  end

  defmodule PauseHandler do
    use Cafex.Consumer

    def consume(_msg, _state) do
      {:pause, 100}
    end
  end

  defmodule LockOk do
    use Cafex.Lock

    def acquire(_path, _args) do
      Cafex.Lock.acquire __MODULE__, [], :infinity
    end

    def release(pid) do
      Cafex.Lock.release pid
    end

    def handle_acquire(state) do
      {:ok, state}
    end
  end

  defmodule LockWait do
    use Cafex.Lock

    def acquire(_path, _args) do
      Cafex.Lock.acquire __MODULE__, [], :infinity
    end

    def release(pid) do
      Cafex.Lock.release pid
    end

    def handle_acquire(_state) do
      {:wait, self}
    end
  end

  defmodule Connection do
    use GenServer

    def start_link(host, port, opts \\ []) do
      GenServer.start_link __MODULE__, [host, port, opts]
    end

    def async_request(pid, request, receiver) do
      GenServer.cast pid, {:async_request, request, receiver}
    end

    def close(pid) do
      GenServer.call pid, :close
    end

    def init([_host, _port, opts]) do
      {:ok, opts}
    end

    def handle_call(:close, _from, state) do
      {:stop, :normal, :ok, state}
    end

    def handle_cast({:async_request, _request, receiver}, state) do
      send_reply(receiver, {:ok, state})
      {:noreply, state}
    end

    defp send_reply({:fsm, pid}, reply) when is_pid(pid) do
      cast_send pid, {:"$gen_event", {:kafka_response, reply}}
    end
    defp send_reply({:server, pid}, reply) when is_pid(pid) do
      cast_send pid, {:"$gen_cast", {:kafka_response, reply}}
    end
    defp send_reply(pid, reply) when is_pid(pid) do
      cast_send pid, {:kafka_response, reply}
    end

    defp cast_send(dest, msg) do
      try do
        :erlang.send dest, msg, [:noconnect, :nosuspend]
      catch
        _, reason -> {:error, reason}
      end
    end
  end

  defmodule OffsetManager do
    use GenServer

    def start_link do
      GenServer.start_link __MODULE__, []
    end

    def init([]) do
      {:ok, nil}
    end

    def handle_call({:fetch, _partition, _leader_conn}, _from, state) do
      {:reply, {:ok, {1, 1}}, state}
    end

    def handle_call({:commit, _partition, _offset, _metadata}, _from, state) do
      {:reply, :ok, state}
    end
  end

  test "consumer worker fsm" do
    {:ok, pid} = OffsetManager.start_link

    coordinator = pid
    handler = {PauseHandler, []}
    topic = "topic"
    group = "group"
    partition = 0
    broker = {"localhost", 9092}
    opts = [lock_cfg: {LockOk, []}]
    args = [coordinator, handler, topic, group, partition, broker, opts]

    assert {:ok, :acquire_lock, %State{} = state, 0} = Worker.init(args)

    state = %{state | connection_mod: Connection}
    next_state = Worker.acquire_lock(:timeout, state)
    assert {:next_state, :prepare, %State{lock: {true, _lock}} = state, 0} = next_state

    next_state = Worker.acquire_lock(:timeout, %State{state | lock_cfg: {LockWait, []}})
    assert {:next_state, :waiting_lock, %State{lock: {false, lock}} = state, _lock_timeout} = next_state

    next_state = Worker.handle_info({:lock, :ok, lock}, :waiting_lock, state)
    assert {:next_state, :prepare, %State{lock: {true, ^lock}}, 0} = next_state

    next_state = Worker.prepare(:timeout, state)
    assert {:next_state, :consuming, %State{buffer: []} = state, 0} = next_state

    next_state = Worker.consuming(:timeout, %State{state | buffer: []})
    assert {:next_state, :waiting_messages, %State{buffer: [], fetching: true} = state} = next_state

    response = {:ok, %{topics: [{"topic", [%{error: :no_error, messages: [], hwm_offset: 10}]}]}}
    next_state = Worker.consuming({:kafka_response, response}, state)
    assert {:next_state, :consuming, %State{buffer: []} = state, 1000} = next_state

    response = {:ok, %{topics: [{"topic", [%{error: :no_error, messages: [%Message{offset: 11}], hwm_offset: 10}]}]}}
    next_state = Worker.consuming({:kafka_response, response}, state)
    assert {:next_state, :consuming, %State{buffer: [_msg]} = state, 0} = next_state

    next_state = Worker.consuming(:timeout, %State{state | buffer: [%Message{}]})
    assert {:next_state, :pausing, state} = next_state

    next_state = Worker.consuming(:timeout, %State{state | fetching: false, buffer: [%Message{}], handler: GenericHandler})
    assert {:next_state, :waiting_messages, %State{fetching: true} = state} = next_state

    buffer = Enum.map(1..51, fn _ ->
      %Message{}
    end)
    next_state = Worker.consuming(:timeout, %State{state | fetching: false, buffer: buffer, handler: GenericHandler})
    assert {:next_state, :consuming, %State{fetching: true} = state, 0} = next_state

    buffer = Enum.map(1..101, fn _ ->
      %Message{}
    end)
    next_state = Worker.consuming(:timeout, %State{state | fetching: false, buffer: buffer, handler: GenericHandler})
    assert {:next_state, :consuming, %State{fetching: false} = state, 0} = next_state

    response = {:ok, %{topics: [{"topic", [%{error: :no_error, messages: [%Message{}], hwm_offset: 10}]}]}}
    next_state = Worker.pausing({:kafka_response, response}, state)
    assert {:next_state, :pausing, state} = next_state

    assert_receive :resume, 200

    next_state = Worker.handle_info(:resume, :pausing, state)
    assert {:next_state, :consuming, _state, 0} = next_state

    response = {:ok, %{topics: [{"topic", [%{error: :no_error, messages: [], hwm_offset: 10}]}]}}
    next_state = Worker.waiting_messages({:kafka_response, response}, %{state | buffer: []})
    assert {:next_state, :consuming, %State{buffer: []} = state, 1000} = next_state

    response = {:ok, %{topics: [{"topic", [%{error: :no_error, messages: [], hwm_offset: 10}]}]}}
    next_state = Worker.waiting_messages({:kafka_response, response}, %State{state | buffer: [%Message{}]})
    assert {:next_state, :consuming, %State{buffer: [_msg]} = state, 0} = next_state

    response = {:ok, %{topics: [{"topic", [%{error: :no_error, messages: [%Message{offset: 11}], hwm_offset: 10}]}]}}
    next_state =  Worker.waiting_messages({:kafka_response, response}, state)
    assert {:next_state, :consuming, %State{buffer: [_msg, _msg2]} = state, 0} = next_state

    next_state = Worker.waiting_messages(:timeout, state)
    assert {:stop, :fetch_timeout, _state} = next_state

    event = :some_event
    for state_name <- [:acquire_lock, :waiting_lock, :prepare, :consuming, :pausing, :waiting_messages] do
      next_state = Worker.handle_event(event, state_name, state)
      assert {:stop, {:bad_event, ^state_name, ^event}, _state} = next_state

      next_state = Worker.handle_sync_event(:stop, nil, state_name, state)
      assert {:stop, :normal, :ok, state} = next_state

      assert :ok == Worker.terminate(:some_reason, state_name, %State{state | lock: {false, nil}})

      assert {:ok, ^state_name, ^state} = Worker.code_change(1, state_name, state, nil)
    end

  end

end
