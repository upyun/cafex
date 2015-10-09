defmodule Cafex.Consumer.Worker do
  @behaviour :gen_fsm

  defmodule State do
    defstruct consumer_mod: nil,
              consumer_state: nil,
              partition: nil,
              broker: nil,
              conn: nil,
              lock: nil
  end

  alias Cafex.Connection
  alias Cafex.ZK.Lock

  # ===================================================================
  # API
  # ===================================================================
  def start_link(topic, group, partition, broker, zk_pid, zk_path) do
    :gen_fsm.start_link __MODULE__, [topic, group, partition, broker, zk_pid, zk_path], []
  end

  def stop(pid) do
    :gen_fsm.sync_send_all_state_event pid, :stop, :infinity
  end

  def lock_aquired(pid, seq) do
    :gen_fsm.send_event pid, {:lock_aquired, seq}
  end

  # ===================================================================
  #  GenServer callbacks
  # ===================================================================

  def init([topic, group, partition, broker, zk_pid, zk_path]) do
    state = %State{broker: broker}
    {:ok, :aquire_lock, state, 0}
  end

  def aquire_lock(:timeout, state) do
    Lock.aquire(zk_pid, )
    {:ok, :waiting_lock, state, @lock_timeout}
  end

  def waiting_lock(:timeout, state) do
    {:stop, :lock_timeout, state}
  end
  def waiting_lock({:lock_aquired, seq}, %{broker: {host, port}} = state) do
    {:ok, conn} = Connection.start_link(host, port)
    {:next_state, :consuming, %{state | lock: seq, conn: conn}, 0}
  end

  def consuming(:timeout, state) do
  end

	@doc false
	def handle_event(event, state_name, state_data) do
		{:stop, {:bad_event, state_name, event}, state_data}
	end

  def handle_sync_event(:stop, _from, _state_name, state) do
    {:stop, :normal, :ok, state}
  end

	@doc false
	def handle_info(_msg, state_name, state_data) do
		{:next_state, state_name, state_data}
	end

	@doc false
	def terminate(_reason, _state_name, _state_data) do
    # TODO close kafka connection
    # TODO release lock
		:ok
	end

	@doc false
	def code_change(_old, state_name, state_data, _extra) do
		{:ok, state_name, state_data}
	end

  # ===================================================================
  #  Internal functions
  # ===================================================================
end
