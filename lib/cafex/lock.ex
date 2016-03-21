defmodule Cafex.Lock do
  @moduledoc """
  A general, backend-pluggable lock implementation.

  It's a FSM.
  """

  @behaviour :gen_fsm

  require Logger

  @typedoc "`Cafex.Lock.Behaviour` module"
  @type locker :: atom

  @typedoc "Arguments for `locker`"
  @type args :: [term]

  defmodule State do
    @moduledoc false
    defstruct [:locker, :timeout, :from, :data, :timeout_start]
  end

  # ===================================================================
  # Macros
  # ===================================================================

  defmacro __using__(_opts) do
    quote do
      @behaviour Cafex.Lock.Behaviour
      def init(_args), do: {:ok, []}
      # def handle_acquire(state), do: {:ok, state}
      def handle_release(state), do: {:ok, state}
      def terminate(_state), do: :ok

      defoverridable [init: 1, handle_release: 1, terminate: 1]
    end
  end

  # ===================================================================
  # API
  # ===================================================================

  @doc """
  Non-blocking function to acquire a lock

  The `args` is pass to `locker.init(args)`.

  If at first it hold the lock, this function will return `{:ok, pid}`.

  Or else, it will return `{:error, :locked}` and stop the FSM if `timeout` is `0`.
  Or it will return `{:wait, pid}` if `timeout` is other then `0`, and the FSM will
  wait for the lock until `timeout`.

  The caller will receive a message `{:lock, :ok}` if it get the lock before timeout,
  or `{:lock, :timeout}` and then stop.

  __NOTE__: The caller process is linked to the FSM process.
  """
  @spec acquire(locker, args, timeout) :: {:ok, pid} | {:error, term}
  def acquire(locker, args, timeout \\ :infinity) do
    {:ok, pid} = :gen_fsm.start_link __MODULE__, [locker, args, timeout], []
    case :gen_fsm.sync_send_event pid, :acquire, :infinity do
      :ok   -> {:ok, pid}
      :wait -> {:wait, pid}
      error -> {:error, error}
    end
  end

  def release(pid) do
    :gen_fsm.sync_send_all_state_event pid, :release, :infinity
  end

  # ===================================================================
  #  :gen_fsm callbacks
  # ===================================================================

  @doc false
  def init([locker, args, timeout]) do
    state = %State{timeout: timeout, locker: locker}
    {:ok, data} = locker.init(args)
    {:ok, :prepared, %{state | data: data}}
  end

  @doc false
  def prepared(:acquire, from, %{timeout: timeout} = state) do
    case do_acquire(state) do
      {:ok, data} ->
        {:reply, :ok, :locked, %{state | data: data}}
      {:wait, data} ->
        state = %{state | from: from, data: data, timeout_start: :os.timestamp}
        case timeout do
          0 -> {:stop, :normal, {:error, :locked}, state}
          t -> {:reply, :wait, :waiting, state, t}
        end
      {:error, _reason} = error ->
        {:stop, :normal, error, state}
    end
  end

  @doc false
  def prepared(:timeout, %{from: {pid, _}, timeout_start: start, timeout: timeout} = state) do
    case do_acquire(state) do
      {:ok, data} ->
        send pid, {:lock, :ok, self}
        {:next_state, :locked, %{state | data: data}}
      {:wait, data} ->
        {:next_state, :waiting, %{state | data: data}, time_left(timeout, start)}
      {:error, _reason} = error ->
        {:stop, error, state}
    end
  end

  @doc false
  def waiting(:timeout, %{from: from} = state) do
    send from, {:lock, :timeout, self}
    {:stop, :normal, state}
  end

  @doc false
  def handle_event(event, state_name, state_data) do
    {:stop, {:bad_event, state_name, event}, state_data}
  end

  @doc false
  def handle_sync_event(:release, _from, _state_name, %{locker: locker, data: data} = state_data) do
    {:ok, data} = locker.handle_release(data)
    {:stop, :normal, :ok, %{state_data | data: data}}
  end
  def handle_sync_event(event, _from, state_name, state_data) do
    {:stop, {:bad_sync_event, state_name, event}, state_data}
  end

  @doc false
  def handle_info(:lock_changed, :waiting, state_data) do
    Logger.debug "lock_changed"
    {:next_state, :prepared, state_data, 0}
  end
  def handle_info(:lock_changed, :locked, state_data) do
    Logger.warn "The lock holder lose the lock"
    {:stop, :lose_lock, state_data}
  end
  def handle_info(info, state_name, state_data) do
    {:stop, {:bad_info, state_name, info}, state_data}
  end

  @doc false
  def terminate(_reason, _state_name, %{locker: locker, data: data}) do
    locker.terminate(data)
    :ok
  end

  @doc false
  def code_change(_old_vsn, state_name, state_data, _extra) do
    {:ok, state_name, state_data}
  end

  # ===================================================================
  #  Internal functions
  # ===================================================================

  defp do_acquire(%{locker: locker, data: data}) do
    locker.handle_acquire(data)
  end

  defp time_left(:infinity, _start), do: :infinity
  defp time_left(timeout, start) when is_integer(timeout) do
    case timeout - div(:timer.now_diff(:os.timestamp, start), 1000) do
      t when t <= 0 -> 0
      t -> t
    end
  end
end
