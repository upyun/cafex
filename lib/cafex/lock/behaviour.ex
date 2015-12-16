defmodule Cafex.Lock.Behaviour do
  @moduledoc """
  Lock behaviour

  """

  use Behaviour

  @type state :: term

  defcallback init(term) :: {:ok, state} | {:error, term}

  @doc """
  Handle acquire callback

  Non-blocking function, return `{:ok, state}` if acquired the lock.
  Or `{:wait, state}` if waiting the lock, continue waiting in a asynchronized way(i.e. a process),
  then if lock changed, a `:lock_changed` message will send to this process.
  """
  defcallback handle_acquire(state) :: {:ok | :wait, state} | {:error, term}
  defcallback handle_release(state) :: {:ok, state} | {:error, term}
  defcallback terminate(state) :: :ok

end
