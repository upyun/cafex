defmodule Cafex.Lock.ZK do
  use Cafex.Lock

  require Logger

  @chroot '/cafex'
  @timeout 5000

  defmodule State do
    @moduledoc false
    defstruct zk: nil,
              zk_server: nil,
              lock: {false, nil},
              path: nil,
              timeout: nil
  end

  # ===================================================================
  # API
  # ===================================================================

  def acquire(path, opts \\ []) do
    Cafex.Lock.acquire __MODULE__, [path, opts], :infinity
  end

  def release(pid) do
    Cafex.Lock.release pid
  end

  # ===================================================================
  # Cafex.Lock.Behaviour callbacks
  # ===================================================================

  def init([path, opts]) do
    timeout = Keyword.get(opts, :zk_timeout, @timeout)
    servers = Keyword.get(opts, :servers)
    chroot  = Keyword.get(opts, :chroot, @chroot) |> :erlang.binary_to_list
    path    = Path.join("/", path)

    {:ok, zk_pid} = :erlzk.connect(servers, timeout, [chroot: chroot])
    {:ok, zk_server} = Cafex.Lock.ZK.Server.start_link self
    state = %State{zk: zk_pid, zk_server: zk_server, path: path, timeout: timeout}
    {:ok, state}
  end

  def handle_acquire(%{lock: {false, seq}, path: path, zk: zk_pid, zk_server: zk_server} = state) do
    case Cafex.Lock.ZK.Server.acquire(zk_server, zk_pid, path, seq) do
      {:ok, seq} ->
        Logger.debug "Held the lock #{inspect self}"
        {:ok, %{state | lock: {true, seq}}}
      {:wait, seq} ->
        state = %{state | lock: {false, seq}}
        {:wait, state}
    end
  end

  def handle_release(%{lock: {_, nil}} = state), do: {:ok, state}
  def handle_release(%{lock: {_, seq}, zk_server: zk_server, zk: zk_pid} = state) do
    Cafex.Lock.ZK.Server.release(zk_server, zk_pid, seq)
    {:ok, %{state | lock: {false, nil}}}
  end

  def terminate(%{zk_server: zk_server, zk: zk_pid} = state) do
    handle_release(state)
    Cafex.Lock.ZK.Server.stop(zk_server)
    :erlzk.close(zk_pid)
  end

end
