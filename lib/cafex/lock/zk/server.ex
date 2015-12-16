defmodule Cafex.Lock.ZK.Server do
  use GenServer

  def start_link(parent) do
    GenServer.start_link __MODULE__, [parent]
  end

  def acquire(pid, zk_pid, path, seq) do
    GenServer.call pid, {:acquire, zk_pid, path, seq}
  end

  def release(pid, zk_pid, seq) do
    GenServer.call pid, {:release, zk_pid, seq}
  end

  def stop(pid) do
    GenServer.call pid, :stop
  end

  def init([parent]) do
    {:ok, parent}
  end

  def handle_call({:acquire, zk_pid, path, nil}, _from, state) do
    reply = Cafex.ZK.Lock.acquire(zk_pid, path, :infinity)
    {:reply, reply, state}
  end
  def handle_call({:acquire, zk_pid, path, seq}, _from, state) do
    reply = Cafex.ZK.Lock.reacquire(zk_pid, path, seq, :infinity)
    {:reply, reply, state}
  end

  def handle_call({:release, zk_pid, seq}, _from, state) do
    Cafex.ZK.Lock.release(zk_pid, seq)
    {:reply, :ok, state}
  end

  def handle_call(:stop, _from, state) do
    {:stop, :normal, :ok, state}
  end

  def handle_info({:lock_again, _seq}, parent) do
    send parent, :lock_changed
    {:noreply, parent}
  end

  def terminate(_reason, _state) do
    :ok
  end

end
