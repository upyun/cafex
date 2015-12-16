defmodule Cafex.ZK.Lock do
  @moduledoc """
  Distributed lock based on ZooKeeper

  Visit [Zookeeper Recipes](http://zookeeper.apache.org/doc/trunk/recipes.html#sc_recipes_Locks) to read more.
  """

  @type zk :: pid
  @type path :: String.t
  @type seq :: String.t
  @type lock :: seq

  alias Cafex.ZK.Util

  @spec acquire(zk, path, timeout) :: {:ok, lock} | {:wait, lock} | {:error, term}
  def acquire(zk, path, timeout \\ 0) do
    {:ok, seq} = create_node(zk, path)
    reacquire(zk, path, seq, timeout)
  end

  @spec reacquire(zk, path, seq, timeout) :: {:ok, lock} | {:wait, lock} | {:error, term}
  def reacquire(zk, path, seq, timeout \\ 0) do
    case check_sequence(zk, path, seq, timeout) do
      {:ok, lock} ->
        {:ok, lock}
      {:wait, lock} ->
        {:wait, lock}
      {:error, reason} ->
        :erlzk.delete(zk, seq)
        {:error, reason}
    end
  end

  @spec release(zk, lock) :: :ok | {:error, term}
  def release(zk, lock) do
    :erlzk.delete(zk, lock)
  end

  defp create_node(zk, path) do
    case :erlzk.create(zk, path <> "/lock-", :ephemeral_sequential) do
      {:ok, seq} ->
        {:ok, List.to_string(seq)}
      {:error, :no_node} ->
        :ok = Util.create_node(zk, path)
        create_node(zk, path)
    end
  end

  defp get_children(zk, path) do
    {:ok, children} = :erlzk.get_children(zk, path)
    children |> Enum.map(fn x -> path <> "/" <> List.to_string(x) end)
             |> Enum.sort
  end

  defp check_sequence(zk, path, seq, 0) do
    case get_children(zk, path) do
      [^seq|_] -> {:ok, seq}
      _x -> {:error, :locked}
    end
  end
  defp check_sequence(zk, path, seq, timeout) do
    case get_children(zk, path) do
      [^seq|_] ->
        {:ok, seq}
      children ->
        # find next lowest sequence
        case Enum.find_index(children, fn x -> x == seq end) do
          nil ->
            acquire(zk, path) # should not happen
          idx ->
            check_exists(zk, path, seq, Enum.at(children, idx - 1), timeout)
        end
    end
  end

  defp check_exists(zk, path, seq, lower, :infinity) do
    case :erlzk.exists(zk, lower, watcher(lower, seq)) do
      {:ok, _} ->
        {:wait, seq}
      {:error, :no_node} ->
        check_sequence(zk, path, seq, :infinity)
    end
  end
  defp check_exists(zk, path, seq, lower, timeout) when is_integer(timeout) do
    case :erlzk.exists(zk, lower, watcher(lower, timeout)) do
      {:ok, _} ->
        start = :os.timestamp
        receive do
          :check_again ->
            timeout = div(:timer.now_diff(:os.timestamp, start), 1000)
            check_sequence(zk, path, seq, timeout)
          :timeout ->
            {:error, :timeout}
        after
          timeout ->
            {:error, :timeout}
        end
      {:error, :no_node} ->
        check_sequence(zk, path, seq, timeout)
    end
  end

  defp watcher(path, seq) when is_binary(seq) do
    parent = self
    spawn_link fn ->
      receive do
        {:node_deleted, ^path} ->
          send parent, {:lock_again, seq}
      end
    end
  end
  defp watcher(path, timeout) when is_integer(timeout) do
    parent = self
    spawn_link fn ->
      receive do
        {:node_deleted, ^path} ->
          send parent, :check_again
      after
        timeout ->
          send parent, :timeout
      end
    end
  end
end
