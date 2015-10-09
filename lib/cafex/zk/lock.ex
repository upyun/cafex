defmodule Cafex.ZK.Lock do
  @moduledoc """
  Distributed lock based on ZooKeeper
  """

  alias Cafex.ZK.Util

  def aquire(pid, path, timeout \\ 0) do
    {:ok, seq} = create_node(pid, path)
    case check_sequence(pid, path, seq, timeout) do
      {:ok, lock} ->
        {:ok, lock}
      {:error, reason} ->
        :erlzk.delete(pid, seq)
        {:error, reason}
    end
  end

  def release(pid, lock) do
    :erlzk.delete(pid, lock)
  end

  defp create_node(pid, path) do
    case :erlzk.create(pid, path <> "/lock-", :ephemeral_sequential) do
      {:ok, seq} ->
        {:ok, List.to_string(seq)}
      {:error, :no_node} ->
        :ok = Util.create_nodes(pid, path)
        aquire(pid, path)
    end
  end

  defp get_children(pid, path) do
    {:ok, children} = :erlzk.get_children(pid, path)
    children |> Enum.map(fn x -> path <> "/" <> List.to_string(x) end)
             |> Enum.sort
  end

  defp check_sequence(pid, path, seq, 0) do
    case get_children(pid, path) do
      [^seq|_] -> {:ok, seq}
      _x -> {:error, :locked}
    end
  end
  defp check_sequence(pid, path, seq, timeout) do
    case get_children(pid, path) do
      [^seq|_] ->
        {:ok, seq}
      children ->
        # find next lowest sequence
        case Enum.find_index(children, fn x -> x == seq end) do
          nil ->
            aquire(pid, path) # should not happen
          idx ->
            check_exists(pid, path, seq, Enum.at(children, idx - 1), timeout)
        end
    end
  end

  defp check_exists(pid, path, seq, lower, timeout) do
    case :erlzk.exists(pid, lower, watcher(lower, timeout)) do
      {:ok, _} ->
        start = :os.timestamp
        receive do
          :gone ->
            timeout = div(:timer.now_diff(:os.timestamp, start), 1000)
            check_sequence(pid, path, seq, timeout)
          :timeout ->
            {:error, :timeout}
        after
          timeout ->
            {:error, :timeout}
        end
      {:error, :no_node} ->
        check_sequence(pid, path, seq, timeout)
    end
  end

  defp watcher(path, timeout) do
    parent = self
    spawn fn ->
      receive do
        {:exists, ^path, :node_deleted} ->
          send parent, :gone
      after
        timeout ->
          send parent, :timeout
      end
    end
  end
end
