defmodule Cafex.ZK.Leader do
  @moduledoc """
  Leader Election
  """

  alias Cafex.ZK.Util

  def election(pid, path) do
    {:ok, seq} = create_node(pid, path)
    election(pid, path, seq)
  end

  def election(pid, path, seq) do
    case get_children(pid, path) do
      [^seq|_] ->
        {true, seq}
      children ->
        # find next lowest sequence
        case Enum.find_index(children, fn x -> x == seq end) do
          nil ->
            election(pid, path) # should not happen
          idx ->
            watch(pid, path, seq, Enum.at(children, idx - 1))
        end
    end
  end

  defp create_node(pid, path) do
    case :erlzk.create(pid, path <> "/n_", :ephemeral_sequential) do
      {:ok, seq} ->
        {:ok, List.to_string(seq)}
      {:error, :no_node} ->
        :ok = Util.create_node(pid, path)
        create_node(pid, path)
    end
  end

  defp get_children(pid, path) do
    {:ok, children} = :erlzk.get_children(pid, path)
    children |> Enum.map(fn x -> path <> "/" <> List.to_string(x) end)
             |> Enum.sort
  end

  defp watch(pid, path, seq, lower) do
    case :erlzk.exists(pid, lower, watcher(lower, seq)) do
      {:ok, _} ->
        {false, seq}
      {:error, :no_node} ->
        election(pid, path, seq)
    end
  end

  defp watcher(path, seq) do
    parent = self
    spawn_link fn ->
      receive do
        {:node_deleted, ^path} ->
          send parent, {:leader_election, seq}
      end
    end
  end
end
