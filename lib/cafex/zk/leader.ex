defmodule Cafex.ZK.Leader do
  @moduledoc """
  Leader Election implementation with ZooKeeper

  Visit [Zookeeper Recipes](http://zookeeper.apache.org/doc/trunk/recipes.html#sc_leaderElection) to read more.

  ## TODO

  Handle zookeeper event missed problem.
  """

  @type zk :: pid
  @type path :: String.t
  @type seq :: String.t

  alias Cafex.ZK.Util

  @doc """
  Leader election function.

  If you haven't got a sequence, call this function first, or else call
  `election/3` with your `seq` instead.

  It will return `{true, seq}` if the node is a leader.

  If it's not a leader, `{false, seq}` will return. And the current process
  will watch for leader changes. If the leader node was deleted, the process
  will receive a message `{:leader_election, seq}` and you must call
  `election/3` again to volunteer to be a leader.
  """
  @spec election(zk, path) :: {true, seq} | {false, seq}
  def election(zk, path) do
    {:ok, seq} = create_node(zk, path)
    election(zk, path, seq)
  end

  @doc """
  Leader election function.

  See `election/2`.
  """
  @spec election(zk, path, seq) :: {true, seq} | {false, seq}
  def election(zk, path, seq) do
    case get_children(zk, path) do
      [^seq|_] ->
        {true, seq}
      children ->
        # find next lowest sequence
        case Enum.find_index(children, fn x -> x == seq end) do
          nil ->
            election(zk, path) # should not happen
          idx ->
            watch(zk, path, seq, Enum.at(children, idx - 1))
        end
    end
  end

  defp create_node(zk, path) do
    case :erlzk.create(zk, path <> "/n_", :ephemeral_sequential) do
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

  defp watch(zk, path, seq, lower) do
    case :erlzk.exists(zk, lower, watcher(lower, seq)) do
      {:ok, _} ->
        {false, seq}
      {:error, :no_node} ->
        election(zk, path, seq)
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
