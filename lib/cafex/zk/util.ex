defmodule Cafex.ZK.Util do
  @moduledoc """
  ZooKeeper Utilities
  """

  @type zk :: pid
  @type watcher :: pid
  @type path :: String.t

  @spec create_nodes(zk, [path]) :: :ok | {:error, term}
  def create_nodes(_zk, []), do: :ok
  def create_nodes( zk, [node|rest]) do
    case create_node(zk, node) do
      :ok -> create_nodes(zk, rest)
      error -> error
    end
  end

  @spec create_node(zk, path) :: :ok | {:error, term}
  def create_node(zk, "/") do
    # if use chroot, the "/" node may be not exists
    case :erlzk.exists(zk, "/") do
      {:ok, _state} -> :ok
      {:error, :no_node} ->
        case :erlzk.create(zk, "/") do
          {:ok, _state} -> :ok
          {:error, :node_exists} -> :ok
          error -> error
        end
    end
  end
  def create_node( zk, path) do
    path = String.rstrip(path, ?/)
    case :erlzk.exists(zk, path) do
      {:ok, _stat} ->
        :ok
      {:error, :no_node} ->
        case create_node(zk, Path.dirname(path)) do
          :ok ->
            case :erlzk.create(zk, path) do
              {:ok, _} -> :ok
              {:error, :node_exists} -> :ok
              error -> error
            end
          error ->
            error
        end
    end
  end

  @spec get_children(zk, path, watcher) :: {:ok, [child_node::String.t]} | {:error, term}
  def get_children(zk, path, watcher \\ nil) do
    case do_get_children(zk, path, watcher) do
      {:ok, children} -> {:ok, Enum.map(children, &(List.to_string &1))}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec get_children_with_data(zk, path, watcher) :: {:ok, [{child_node :: String.t, data :: binary}]} | {:error, term}
  def get_children_with_data(zk, path, watcher \\ nil) do
    case do_get_children(zk, path, watcher) do
      {:ok, children} ->
        {:ok, children |> Enum.map(fn x ->
                         x = List.to_string(x)
                         case :erlzk.get_data(zk, Path.join(path, x)) do
                           {:ok, {data, _stat}} ->
                             {x, data}
                           {:error, :no_node} ->
                             {x, nil}
                         end
                       end)
                       |> Enum.into(%{})}
      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec get_data(zk, path, watcher) :: {:ok, term} | {:error, term}
  def get_data(zk, path, watcher \\ nil) do
    case do_exists(zk, path, watcher) do
      {:ok, _stat} -> do_get_data(zk, path, watcher)
      error -> error
    end
  end

  defp do_get_data(zk, path, nil), do: :erlzk.get_data(zk, path)
  defp do_get_data(zk, path, watcher), do: :erlzk.get_data(zk, path, watcher)

  defp do_exists(zk, path, nil), do: :erlzk.exists(zk, path)
  defp do_exists(zk, path, watcher), do: :erlzk.exists(zk, path, watcher)

  defp do_get_children(zk, path, nil), do: :erlzk.get_children(zk, path)
  defp do_get_children(zk, path, watcher), do: :erlzk.get_children(zk, path, watcher)
end
