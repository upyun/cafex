defmodule Cafex.ZK.Util do
  @moduledoc """
  ZooKeeper Utilities
  """

  def create_nodes(_pid, []), do: :ok
  def create_nodes( pid, [node|rest]) do
    case create_node(pid, node) do
      :ok -> create_nodes(pid, rest)
      error -> error
    end
  end

  def create_node(_pid, "/"), do: :ok
  def create_node( pid, path) do
    path = String.rstrip(path, ?/)
    case :erlzk.exists(pid, path) do
      {:ok, _stat} ->
        :ok
      {:error, :no_node} ->
        case create_node(pid, Path.dirname(path)) do
          :ok ->
            case :erlzk.create(pid, path) do
              {:ok, _} -> :ok
              {:error, :node_exists} -> :ok
              error -> error
            end
          error ->
            error
        end
    end
  end

  def get_children(pid, path), do: get_children(pid, path, nil)
  def get_children(pid, path, watcher) do
    case do_get_children(pid, path, watcher) do
      {:ok, children} -> {:ok, Enum.map(children, &(List.to_string &1))}
      {:error, reason} -> {:error, reason}
    end
  end

  def get_children_with_data(pid, path), do: get_children_with_data(pid, path, nil)
  def get_children_with_data(pid, path, watcher) do
    case do_get_children(pid, path, watcher) do
      {:ok, children} ->
        {:ok, children |> Enum.map(fn x ->
                         x = List.to_string(x)
                         case :erlzk.get_data(pid, Path.join(path, x)) do
                           {:ok, {data, _stat}} ->
                             {x, data}
                           {:error, :no_node} ->
                             {x, nil}
                         end
                       end)
                       |> Enum.into(HashDict.new)}
      {:error, reason} ->
        {:error, reason}
    end
  end

  def get_data(pid, path), do: get_data(pid, path, nil)
  def get_data(pid, path, watcher) do
    case do_exists(pid, path, watcher) do
      {:ok, _stat} -> do_get_data(pid, path, watcher)
      error -> error
    end
  end

  defp do_get_data(pid, path, nil), do: :erlzk.get_data(pid, path)
  defp do_get_data(pid, path, watcher), do: :erlzk.get_data(pid, path, watcher)

  defp do_exists(pid, path, nil), do: :erlzk.exists(pid, path)
  defp do_exists(pid, path, watcher), do: :erlzk.exists(pid, path, watcher)

  defp do_get_children(pid, path, nil), do: :erlzk.get_children(pid, path)
  defp do_get_children(pid, path, watcher), do: :erlzk.get_children(pid, path, watcher)
end
