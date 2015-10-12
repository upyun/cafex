defmodule Cafex.ZK.Util do
  @moduledoc """
  ZooKeeper Utilities
  """

  def create_nodes(_pid, "/"), do: :ok
  def create_nodes( pid, path) do
    path = String.rstrip(path, ?/)
    case :erlzk.exists(pid, path) do
      {:ok, _stat} ->
        :ok
      {:error, :no_node} ->
        case create_nodes(pid, Path.dirname(path)) do
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

  def get_children_with_data(pid, path, watcher) do
    case :erlzk.get_children(pid, path, watcher) do
      {:ok, children} ->
        {:ok, children |> Enum.map(fn x ->
                         x = List.to_string(x)
                         case :erlzk.get_data(pid, Path.join(path, x)) do
                           {:ok, {data, _stat}} ->
                             {x, data}
                           {:error, :no_node} ->
                             {x, nil}
                         end
                       end)}
      {:error, reason} ->
        {:error, reason}
    end
  end
end
