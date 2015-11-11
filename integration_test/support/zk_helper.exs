defmodule ZKHelper do
  def connect(zk_cfg) do
    zk_servers = Keyword.get(zk_cfg, :servers)
               |> Enum.map(fn {h, p} -> {:erlang.bitstring_to_list(h), p} end)
    zk_timeout = Keyword.get(zk_cfg, :timeout)
    :erlzk.connect(zk_servers, zk_timeout)
  end

  def close(pid) do
    :erlzk.close(pid)
  end

  def rmr(pid, path) do
    case :erlzk.delete(pid, path) do
      :ok -> :ok
      {:error, :no_node} -> :ok
      {:error, :not_empty} ->
        case :erlzk.get_children(pid, path) do
          {:ok, children} ->
            Enum.each(children, fn child_path ->
              rmr(pid, Path.join(path, child_path))
            end)
            rmr(pid, path)
          {:error, :no_node} ->
            :ok
          {:error, _} = error ->
            error
        end
      error -> error
    end
  end
end
