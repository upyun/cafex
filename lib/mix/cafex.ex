defmodule Mix.Cafex do
  @moduledoc false

  def get_opts(opts, key, required \\ false, default \\ nil) do
    case Keyword.get(opts, key) do
      nil ->
        case required do
          true ->
            raise "Require --#{key}"
          false -> default
        end
      value -> value
    end
  end

  def info_msg(msg), do: output(:info, msg)
  def log_msg(msg), do: output(:info, msg)
  def success_msg(msg), do: output(:success, msg)
  def debug_msg(msg), do: output(:debug, msg)
  def warn_msg(msg), do: output(:warn, msg)
  def error_msg(msg), do: output(:error, msg)

  def parse_servers_url(url) do
    {servers, path} = case String.split(url, "/", parts: 2) do
      [servers] -> {servers, "/"}
      [servers, path] -> {servers, Enum.join(["/", path])}
    end
    servers = String.split(servers, ",") |> Enum.map(fn server ->
      [host, port] = String.split(server, ":", parts: 2)
      {host, String.to_integer(port)}
    end)

    %{servers: servers, path: path}
  end

  def ensure_started do
    {:ok, _} = Application.ensure_all_started(:cafex)
  end

  # ===================================================================
  #  Internal functions
  # ===================================================================

  defp output(level, msg) when is_binary(msg) do
    Mix.shell.info [level_to_color(level), msg]
  end
  defp output(level, msg), do: output(level, "#{inspect msg}")

  defp level_to_color(:info), do: :normal
  defp level_to_color(:success), do: :green
  defp level_to_color(:debug), do: :cyan
  defp level_to_color(:warn), do: :yellow
  defp level_to_color(:error), do: :red
end
