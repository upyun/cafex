defmodule Cafex.Consumer.LogConsumer do
  use Cafex.Consumer

  require Logger

  def init(opts) do
    {:ok, opts}
  end

  def consume(%{offset: offset, key: key, value: value}, opts) do
    level = Keyword.get(opts, :level, :debug)
    Logger.log level, fn -> "{offset: #{offset}, key: #{key}, value: #{inspect value}}" end
    {:ok, opts}
  end
end
