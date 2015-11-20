defmodule Cafex.Consumer.LogConsumer do
  @moduledoc """
  A simple `Cafex.Consumer` implementation.

  It just print the message into logger output.
  """
  use Cafex.Consumer

  @type options :: [level: Logger.level]
  @type state :: options

  require Logger

  @spec init(opts :: options) :: {:ok, state}
  def init(options) do
    {:ok, options}
  end

  @spec consume(Cafex.Protocol.Message.t, state) :: {:ok, state}
  def consume(message, state)
  def consume(%{offset: offset, key: key, value: value}, opts) do
    level = Keyword.get(opts, :level, :debug)
    Logger.log level, fn -> "{offset: #{offset}, key: #{key}, value: #{inspect value}}" end
    {:ok, opts}
  end
end
