defmodule Cafex.Partitioner.Random do
  @moduledoc """
  Random partitioner implementation.

  Read `Cafex.Partitioner` behaviour.
  """

  @behaviour Cafex.Partitioner

  def init(partitions) do
    :rand.seed(:exs1024)
    {:ok, partitions}
  end

  def partition(_message, partitions) do
    {:rand.uniform(partitions) - 1, partitions}
  end
end
