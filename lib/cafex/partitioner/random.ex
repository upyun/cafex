defmodule Cafex.Partitioner.Random do
  @behaviour Cafex.Partitioner

  def init(partitions) do
    :random.seed(:os.timestamp)
    {:ok, partitions}
  end

  def partition(_message, partitions) do
    {:random.uniform(partitions) - 1, partitions}
  end
end
