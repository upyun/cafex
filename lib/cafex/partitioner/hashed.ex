defmodule Cafex.Partitioner.Hashed do
  @behaviour Cafex.Partitioner

  def init(partitions) do
    {:ok, partitions}
  end

  def partition(message, partitions) do
    hash = :erlang.phash2(message.key, partitions)
    {hash, partitions}
  end
end
