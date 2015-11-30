defmodule Cafex.Consumer.WorkerPartitionTest do
  use ExUnit.Case, async: true

  alias Cafex.Consumer.WorkerPartition

  test "WorkerPartition index test" do
    index = WorkerPartition.new

    assert [] == WorkerPartition.partitions(index)
    assert [] == WorkerPartition.workers(index)

    {w2p, p2w} = index = index
          |> WorkerPartition.update(1, 1)
          |> WorkerPartition.update(2, 2)
          |> WorkerPartition.update(3, 3)

    assert [1, 2, 3] = WorkerPartition.partitions(index) |> Enum.sort
    assert [1, 2, 3] = WorkerPartition.workers(index) |> Enum.sort

    assert_dict %{1 => 1, 2 => 2, 3 => 3}, w2p
    assert_dict %{1 => 1, 2 => 2, 3 => 3}, p2w

    assert 1 = WorkerPartition.worker(index, 1)
    assert 2 = WorkerPartition.worker(index, 2)
    assert 3 = WorkerPartition.worker(index, 3)

    assert 1 = WorkerPartition.partition(index, 1)
    assert 2 = WorkerPartition.partition(index, 2)
    assert 3 = WorkerPartition.partition(index, 3)

    {w2p, p2w} = index = index
          |> WorkerPartition.update(1, 4)
          |> WorkerPartition.update(2, 5)
          |> WorkerPartition.update(3, 6)

    assert [1, 2, 3] = WorkerPartition.partitions(index) |> Enum.sort
    assert [4, 5, 6] = WorkerPartition.workers(index) |> Enum.sort

    assert_dict %{4 => 1, 5 => 2, 6 => 3}, w2p
    assert_dict %{1 => 4, 2 => 5, 3 => 6}, p2w

    {w2p, p2w} = index = WorkerPartition.delete(index, 1, 4)

    assert [2, 3] = WorkerPartition.partitions(index) |> Enum.sort
    assert [5, 6] = WorkerPartition.workers(index) |> Enum.sort

    assert_dict %{5 => 2, 6 => 3}, w2p
    assert_dict %{2 => 5, 3 => 6}, p2w

    {w2p, p2w} = index
          |> WorkerPartition.update(2, 6)

    assert_dict %{6 => 2}, w2p
    assert_dict %{2 => 6}, p2w
  end

  defp assert_dict(value, dict) do
    assert value == Enum.into(dict, %{})
  end
end
