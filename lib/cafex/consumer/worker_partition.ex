defmodule Cafex.Consumer.WorkerPartition do
  @moduledoc """
  The partitions and their corresponding worker pids

  ```
  {
    #Map{worker_pid => partition},
    #Map{partition => worker_pid}
  }
  ```

  """

  def new do
    {%{}, %{}}
  end

  def partitions({_w2p, p2w}) do
    Map.keys(p2w)
  end

  def workers({w2p, _p2w}) do
    Map.keys(w2p)
  end

  def partition({w2p, _p2w}, worker) do
    Map.get(w2p, worker)
  end

  def worker({_w2p, p2w}, partition) do
    Map.get(p2w, partition)
  end

  def update({w2p, p2w} = index, partition, worker) do
    old_worker = worker(index, partition)
    old_partition = partition(index, worker)

    w2p = w2p
        |> Map.delete(old_worker)
        |> Map.put(worker, partition)

    p2w = p2w
        |> Map.delete(old_partition)
        |> Map.put(partition, worker)

    {w2p, p2w}
  end

  def delete({w2p, p2w}, partition, worker) do
    {Map.delete(w2p, worker), Map.delete(p2w, partition)}
  end

  def delete_by_worker(index, worker) do
    partition = partition(index, worker)
    delete(index, partition, worker)
  end

  def delete_by_partition(index, partition) do
    worker = worker(index, partition)
    delete(index, partition, worker)
  end
end
