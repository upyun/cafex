defmodule Cafex.Consumer.WorkerPartition do
  @moduledoc """
  The partitions and their corresponding worker pids

  ```
  {
    #HashDict<[worker_pid => partition]>,
    #HashDict<[partition => worker_pid]>
  }
  ```

  """

  def new do
    {HashDict.new, HashDict.new}
  end

  def partitions({_w2p, p2w}) do
    HashDict.keys(p2w)
  end

  def workers({w2p, _p2w}) do
    HashDict.keys(w2p)
  end

  def partition({w2p, _p2w}, worker) do
    HashDict.get(w2p, worker)
  end

  def worker({_w2p, p2w}, partition) do
    HashDict.get(p2w, partition)
  end

  def update({w2p, p2w} = index, partition, worker) do
    old_worker = worker(index, partition)
    old_partition = partition(index, worker)

    w2p = w2p
        |> HashDict.delete(old_worker)
        |> HashDict.put(worker, partition)

    p2w = p2w
        |> HashDict.delete(old_partition)
        |> HashDict.put(partition, worker)

    {w2p, p2w}
  end

  def delete({w2p, p2w}, partition, worker) do
    {HashDict.delete(w2p, worker), HashDict.delete(p2w, partition)}
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
