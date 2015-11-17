defmodule Cafex do

  def start_topic(name, brokers, opts \\ []) do
    Cafex.Topic.Supervisor.start_topic(name, brokers, opts)
  end

  def start_producer(topic_name, brokers, opts \\ []) do
    Cafex.Producer.Supervisor.start_producer(topic_name, brokers, opts)
  end

  def produce(producer, value, opts \\ []) do
    Cafex.Producer.produce(producer, value, opts)
  end

  def fetch(topic_pid, partition, offset) when is_integer(partition)
                                           and is_integer(offset) do
    Cafex.Topic.Server.fetch topic_pid, partition, offset
  end

  @doc """
  Start consumer

  ## Options

  * zookeeper  - zookeeper config
  * handler    - handler module and initialize args
  """
  def start_consumer(name, topic_name, opts \\ []) do
    Cafex.Consumer.Supervisor.start_consumer(name, topic_name, opts)
  end

  def offline_consumer(name) when is_atom(name) do
    name |> Process.whereis |> offline_consumer
  end
  def offline_consumer(pid) when is_pid(pid) do
    Cafex.Consumer.Manager.offline pid
  end
end
