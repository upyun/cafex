defmodule Cafex do

  def start_topic(name, brokers, opts \\ []) do
    Cafex.Topic.Supervisor.start_topic(name, brokers, opts)
  end

  def start_producer(topic_pid, opts \\ []) do
    Cafex.Producer.Supervisor.start_producer(topic_pid, opts)
  end

  def produce(producer, value, opts \\ []) do
    Cafex.Producer.produce(producer, value, opts)
  end

  @doc """
  Start consumer

  ## Options

  * group_name  - consumer group name
  * zk_servers  - zookeeper servers
  * zk_path     - zookeeper root path
  """
  def start_consumer(topic_pid, opts \\ []) do
    Cafex.Consumer.Supervisor.start_consumer(topic_pid, opts)
  end
end
