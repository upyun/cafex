defmodule Cafex do

  @type server :: {host :: String.t, port :: 0..65535}
  @type broker :: server
  @type client_id :: String.t
  @type zookeeper :: [zookeeper_option]
  @type zookeeper_option :: {:servers, [server]} |
                            {:path, String.t} |
                            {:timeout, non_neg_integer}

  def start_topic(name, brokers, opts \\ []) do
    Cafex.Supervisor.start_topic(name, brokers, opts)
  end

  @doc """
  Start a producer.

  Read `Cafex.Producer` for more details.
  """
  @spec start_producer(topic_name :: String.t, opts :: Cafex.Producer.options) :: Supervisor.on_start_child
  def start_producer(topic_name, opts \\ []) do
    Cafex.Supervisor.start_producer(topic_name, opts)
  end

  @doc """
  Produce message to kafka server in the synchronous way.

  See `Cafex.Producer.produce/3`
  """
  def produce(producer, value, opts \\ []) do
    Cafex.Producer.produce(producer, value, opts)
  end

  @doc """
  Produce message to kafka server in the asynchronous way.

  See `Cafex.Producer.produce/3`
  """
  def async_produce(producer, value, opts \\ []) do
    Cafex.Producer.async_produce(producer, value, opts)
  end

  def fetch(topic_pid, partition, offset) when is_integer(partition)
                                           and is_integer(offset) do
    Cafex.Topic.Server.fetch topic_pid, partition, offset
  end

  @doc """
  Start a consumer.

  Read `Cafex.Consumer.Manager` for more details.
  """
  @spec start_consumer(name :: atom, topic_name :: String.t, Cafex.Consumer.Manager.options) :: Supervisor.on_start_child
  def start_consumer(name, topic_name, opts \\ []) do
    Cafex.Supervisor.start_consumer(name, topic_name, opts)
  end

  @spec offline_consumer(name :: atom | pid) :: :ok
  def offline_consumer(name) when is_atom(name) do
    name |> Process.whereis |> offline_consumer
  end
  def offline_consumer(pid) when is_pid(pid) do
    Cafex.Consumer.Manager.offline pid
  end
end
