defmodule Cafex do

  def start_topic(name, brokers, opts \\ []) do
    Cafex.Topic.Supervisor.start_topic(name, brokers, opts)
  end

  def start_producer(topic, opts \\ []) do
    Cafex.Producer.Supervisor.start_producer(topic, opts)
  end

  def produce(producer, value, opts \\ []) do
    Cafex.Producer.produce(producer, value, opts)
  end
end
