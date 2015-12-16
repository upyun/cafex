defmodule Mix.Tasks.Cafex.Topic do
  use Mix.Task
  import Mix.Cafex

  alias Cafex.Kafka.Metadata

  @shortdoc "Print topic metadata from Kafka."
  @recursive true
  @moduledoc """
  This is a tool that reads topic metadata from Kafka and outputs it to standard output.

  ## Examples

      mix cafex.topic topic_name --brokers 192.168.99.100:9091

  ## Command line options

    * `-l`, `--list`   - List topics
    * `-b`, `--broker` - The Kafka broker in the form: `host1:port1`
  """

  @client_id "cafex_mix"

  @doc false
  def run(args) do
    Mix.Task.run "compile"
    Logger.configure level: :warn

    {cli_opts, args, _} = OptionParser.parse(args, aliases: [b: :broker, l: :list])
    list = Keyword.get(cli_opts, :list)
    broker = Keyword.get(cli_opts, :broker)

    topic = case args do
      [t|_] -> t
      _ -> nil
    end

    args = cond do
      list == true ->
        nil
      topic == nil ->
        Mix.raise "`--topic topic` or `--list` must be given. Run `mix help #{Mix.Task.task_name __MODULE__}` for help"
      true ->
        topic
    end

    %{servers: brokers} = parse_servers_url(broker)
    ensure_started

    {:ok, metadata} = Metadata.request(brokers, args)

    success_msg "Brokers:"
    Enum.map(metadata.brokers, fn %{node_id: id, host: host, port: port} ->
      info_msg "Node ID: #{id}\tHost: #{host}\tPort: #{port}"
    end)
    info_msg ""

    cond do
      list == true ->
        list_topics(metadata.topics)
      true ->
        topic = Enum.find(metadata.topics, fn %{name: name} ->
          name == topic
        end)
        if topic do
          describe_topic(topic)
        else
          :ok
        end
    end

    :ok
  end

  defp list_topics(topics) do
    success_msg "Topics:"

    topics
    |> Enum.sort
    |> Enum.map(fn %{error: _error, name: name, partitions: _partitions} ->
      info_msg "#{name}"
    end)
  end

  defp describe_topic(%{error: _error, name: name, partitions: partitions}) do
    info_msg "Topic: #{name}"

    partitions
    |> Enum.sort(fn(%{partition_id: id1}, %{partition_id: id2}) ->
      id1 < id2
    end)
    |> Enum.map(fn %{error: err, partition_id: id, leader: leader, replicas: replicas, isrs: isrs} ->
        info_msg "partition: #{id}\tleader: #{leader}\treplicas: #{inspect replicas}\tisrs: #{inspect isrs}\terror: #{inspect err}"
    end)
  end
end
