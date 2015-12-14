defmodule Mix.Tasks.Cafex.Offsetfetch do
  use Mix.Task
  import Mix.Cafex

  @shortdoc "Fetch consumer group offsets"
  @recursive true
  @moduledoc """
  Fetch consumer group offsets

  ## Examples

      mix cafex.offsetfetch -t topic -g group -b localhost:9091

  ## Command line options

    * `-t`, `--topic`  - Topic name
    * `-g`, `--group`  - Consumer group name
    * `-b`, `--broker` - The Kafka broker in the form: `host1:port1`
  """

  alias Cafex.Connection

  @doc false
  def run(args) do
    Mix.Task.run "compile"
    Logger.configure level: :warn

    {cli_opts, _, _} = OptionParser.parse(args, aliases: [t: :topic, g: :group, b: :broker])
    topic = Keyword.get(cli_opts, :topic)
    group = Keyword.get(cli_opts, :group)
    broker = Keyword.get(cli_opts, :broker)

    unless topic && group && broker do
      Mix.raise "Missed required arguments. Run `mix help #{Mix.Task.task_name(__MODULE__)}` for help"
    end

    %{servers: brokers} = parse_servers_url(broker)
    ensure_started

    {:ok, %{brokers: brokers, topics: [%{partitions: partitions}]}} = Cafex.Kafka.Metadata.request(brokers, topic)
    partitions = Enum.map(partitions, fn %{partition_id: id} ->
        id
    end)

    request = %Cafex.Protocol.GroupCoordinator.Request{group_id: group}
    [%{host: host, port: port}|_] = brokers
    {:ok, conn} = Connection.start(host, port)
    {:ok, %{coordinator_host: host, coordinator_port: port}} = Cafex.Connection.request(conn, request)
    Connection.close(conn)
    {:ok, conn} = Connection.start(host, port)
    request = %Cafex.Protocol.OffsetFetch.Request{api_version: 1, consumer_group: group, topics: [{topic, partitions}]}
    {:ok, %{topics: [{^topic, partitions}]}} = Connection.request(conn, request)

    success_msg "Topic: #{topic}\n"
    partitions
    |> Enum.sort
    |> Enum.each(fn {id, offset, meta, error} ->
      info_msg "partition: #{id}\t offset: #{offset}\t meta: #{inspect meta}\t error: #{inspect error}"
    end)

    :ok
  end
end
