defmodule Mix.Tasks.Cafex.OffsetFetch do
  use Mix.Task
  import Mix.Cafex

  alias Cafex.Protocol.Offset
  alias Cafex.Protocol.OffsetFetch

  @shortdoc "Fetch consumer group offsets"
  @recursive true
  @moduledoc """
  Fetch consumer group offsets

  ## Examples

      mix cafex.offset_fetch -t topic -g group -b localhost:9092

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

    {:ok, %{brokers: [%{host: host, port: port}|_]=brokers,
            topics: [%{partitions: partitions}]}} = Cafex.Kafka.Metadata.request(brokers, topic)
    partition_ids = Enum.map(partitions, fn %{partition_id: id} -> id end)
    brokers_map = Enum.map(brokers, fn %{node_id: node_id} = broker ->
      {node_id, broker}
    end) |> Enum.into(%{})

    hwm = partitions
    |> Enum.group_by(fn %{leader: leader} -> leader end)
    |> Enum.map(fn {k, v} ->
      partitions = Enum.map(v, fn %{partition_id: partition} ->
        {partition, :latest, 1}
      end)
      request = %Offset.Request{topics: [{topic, partitions}]}
      %{host: host, port: port} = brokers_map[k]
      {:ok, conn} = Connection.start(host, port)
      {:ok, %{offsets: [{^topic, partitions}]}} = Connection.request(conn, request)
      Connection.close(conn)
      partitions
    end)
    |> List.flatten
    |> Enum.map(fn %{partition: partition, offsets: [offset]} ->
      {partition, offset}
    end)
    |> Enum.into(%{})

    {host, port} = get_coordinator(group, host, port)
    {:ok, conn} = Connection.start(host, port)
    request = %OffsetFetch.Request{api_version: 1, consumer_group: group, topics: [{topic, partition_ids}]}
    {:ok, %{topics: [{^topic, partitions}]}} = Connection.request(conn, request)
    :ok = Connection.close(conn)

    success_msg "Topic: #{topic}\n"
    partitions
    |> Enum.sort
    |> Enum.each(fn {id, offset, meta, error} ->
      info_msg "partition: #{id}\t offset: #{offset}\thwmOffset: #{hwm[id]}\t " <>
               "lag: #{hwm[id] - offset}\t meta: #{inspect meta}\t error: #{inspect error}"
    end)

    :ok
  end
end
