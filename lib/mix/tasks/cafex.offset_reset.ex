defmodule Mix.Tasks.Cafex.OffsetReset do
  use Mix.Task
  import Mix.Cafex
  import Cafex.Consumer.Util

  alias Cafex.Connection
  alias Cafex.Protocol.Offset
  alias Cafex.Protocol.OffsetCommit
  alias Cafex.Protocol.JoinGroup
  alias Cafex.Protocol.SyncGroup
  alias Cafex.Consumer.LoadBalancer

  @shortdoc "Reset consumer group's offsets to latest/earliest"
  @recursive true
  @moduledoc """
  Reset consumer group's offsets.
  You should really carful in using this task.

  ## Examples

      mix cafex.offset_reset -t topic -g group -b localhost:9092 -s strategy

  ## Command line options

    * `-t`, `--topic`     - Topic name
    * `-g`, `--group`     - Consumer group name
    * `-b`, `--broker`    - The Kafka broker in the form: `host1:port1`
    * `-s`, `--strategy`  - Reset to the `latest` of `earliest`
  """


  @doc false
  def run(args) do
    Mix.Task.run "compile"
    Logger.configure level: :warn

    {cli_opts, _, _} = OptionParser.parse args,
      aliases: [t: :topic, g: :group, b: :broker, s: :strategy]
    topic = Keyword.get(cli_opts, :topic)
    group = Keyword.get(cli_opts, :group)
    broker = Keyword.get(cli_opts, :broker)
    strategy = Keyword.get(cli_opts, :strategy)

    unless topic && group && broker && strategy do
      Mix.raise "Missed required arguments. Run `mix help #{Mix.Task.task_name(__MODULE__)}` for help"
    end

    strategy = strategy |> String.to_atom

    unless strategy in [:latest, :earliest] do
      Mix.raise "`-s` `--strategy` only support: [`:latest`, `:earliest`]"
    end

    %{servers: brokers} = parse_servers_url(broker)
    ensure_started

    {:ok, %{brokers: [%{host: host, port: port}|_]=brokers,
            topics: [%{partitions: partitions}]}} = Cafex.Kafka.Metadata.request(brokers, topic)
    brokers_map = Enum.map(brokers, fn %{node_id: node_id} = broker ->
      {node_id, broker}
    end) |> Enum.into(%{})

    hwm = partitions
    |> Enum.group_by(fn %{leader: leader} -> leader end)
    |> Enum.map(fn {k, v} ->
      partitions = Enum.map(v, fn %{partition_id: partition} ->
        {partition, strategy, 1}
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
      {partition, offset, ""}
    end)

    {host, port} = get_coordinator(group, host, port)

    {:ok, conn} = Connection.start(host, port)

    request = %JoinGroup.Request{group_id: group,
                                 member_id: "",
                                 session_timeout: 6000,
                                 protocol_type: "consumer",
                                 group_protocols: [{"cafex", {0, [topic], ""}}]}
    {:ok, %{error: :no_error,
            generation_id: generation_id,
            member_id: member_id,
            members: members}} = Connection.request(conn, request)
    if length(members) <= 0 do
      Mix.raise "Not the leader, can't commit offset!"
    end

    members = Enum.map(members, fn {member_id, {0, [^topic], user_data}} ->
      {member_id, decode_partitions(user_data)}
    end)
    info_msg "Assigned members are: #{inspect members}"

    rebalanced = LoadBalancer.rebalance(members, length(partitions))
    group_assignment = Enum.map(rebalanced, fn {member_id, assignment} ->
      member_assignment = {0, [{topic, assignment}], nil}
      {member_id, member_assignment}
    end)

    request = %SyncGroup.Request{group_id: group,
                                 member_id: member_id,
                                 generation_id: generation_id,
                                 group_assignment: group_assignment}

    {:ok, %{error: :no_error}} = Connection.request(conn, request)

    request = %OffsetCommit.Request{api_version: 1,
                                    consumer_group: group,
                                    consumer_id: member_id,
                                    consumer_group_generation_id: generation_id,
                                    topics: [{topic, hwm}]}
    {:ok, %{topics: [{^topic, partition_errors}]}} = Connection.request(conn, request)
    case Enum.find(partition_errors, fn {_, e} -> e != :no_error end) do
      nil    -> success_msg "OffsetReset topic '#{topic}' with strategy: #{strategy} success."
      {p, e} -> error_msg "OffsetReset topic '#{topic}' partition: #{p} with #{strategy} offset error: #{inspect e}"
    end

    :ok = Connection.close(conn)
  end

end
