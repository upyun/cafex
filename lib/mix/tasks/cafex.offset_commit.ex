defmodule Mix.Tasks.Cafex.OffsetCommit do
  use Mix.Task
  import Mix.Cafex
  import Cafex.Consumer.Util

  alias Cafex.Connection
  alias Cafex.Protocol.OffsetCommit
  alias Cafex.Protocol.JoinGroup
  alias Cafex.Protocol.SyncGroup

  @shortdoc "Commit consumer group offset"
  @recursive true
  @moduledoc """
  Commit consumer group offsets.
  You should really carful in using this task.

  ## Examples

      mix cafex.offset_commit -t topic -g group -b localhost:9091 -o offset -p partition

  ## Command line options

    * `-t`, `--topic`     - Topic name
    * `-g`, `--group`     - Consumer group name
    * `-b`, `--broker`    - The Kafka broker in the form: `host1:port1`
    * `-o`, `--offset`    - Offset to be committed
    * `-p`, `--partition` - Partition associated with the offset
  """


  @doc false
  def run(args) do
    Mix.Task.run "compile"
    Logger.configure level: :warn

    {cli_opts, _, _} = OptionParser.parse args,
      switches: [partition: :integer, offset: :integer],
      aliases: [t: :topic, g: :group, b: :broker, o: :offset, p: :partition]
    topic = Keyword.get(cli_opts, :topic)
    group = Keyword.get(cli_opts, :group)
    broker = Keyword.get(cli_opts, :broker)
    offset = Keyword.get(cli_opts, :offset)
    partition = Keyword.get(cli_opts, :partition)

    unless topic && group && broker && offset && partition do
      Mix.raise "Missed required arguments. Run `mix help #{Mix.Task.task_name(__MODULE__)}` for help"
    end

    %{servers: brokers} = parse_servers_url(broker)
    ensure_started

    {:ok, %{brokers: [%{host: host, port: port}|_]}} = Cafex.Kafka.Metadata.request(brokers, topic)

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

    member_assignment = {0, [{topic, [partition]}], nil}
    request = %SyncGroup.Request{group_id: group,
                                 member_id: member_id,
                                 generation_id: generation_id,
                                 group_assignment: [{member_id, member_assignment}]}

    {:ok, %{error: :no_error}} = Connection.request(conn, request)

    request = %OffsetCommit.Request{api_version: 1,
                                    consumer_group: group,
                                    consumer_id: member_id,
                                    consumer_group_generation_id: generation_id,
                                    topics: [{topic, [{partition, offset, ""}]}]}
    {:ok, %{topics: [{^topic, partition_errors}]}} = Connection.request(conn, request)
    case Enum.find(partition_errors, fn {_, e} -> e != :no_error end) do
      nil -> success_msg "OffsetCommit topic '#{topic}' partition: #{partition} with offset: #{offset} success."
      {p, e} -> error_msg "OffsetCommit topic '#{topic}' partition: #{p} with offset: #{offset} error: #{inspect e}"
    end

    :ok = Connection.close(conn)
  end
end
