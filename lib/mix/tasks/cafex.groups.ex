defmodule Mix.Tasks.Cafex.Groups do
  use Mix.Task
  import Mix.Cafex

  alias Cafex.Connection

  @shortdoc "List/Describe kafka consumer groups"
  @recursive true
  @moduledoc """
  List/Describe kafka consumer groups

  ## Examples

      mix cafex.groups -b localhost:9091

  ## Command line options

    * `-b`, `--broker` - The Kafka broker in the form: `host1:port1`
  """
  @doc false
  def run(args) do
    Mix.Task.run "compile"
    Logger.configure level: :warn

    {cli_opts, groups, _} = OptionParser.parse(args, aliases: [b: :broker])
    broker = Keyword.get(cli_opts, :broker)

    unless broker do
      Mix.raise "Missed required arguments. Run `mix help #{Mix.Task.task_name(__MODULE__)}` for help"
    end

    %{servers: brokers} = parse_servers_url(broker)
    ensure_started

    {:ok, %{brokers: [%{host: host, port: port}|_] = brokers}} = Cafex.Kafka.Metadata.request(brokers, nil)

    if length(groups) > 0 do
      Enum.each(groups, fn group ->
        describe_group(group, host, port)
      end)
    else
      Enum.each(brokers, &list_groups/1)
    end

    :ok
  end

  defp describe_group(group, host, port) do
    {:ok, conn} = Connection.start(host, port)
    request = %Cafex.Protocol.GroupCoordinator.Request{group_id: group}
    {:ok, %{coordinator_host: host, coordinator_port: port}} = Cafex.Connection.request(conn, request)
    Connection.close(conn)
    {:ok, conn} = Connection.start(host, port)
    request = %Cafex.Protocol.DescribeGroups.Request{groups: [group]}
    {:ok, %{groups: [%{error: error,
                      group_id: ^group,
                      state: state,
                      protocol_type: protocol_type,
                      protocol: protocol,
                      members: members}]}} = Connection.request(conn, request)
    info_msg "Group: #{group}"
    info_msg "Error: #{inspect error}"
    info_msg "State: #{state}"
    info_msg "Protocol Type: #{protocol_type}"
    info_msg "Protocol: #{protocol}"
    info_msg "Members:"
    Enum.each(members, fn %{member_id: member_id,
                            client_id: client_id,
                            client_host: client_host,
                            member_metadata: {metadata_version, subscriptions, metadata_user_data},
                            member_assignment: {assignment_version, partitions, assignment_user_data}} ->
      info_msg "  MemberId: #{member_id}"
      info_msg "  ClientId: #{client_id}"
      info_msg "  ClientHost: #{client_host}"
      info_msg "  Member Metadata:"
      info_msg "    Version: #{metadata_version}"
      info_msg "    Subscriptions: #{inspect subscriptions}"
      info_msg "    UserData: #{metadata_user_data}"
      info_msg "  Member Assignment:"
      info_msg "    Version: #{assignment_version}"
      info_msg "    Partitions: #{inspect partitions}"
      info_msg "    UserData: #{assignment_user_data}"
      info_msg ""
    end)
  end

  defp list_groups(%{node_id: id, host: host, port: port}) do
    {:ok, conn} = Connection.start(host, port)
    request = %Cafex.Protocol.ListGroups.Request{}
    {:ok, response} = Connection.request(conn, request)
    info_msg "Broker: #{id} [#{host}:#{port}]"
    case response do
      %{error: :no_error, groups: groups} ->
        display groups
      %{error: error} ->
        error_msg "Error: #{inspect error}"
    end
    info_msg ""
  end

  defp display([]) do
    warn_msg "No groups in this broker"
  end
  defp display(groups) do
    Enum.each(groups, fn group ->
      info_msg group
    end)
  end
end
