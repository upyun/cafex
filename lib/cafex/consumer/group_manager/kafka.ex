defmodule Cafex.Consumer.GroupManager.Kafka do
  @moduledoc """
  The implementation of Kafka Client-side Assignment Proposal

  Supported Kafka 0.9.x or above.

  See [Kafka Client-side Assignment Proposal](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal) to read more.
  """
  @behaviour :gen_fsm

  require Logger

  import Cafex.Consumer.Util
  alias Cafex.Connection
  alias Cafex.Consumer.LoadBalancer
  alias Cafex.Protocol.JoinGroup
  alias Cafex.Protocol.SyncGroup
  alias Cafex.Protocol.LeaveGroup
  alias Cafex.Protocol.Heartbeat.Request, as: HeartbeatRequest
  alias Cafex.Kafka.Heartbeat

  @protocol_type "consumer"
  @protocol_name "cafex"
  @protocol_version 0
  @timeout 6000

  defmodule State do
    @moduledoc false
    defstruct [:manager,
               :topic,
               :group_id,
               :partitions,
               :conn,
               :coordinator,
               :leader?,
               :member_id,
               :assignment,
               :generation_id,
               :offset_manager,
               :members,
               :session_timeout,
               :heartbeat]
  end

  #
  # API
  #

  def start_link(manager, topic, group, partitions, opts \\ []) do
    :gen_fsm.start_link __MODULE__, [manager, topic, group, partitions, opts], []
  end

  def stop(pid) do
    :gen_fsm.sync_send_all_state_event pid, :stop
  end

  #
  # callbacks
  #

  @doc false
  def init([manager, topic, group, partitions, opts]) do
    # TODO
    timeout = Keyword.get(opts, :timeout) || @timeout
    coordinator = Keyword.get(opts, :group_coordinator)
    offset_manager = Keyword.get(opts, :offset_manager)
    state = %State{manager: manager,
                   coordinator: coordinator,
                   offset_manager: offset_manager,
                   topic: topic,
                   group_id: group,
                   leader?: false,
                   member_id: "",
                   session_timeout: timeout,
                   partitions: partitions}
                 |> start_conn
    {:ok, :election, state, 0}
  end

  @doc false
  def election(:timeout, state) do
    state = do_election(state)
    {:next_state, :rebalance, state, 0}
  end

  @doc false
  def rebalance(:timeout, %{leader?: true, group_id: group, topic: topic, members: members, partitions: partitions} = state) do
    Logger.info "I am the leader of the group [#{group}] now"
    rebalanced = LoadBalancer.rebalance(members, partitions)
    group_assignment = Enum.map(rebalanced, fn {member_id, assignment} ->
      member_assignment = {@protocol_version, [{topic, assignment}], nil}
      {member_id, member_assignment}
    end)
    {:ok, assignment} = do_sync_group(group_assignment, state)
    state = start_heartbeat(%{state | assignment: assignment, members: rebalanced})
    notify_manager(state)
    {:next_state, :idle, state}
  end
  def rebalance(:timeout, state) do
    {:ok, assignment} = do_sync_group([], state)
    state = start_heartbeat(%{state | assignment: assignment})
    notify_manager(state)
    {:next_state, :idle, state}
  end

  @doc false
  def handle_sync_event(:stop, _from, _state_name, state_data) do
    {:stop, :normal, :ok, state_data}
  end

  @doc false
  def handle_event(event, state_name, state_data) do
      {:stop, {:bad_event, state_name, event}, state_data}
  end

  @doc false
  def handle_info({:no_heartbeat, _reason}, :idle, state) do
    {:next_state, :election, %{state | heartbeat: nil}, 0}
  end

  @doc false
  def code_change(_old, state_name, state_data, _extra) do
    {:ok, state_name, state_data}
  end

  @doc false
  def terminate(_reason, _state_name, state_data) do
    leave(state_data) |> close_conn
    :ok
  end

  #
  # Internal functions
  #

  defp start_conn(%{coordinator: {host, port}} = state) do
    {:ok, pid} = Connection.start_link(host, port)
    %{state | conn: pid}
  end

  defp close_conn(%{conn: nil} = state), do: state
  defp close_conn(%{conn: pid} = state) do
    if Process.alive?(pid) do
      Connection.close(pid)
    end
    %{state | conn: nil}
  end

  defp notify_manager(%{manager: pid, assignment: assignment} = state) do
    send pid, {:rebalanced, assignment}
    state
  end

  defp leave(%{group_id: group, member_id: member_id} = state) do
    Logger.debug "Leave consumer group: #{inspect group}, member_id: #{inspect member_id}"
    state |> stop_heartbeat |> do_leave
  end

  defp do_leave(%{conn: conn} = state) do
    request =
    Map.take(state, [:group_id, :member_id])
    |> (&(struct(LeaveGroup.Request, &1))).()

    Connection.request(conn, request)
    state
  end

  defp do_election(%{conn: conn,
                     topic: topic,
                     assignment: assignment,
                     offset_manager: offset_manager} = state) do
    protocol_metadata = {@protocol_version, [topic], encode_partitions(assignment)}
    group_protocols = [{@protocol_name, protocol_metadata}]

    request =
    Map.take(state, [:group_id, :member_id, :session_timeout])
    |> Map.put(:protocol_type, @protocol_type)
    |> Map.put(:group_protocols, group_protocols)
    |> (&(struct(JoinGroup.Request, &1))).()

    case Connection.request(conn, request) do
      {:ok, %{error: :no_error,
              generation_id: generation_id,
              member_id: member_id,
              members: members}} ->
        leader? = length(members) > 0
        members = Enum.map(members, fn {member_id, {_version, [_topic], user_data}} ->
          {member_id, decode_partitions(user_data)}
        end)
        Cafex.Consumer.OffsetManager.update_generation_id(offset_manager, member_id, generation_id)
        %{state | leader?: leader?, member_id: member_id, generation_id: generation_id, members: members}
      {:ok, %{error: error}} -> throw {:error, error}
      error -> throw error
    end
  end

  defp do_sync_group(group_assignment, %{conn: conn, topic: topic} = state) do
    request =
    Map.take(state, [:group_id, :member_id, :generation_id])
    |> Map.put(:group_assignment, group_assignment)
    |> (&(struct(SyncGroup.Request, &1))).()

    case Cafex.Connection.request(conn, request) do
      {:ok, %{error: :no_error, member_assignment: {_version, [{^topic, assignment}], _user_data}}} ->
        {:ok, assignment}
      {:ok, %{error: error}} ->
        # TODO handle kafka response error
        Logger.error "SyncGroup request error: #{inspect error}"
        {:error, error}
      error ->
        Logger.error "SyncGroup request error: #{inspect error}"
        error
    end
  end

  defp start_heartbeat(%{conn: conn, session_timeout: timeout} = state) do
    request =
    Map.take(state, [:group_id, :member_id, :generation_id])
    |> (&(struct(HeartbeatRequest, &1))).()

    {:ok, pid} = Heartbeat.start_link(self, conn, request, div(timeout, 3) * 2)
    %{state | heartbeat: pid}
  end

  defp stop_heartbeat(%{heartbeat: nil} = state), do: state
  defp stop_heartbeat(%{heartbeat: pid} = state) do
    if Process.alive?(pid) do
      Heartbeat.stop(pid)
    end
    %{state | heartbeat: nil}
  end
end
