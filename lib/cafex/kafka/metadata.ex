defmodule Cafex.Kafka.Metadata do
  use Cafex.Kafka.MetadataFsm

  require Logger

  alias Cafex.Protocol.Metadata.Request

  def make_request() do
    %Request{topics: []}
  end

  def make_request(nil), do: make_request
  def make_request(topic) do
    %Request{topics: [topic]}
  end

  def extract_metadata(metadata) do
    brokers = metadata.brokers |> Enum.map(fn b -> {b.node_id, {b.host, b.port}} end)
                               |> Enum.into(%{})

    [%{name: name, partitions: partitions}] = metadata.topics

    leaders = partitions |> Enum.map(fn p -> {p.partition_id, p.leader} end)
                         |> Enum.into(%{})

    %{name: name, brokers: brokers, leaders: leaders, partitions: length(partitions)}
  end

  def do_request(%{conn: conn,
                   feed_brokers: [broker|rest],
                   dead_brokers: deads} = state) do
    case send_request(state) do
      {:ok, %{topics: [%{error: :unknown_topic_or_partition}]}} ->
        %{state | error: :unknown_topic_or_partition}
      {:ok, %{topics: [%{error: :leader_not_available}]}} ->
        :timer.sleep(100)
        do_request(state)
      {:ok, metadata} ->
        %{state | result: metadata}
      {:error, reason} ->
        Logger.error "Failed to get metadata from: #{inspect conn}, reason: #{inspect reason}"
        %{state | feed_brokers: rest, dead_brokers: [broker|deads]}
        |> close_conn
        |> do_request
    end
  end
end
