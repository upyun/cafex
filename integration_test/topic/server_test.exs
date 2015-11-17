defmodule Cafex.Integration.Topic.ServerTest do
  use ExUnit.Case, async: true

  alias Cafex.Topic.Server
  alias Cafex.Protocol.Offset
  alias Cafex.Protocol.Fetch

  @default_topic "cafex_integration_test_topic"

  setup do
    topic = Application.get_env(:cafex, :topic, @default_topic)
    brokers = case Application.get_env(:cafex, :brokers) do
      nil -> Process.exit(self, "must set brokers for Integration test")
      brokers -> brokers
    end

    {:ok, pid} = Cafex.start_topic topic, brokers
    {:ok, topic_pid: pid, topic_name: topic, brokers: brokers}
  end

  test "Topic server", context do
    pid = context[:topic_pid]
    brokers = context[:brokers]
    topic_name = context[:topic_name]

    metadata = Server.metadata(pid)
    assert is_map(metadata)
    assert Map.has_key?(metadata, :name)
    assert Map.has_key?(metadata, :brokers)
    assert Map.has_key?(metadata, :leaders)
    assert Map.has_key?(metadata, :partitions)
    assert topic_name, metadata.name
    assert Enum.sort(brokers) == metadata.brokers |> HashDict.to_list |> Keyword.values |> Enum.sort

    Enum.map(0..metadata.partitions - 1, fn partition ->
      assert {:ok, %Offset.Response{offsets: [{^topic_name, [%{error: :no_error, offsets: [offset], partition: ^partition}]}]}} = Server.offset(pid, partition, :earliest, 1)
      assert {:ok, %Fetch.Response{topics: [{^topic_name, [%{error: :no_error, hwm_offset: _, messages: _messages, partition: ^partition}]}]}} = Server.fetch(pid, partition, offset)
    end)
  end
end
