defmodule Cafex.Protocol do
  @produce_request           0
  @fetch_request             1
  @offset_request            2
  @metadata_request          3
  @offset_commit_request     8
  @offset_fetch_request      9
  @consumer_metadata_request 10

  @api_version  0

  defp api_key(:produce) do
    @produce_request
  end

  defp api_key(:fetch) do
    @fetch_request
  end

  defp api_key(:offset) do
    @offset_request
  end

  defp api_key(:metadata) do
    @metadata_request
  end

  defp api_key(:offset_commit) do
    @offset_commit_request
  end

  defp api_key(:offset_fetch) do
    @offset_fetch_request
  end

  defp api_key(:consumer_metadata) do
    @consumer_metadata_request
  end

  def create_request(type, correlation_id, client_id) do
    << api_key(type) :: 16, @api_version :: 16, correlation_id :: 32,
       byte_size(client_id) :: 16, client_id :: binary >>
  end

  @error_map %{
     0 => :no_error,
     1 => :offset_out_of_range,
     2 => :invalid_message,
     3 => :unknown_topic_or_partition,
     4 => :invalid_message_size,
     5 => :leader_not_available,
     6 => :not_leader_for_partition,
     7 => :request_timed_out,
     8 => :broker_not_available,
     9 => :replica_not_available,
    10 => :message_size_too_large,
    11 => :stale_controller_epoch,
    12 => :offset_metadata_too_large,
    14 => :offset_loads_in_progress,
    15 => :consumer_coordinator_not_available,
    16 => :not_coordinator_for_consumer
  }

  def error(err_no) do
    case err_no do
      -1 -> :unknown_error
      _  -> @error_map[err_no] || err_no
    end
  end

  @client_id "cafex"

  def default_client_id, do: @client_id

  def create_message_set(value, key \\ nil) do
    message = create_message(value, key)
    << 0 :: 64, byte_size(message) :: 32 >> <> message
  end

  def create_message(value, key \\ nil) do
    sub = << 0 :: 8, 0 :: 8 >> <> bytes(key) <> bytes(value)
    crc = :erlang.crc32(sub)
    << crc :: 32 >> <> sub
  end

  def bytes(nil), do: << -1 :: 32 >>

  def bytes(data) do
    case byte_size(data) do
      0 -> << -1 :: 32 >>
      size -> << size :: 32, data :: binary >>
    end
  end

  def parse_message_set([], << >>) do
    {:ok, [], nil}
  end

  def parse_message_set([last|_] = list, << >>) do
    {:ok, Enum.reverse(list), last.offset}
  end

  def parse_message_set(list, << offset :: 64, msg_size :: 32, msg_data :: size(msg_size)-binary, rest :: binary >>) do
    {:ok, message} = parse_message(msg_data)
    parse_message_set([Map.put(message, :offset, offset)|list], rest)
  end

  def parse_message_set([], _) do
    {:ok, [], nil}
  end

  def parse_message_set([last|_] = list, _) do
    {:ok, Enum.reverse(list), last.offset}
  end

  def parse_message(<< crc :: 32, _magic :: 8, attributes :: 8, rest :: binary>>) do
    parse_key(crc, attributes, rest)
  end

  def parse_key(crc, attributes, << -1 :: 32-signed, rest :: binary >>) do
    parse_value(crc, attributes, nil, rest)
  end

  def parse_key(crc, attributes, << key_size :: 32, key :: size(key_size)-binary, rest :: binary >>) do
    parse_value(crc, attributes, key, rest)
  end

  def parse_value(crc, attributes, key, << -1 :: 32-signed >>) do
    {:ok, %{:crc => crc, :attributes => attributes, :key => key, :value => nil}}
  end

  def parse_value(crc, attributes, key, << value_size :: 32, value :: size(value_size)-binary >>) do
    {:ok, %{:crc => crc, :attributes => attributes, :key => key, :value => value}}
  end
end
