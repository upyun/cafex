defmodule Cafex.Protocol do
  @client_id "cafex"

  alias KafkaEx.Protocol.Metadata
  alias KafkaEx.Protocol.Produce

  def create_metadata_request(correlation_id, topic) do
    Metadata.create_request(correlation_id, @client_id, topic)
  end

  def parse_metadata_response(data) do
    Metadata.parse_response(data)
  end

  def create_produce_request(correlation_id, topic, partition, value, required_acks, timeout) do
    Produce.create_request(correlation_id, @client_id, topic, partition, value, nil, required_acks, timeout)
  end

  def parse_produce_response(data) do
    Produce.parse_response(data)
  end

  def error(code), do: KafkaEx.Protocol.error(code)
end
