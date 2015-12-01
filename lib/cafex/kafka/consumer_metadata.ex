defmodule Cafex.Kafka.ConsumerMetadata do
  use Cafex.Kafka.MetadataFsm

  require Logger
  alias Cafex.Protocol.ConsumerMetadata.Request

  def make_request(group) do
    %Request{consumer_group: group}
  end

  def do_request(state) do
    case try_fetch_consumer_metadata(state) do
      {:ok, result} -> %{state | result: result}
      {:error, error} -> %{state | error: error}
    end
  end

  defp try_fetch_consumer_metadata(state) do
    try_fetch_consumer_metadata(state, 10)
  end

  defp try_fetch_consumer_metadata(_state, 0) do
    {:error, :consumer_coordinator_not_available}
  end
  defp try_fetch_consumer_metadata(state, retries) when is_integer(retries) and retries > 0 do
    case send_request(state) do
      {:ok, %{error: :no_error, coordinator_host: host, coordinator_port: port}} ->
        {:ok, {host, port}}
      {:ok, %{error: :consumer_coordinator_not_available}} ->
        # We have to send a ConsumerMetadataRequest and retry with back off if
        # you receive a ConsumerCoordinatorNotAvailableCode returned as an
        # error.
        # See
        # [Kafka Error Codes](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes)
        # [issue on StackOverflow](http://stackoverflow.com/questions/28513744/kafka-0-8-2-consumermetadatarequest-always-return-consumercoordinatornotavailabl)
        Logger.warn "Try fetch consumer metadata error: :consumer_coordinator_not_available, retry"
        :timer.sleep(2000)
        try_fetch_consumer_metadata(state, retries - 1)
      {:ok, %{error: code}} ->
        Logger.error "Try fetch consumer metadata error: #{inspect code}"
        {:error, code}
      {:error, reason} ->
        Logger.error "Try fetch consumer metadata error: #{inspect reason}"
        {:error, reason}
    end
  end
end
