defmodule Cafex.Kafka.GroupCoordinator do
  use Cafex.Kafka.MetadataFsm

  require Logger
  alias Cafex.Protocol.GroupCoordinator.Request

  def make_request(group) do
    %Request{group_id: group}
  end

  def do_request(state) do
    case try_fetch_group_coordinator(state) do
      {:ok, result} -> %{state | result: result}
      {:error, error} -> %{state | error: error}
    end
  end

  defp try_fetch_group_coordinator(state) do
    try_fetch_group_coordinator(state, 10)
  end

  defp try_fetch_group_coordinator(_state, 0) do
    {:error, :group_coordinator_not_available}
  end
  defp try_fetch_group_coordinator(state, retries) when is_integer(retries) and retries > 0 do
    case send_request(state) do
      {:ok, %{error: :no_error, coordinator_host: host, coordinator_port: port}} ->
        {:ok, {host, port}}
      {:ok, %{error: :group_coordinator_not_available}} ->
        # We have to send a GroupCoordinatorRequest and retry with back off if
        # you receive a ConsumerCoordinatorNotAvailableCode returned as an
        # error.
        # See
        # [Kafka Error Codes](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes)
        # [issue on StackOverflow](http://stackoverflow.com/questions/28513744/kafka-0-8-2-consumermetadatarequest-always-return-consumercoordinatornotavailabl)
        Logger.warn "Try fetch group coordinator error: :group_coordinator_not_available, retry"
        :timer.sleep(2000)
        try_fetch_group_coordinator(state, retries - 1)
      {:ok, %{error: code}} ->
        Logger.error "Try fetch group coordinator error: #{inspect code}"
        {:error, code}
      {:error, reason} ->
        Logger.error "Try fetch group coordinator error: #{inspect reason}"
        {:error, reason}
    end
  end
end
