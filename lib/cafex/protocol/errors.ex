defmodule Cafex.Protocol.Errors do
  @moduledoc """
  Use atom to represent the numeric error codes.

  For more details about errors, read [A Guide To The Kafka Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes)
  """

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

  # Generate
  # @type t :: :no_error
  #          | :offset_out_of_range
  #          | ...

  @type t :: unquote(Map.values(@error_map) ++ [:unknown_error]
             |> List.foldr([], fn
               v, [] ->
                 v
               v, acc ->
                 {:|, [], [v, acc]}
             end))

  @spec error(error_code :: integer) :: t
  def error(error_code)

  for {k, v} <- @error_map do
    def error(unquote(k)), do: unquote(v)
  end

  def error(_), do: :unknown_error
end
