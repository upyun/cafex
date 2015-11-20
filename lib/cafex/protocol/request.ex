defprotocol Cafex.Protocol.Request do
  @moduledoc """
  The Cafex.Protocol.Request protocol used by `Cafex.Protocol` module.

  Read [A Guide To The Kafka Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Requests) for more details.
  """

  @doc """
  Return the API Key of the request type
  """
  def api_key(req)

  @doc """
  Return the per-api based version number of a API request
  """
  def api_version(req)

  @doc """
  Kafka server will reply to every request except produce request if the required_acks is 0 for now.

  `Cafex.Protocol` module will call this function on every request to check if server will reply or not.
  """
  def has_response?(req)

  @doc """
  Encode the request struct to binary
  """
  def encode(req)
end
