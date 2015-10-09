defprotocol Cafex.Protocol.Request do
  @doc """
  Return the API Key of the request type

  See: https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Requests
  """
  def api_key(req)

  @doc """
  Encode the request
  """
  def encode(req)
end
