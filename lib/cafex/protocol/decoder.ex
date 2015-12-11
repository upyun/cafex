defmodule Cafex.Protocol.Decoder do
  @moduledoc """
  Kafka server response decoder implementation specification.
  """

  use Behaviour

  @decoders [ Metadata,
              Produce,
              Fetch,
              Offset,
              ConsumerMetadata,
              OffsetCommit,
              OffsetFetch,
              JoinGroup,
              SyncGroup,
              LeaveGroup,
              Heartbeat,
              ListGroups,
              DescribeGroups]

  @typedoc """
  The `decode` function in each decoder will return there own response struct

  See `Cafex.Protocol`
  """
  @type response :: unquote(Enum.map(@decoders, fn d ->
    quote do: Cafex.Protocol.unquote(d).Response.t
  end) |> List.foldr([], fn
    v, []  -> quote do: unquote(v)
    v, acc -> quote do: unquote(v) | unquote(acc)
  end))

  @typedoc """
  The modules which implement the `Decoder` interface

  See `Cafex.Protocol`
  """
  @type decoder :: unquote(Enum.map(@decoders, fn d ->
    quote do: Cafex.Protocol.unquote(d)
  end) |> List.foldr([], fn
    v, []  -> quote do: unquote(v)
    v, acc -> quote do: unquote(v) | unquote(acc)
  end))

  @doc """
  Decode the response message in the Kafka server response
  """
  defcallback decode(binary) :: response

end
