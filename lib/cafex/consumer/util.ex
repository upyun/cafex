defmodule Cafex.Consumer.Util do

  @doc """
  Decode partitioins string stored outside.

  ## Example

      iex> decode_partitions(nil)
      []
      iex> decode_partitions("")
      []
      iex> decode_partitions("[1,2,3,4,5]")
      [1,2,3,4,5]
      iex> decode_partitions("[1,2,3,]")
      [1,2,3]
      iex> decode_partitions("[1,2,3")
      [1,2,3]
      iex> decode_partitions("1,2,3")
      [1,2,3]
  """
  def decode_partitions(nil), do: []
  def decode_partitions(""), do: []
  def decode_partitions(value) do
    value |> String.lstrip(?[)
          |> String.rstrip(?])
          |> String.split(",")
          |> Enum.filter(&(String.length(&1) > 0))
          |> Enum.map(&String.to_integer/1)
  end

  @doc """
  Encode partitions list to string.

  ## Example

      iex> encode_partitions(nil)
      ""
      iex> encode_partitions([])
      "[]"
      iex> encode_partitions([1,2,3,4])
      "[1,2,3,4]"
  """
  def encode_partitions(nil), do: ""
  def encode_partitions(partitions) do
    "[#{ partitions |> Enum.map(&Integer.to_string/1) |> Enum.join(",") }]"
  end
end
