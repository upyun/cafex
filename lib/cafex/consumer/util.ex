defmodule Cafex.Consumer.Util do

  def decode_partitions(nil), do: []
  def decode_partitions(""), do: []
  def decode_partitions(value) do
    value |> String.lstrip(?[)
          |> String.rstrip(?])
          |> String.split(",")
          |> Enum.filter(&(String.length(&1) > 0))
          |> Enum.map(&String.to_integer/1)
  end

  def encode_partitions(nil), do: ""
  def encode_partitions(partitions) do
    "[#{ partitions |> Enum.map(&Integer.to_string/1) |> Enum.join(",") }]"
  end
end
