defmodule Cafex.Consumer.LoadBalancer do

  @doc """
  _Deprecated_

  Balance partition assignment between consumers

  ## Examples

      iex> balance([:a], 5)
      [{:a, [0, 1, 2, 3, 4]}]

      iex> balance([:a, :b], 5)
      [{:a, [0, 1, 2]}, {:b, [3, 4]}]

      iex> balance([:a, :b, :c], 5)
      [{:a, [0, 1]}, {:b, [2, 3]}, {:c, [4]}]

      iex> balance([:a, :b, :c], 6)
      [{:a, [0, 1]}, {:b, [2, 3]}, {:c, [4, 5]}]
  """
  @spec balance([atom], integer) :: [{atom, [integer]}]
  def balance(consumers, partitions) do
    count  = round(partitions / length(consumers))
    chunks = Enum.chunk 0..(partitions - 1), count, count, []
    Enum.zip consumers, chunks
  end

  @doc """
  Balance partition assignment between consumers

  ## Examples

      iex> rebalance [{:a, [0, 1, 2, 3, 4]}], 5
      [{:a, [0, 1, 2, 3, 4]}]

      iex> rebalance [{:a, [0, 1, 2, 3, 4]}, {:b, []}], 5
      [{:a, [0, 1, 2]}, {:b, [3, 4]}]

      iex> rebalance [{:a, [0, 1, 2, 3, 4]}, {:b, []}, {:c, []}], 5
      [{:a, [0, 1]}, {:b, [2, 3]}, {:c, [4]}]

      iex> rebalance [{:a, [0, 1, 2]}, {:b, [3, 4]}, {:c, []}], 5
      [{:a, [0, 1]}, {:b, [3, 4]}, {:c, [2]}]

      iex> rebalance [{:a, [0, 1]}, {:c, [2]}], 5
      [{:a, [0, 1, 3]}, {:c, [2, 4]}]

      iex> rebalance [{:a, []}, {:b, [0, 1, 2, 3, 4]}], 5
      [{:a, [3, 4]}, {:b, [0, 1, 2]}]

  More details see the source of this module or test.
  """
  @spec balance([{atom, [integer]}], integer) :: [{atom, [integer]}]
  def rebalance(layout, partitions) do
    consumers = Keyword.keys(layout)
    count     = Float.floor(partitions / length(consumers)) |> trunc
    remainder = rem(partitions, length(consumers))
    all       = Enum.into(0..(partitions - 1), HashSet.new)

    assigned  = layout |> Keyword.values
                       |> List.flatten
                       |> Enum.into(HashSet.new)
    not_assigned = all |> HashSet.difference(assigned)
                       |> Enum.uniq
                       |> Enum.sort

    {new_layout, [], 0} =
    layout |> Enum.sort(fn {_c1, p1}, {_c2, p2} ->
                length(p1) >= length(p2)
              end)
           |> Enum.reduce({[], not_assigned, remainder}, fn
             {consumer, partitions}, {layout, not_assigned, remainder} when remainder > 0 ->
               {keep, rest} = assign(partitions, count + 1, not_assigned)
               {[{consumer, keep}|layout], rest, remainder - 1}

             {consumer, partitions}, {layout, not_assigned, remainder} when remainder == 0 ->
               {keep, rest} = assign(partitions, count, not_assigned)
               {[{consumer, keep}|layout], rest, remainder}

           end)

    Enum.sort(new_layout)
  end

  defp assign(current, count, not_assigned) when length(current) > count do
    {partitions, rest} = Enum.split(current, count)
    {partitions, Enum.sort(rest ++ not_assigned)}
  end
  defp assign(current, count, not_assigned) when length(current) < count do
    {partitions, rest} = Enum.split(not_assigned, count - length(current))
    {Enum.sort(current ++ partitions), rest}
  end
  defp assign(current, count, not_assigned) when length(current) == count do
    {current, not_assigned}
  end
end
