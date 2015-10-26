defmodule Cafex.Consumer.LoadBalancer.Test do
  use ExUnit.Case, async: true

  import Cafex.Consumer.LoadBalancer
  doctest Cafex.Consumer.LoadBalancer

  test "more rebalance cases" do
    assert [{:a, [0, 1, 2, 3, 4]}] == rebalance([{:a, [0, 1, 2, 3, 4]}], 5)
    assert [{:a, [0, 1, 2, 3, 4]}] == rebalance([{:a, [0, 1, 2, 3]}], 5)
    assert [{:a, [0, 1, 2, 3, 4]}] == rebalance([{:a, [0, 1, 2]}], 5)
    assert [{:a, [0, 1, 2, 3, 4]}] == rebalance([{:a, [0, 1]}], 5)
    assert [{:a, [0, 1, 2, 3, 4]}] == rebalance([{:a, []}], 5)
    assert [{:a, [0, 1, 2, 3, 4]}] == rebalance([{:a, [0]}], 5)
    assert [{:a, [0, 1, 2, 3, 4]}] == rebalance([{:a, [1]}], 5)
    assert [{:a, [0, 1, 2, 3, 4]}] == rebalance([{:a, [1, 2, 3]}], 5)

    assert [{:a, [0, 1]}, {:b, [2, 3]}, {:c, [4]}] == rebalance([{:a, []}, {:b, []}, {:c, []}], 5)
    assert [{:a, [1, 2]}, {:b, [0, 3]}, {:c, [4]}] == rebalance([{:a, [1]}, {:b, [0]}, {:c, []}], 5)

    assert [{:a, [0]}, {:b, [1]}, {:c, [2]}, {:d, [3]}, {:e, []}] == rebalance([{:a, []}, {:b, []}, {:c, []}, {:d, []}, {:e, []}], 4)

    assert [{:a, [0, 1]}, {:b, [2]}, {:c, [3]}, {:d, [4]}] == rebalance([{:a, []}, {:b, []}, {:c, []}, {:d, []}], 5)
    assert [{:a, [0, 1]}, {:b, [2]}, {:c, [3]}] == rebalance([{:a, []}, {:b, []}, {:c, []}], 4)
    assert [{:a, [0, 1]}, {:b, [2]}, {:c, [3]}, {:d, [4]}] == rebalance([{:a, [0, 1]}, {:b, [2, 3]}, {:c, []}, {:d, []}], 5)

    assert [{:a, [0, 1]}, {:b, [2, 3]}, {:c, [4]}, {:d, [5]}] == rebalance([{:a, []}, {:b, []}, {:c, []}, {:d, []}], 6)
    assert [{:a, [0, 1]}, {:b, [2, 3]}, {:c, [4, 5]}] == rebalance([{:a, []}, {:b, []}, {:c, []}], 6)
    assert [{:a, [0, 3]}, {:b, [4, 5]}, {:c, [1]}, {:d, [2]}] == rebalance([{:a, [0, 3]}, {:b, [4, 5]}, {:c, [1, 2]}, {:d, []}], 6)
  end
end

