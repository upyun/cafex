defmodule Cafex.Integration.ZK.LeaderTest do
  use ExUnit.Case, async: true

  alias Cafex.ZK.Leader

  setup do
    zk_cfg = Application.get_env(:cafex, :zookeeper)
    zk_servers = Keyword.get(zk_cfg, :servers)
               |> Enum.map(fn {h, p} -> {:erlang.bitstring_to_list(h), p} end)
    zk_timeout = Keyword.get(zk_cfg, :timeout)
    chroot = Keyword.get(zk_cfg, :chroot)
    zk_prefix = "/leader_test"
    {:ok, pid1} = :erlzk.connect(zk_servers, zk_timeout, chroot: chroot)
    {:ok, pid2} = :erlzk.connect(zk_servers, zk_timeout, chroot: chroot)
    {:ok, pid3} = :erlzk.connect(zk_servers, zk_timeout, chroot: chroot)

    on_exit fn ->
      ZKHelper.rmr(pid1, zk_prefix)
      :erlzk.close pid1
      :erlzk.close pid2
      :erlzk.close pid3
    end

    {:ok, zk_prefix: zk_prefix, pid1: pid1, pid2: pid2, pid3: pid3}
  end

  test "leader election", context do
    pid1 = context[:pid1]
    pid2 = context[:pid2]
    pid3 = context[:pid3]
    prefix = context[:zk_prefix]

    path = Path.join(prefix, "leader_election_test")
    assert {:true,  seq1} = Leader.election(pid1, path)
    assert {:false, seq2} = Leader.election(pid2, path)
    assert {:false, seq3} = Leader.election(pid3, path)

    assert :ok == :erlzk.delete(pid1, seq1)
    assert_receive {:leader_election, ^seq2}
    assert {:false, seq1} = Leader.election(pid1, path)

    assert :ok == :erlzk.delete(pid2, seq2)
    assert_receive {:leader_election, ^seq3}
    assert {:false,  seq2} = Leader.election(pid2, path)

    assert :ok == :erlzk.delete(pid3, seq3)
    assert_receive {:leader_election, ^seq1}
    assert {:false,  seq3} = Leader.election(pid3, path)

    [{pid1, seq1}, {pid2, seq2}, {pid3, seq3}]
    |> Enum.each fn {pid, seq} ->
      assert :ok == :erlzk.delete(pid, seq), "seq #{inspect seq} should be deleted"
    end

    elected = [pid1, pid2, pid3]
    |> Enum.shuffle
    |> Enum.map(fn pid ->
      Leader.election(pid, path)
    end)
    |> Enum.group_by(fn {leader, _seq} ->
      leader
    end)

    assert 1 == Map.get(elected, true) |> length
    assert 2 == Map.get(elected, false) |> length
  end
end
