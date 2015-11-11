defmodule Cafex.Integration.ZK.LockTest do
  use ExUnit.Case, async: true

  alias Cafex.ZK.Lock

  setup do
    zk_cfg = Application.get_env(:cafex, :zookeeper)
    zk_servers = Keyword.get(zk_cfg, :servers)
               |> Enum.map(fn {h, p} -> {:erlang.bitstring_to_list(h), p} end)
    zk_timeout = Keyword.get(zk_cfg, :timeout)
    zk_prefix  = Keyword.get(zk_cfg, :path)
    zk_prefix  = Path.join(zk_prefix, "lock_test")
    {:ok, pid} = :erlzk.connect(zk_servers, zk_timeout)

    on_exit fn ->
      ZKHelper.rmr(pid, zk_prefix)
    end

    {:ok, zk_pid: pid, zk_prefix: zk_prefix}
  end

  test "zk lock", context do
    pid = context[:zk_pid]
    prefix = context[:zk_prefix]

    lock_path = Path.join(prefix, "lock")
    assert {:ok, seq1} = Lock.aquire(pid, lock_path)

    assert {:error, :locked}  == Lock.aquire(pid, lock_path)
    assert {:error, :timeout} == Lock.aquire(pid, lock_path, 10)
    assert {:wait,  seq2}     =  Lock.aquire(pid, lock_path, :infinity)

    assert :ok == Lock.release(pid, seq1)
    assert_receive {:lock_again, seq2}
    assert {:ok, seq2} == Lock.reaquire(pid, lock_path, seq2)
  end

end
