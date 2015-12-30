defmodule Cafex.Integration.Lock.ZkTest do
  use ExUnit.Case, async: true

  alias Cafex.Lock.ZK, as: Lock

  @path "/lock"
  @cfg Application.get_env(:cafex, :zookeeper)

  setup do
    servers = @cfg[:servers] |> Enum.map(fn {h, p} ->
      {:erlang.bitstring_to_list(h), p}
    end)
    opts = [servers: servers,
            timeout: @cfg[:timeout],
            chroot: @cfg[:chroot]]
    {:ok, opts: opts}
  end
  test "zk lock", context do
    opts = context[:opts]
    {:ok, lock1} = Lock.acquire(@path, opts)
    {:wait, lock2} = Lock.acquire(@path, opts)

    assert :ok = Lock.release(lock2)

    {:wait, lock2} = Lock.acquire(@path, opts)
    assert :ok = Lock.release(lock1)
    assert_receive {:lock, :ok, lock2}

    assert :ok = Lock.release(lock2)
  end
end
