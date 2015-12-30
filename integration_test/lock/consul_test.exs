defmodule Cafex.Integration.Lock.ConsulTest do
  use ExUnit.Case, async: true

  @path "cafex_integration_test"

  alias Cafex.Lock.Consul, as: Lock

  test "consul lock" do
    {:ok, lock1} = Lock.acquire(@path)
    {:wait, lock2} = Lock.acquire(@path)
    assert :ok = Lock.release(lock2)

    {:wait, lock2} = Lock.acquire(@path)
    assert :ok = Lock.release(lock1)
    assert_receive {:lock, :ok, lock2}

    assert :ok = Lock.release(lock2)
  end
end
