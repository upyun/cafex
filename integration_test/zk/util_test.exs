defmodule Cafex.Integration.ZK.UtilTest do
  use ExUnit.Case, async: true

  alias Cafex.ZK.Util

  setup_all do
    zk_cfg = Application.get_env(:cafex, :zookeeper)
    zk_servers = Keyword.get(zk_cfg, :servers)
               |> Enum.map(fn {h, p} -> {:erlang.bitstring_to_list(h), p} end)
    zk_timeout = Keyword.get(zk_cfg, :timeout)
    zk_prefix  = Keyword.get(zk_cfg, :path)
    zk_prefix  = Path.join(zk_prefix, "util_test")
    {:ok, pid} = :erlzk.connect(zk_servers, zk_timeout)

    on_exit fn ->
      ZKHelper.rmr(pid, zk_prefix)
    end

    {:ok, zk_pid: pid, zk_prefix: zk_prefix}
  end

  test "create node", context do
    pid = context[:zk_pid]
    prefix = context[:zk_prefix]

    path = Path.join [prefix, "/a", "/b"]
    assert {:error, :no_node} == :erlzk.exists(pid, path)
    assert :ok == Util.create_node(pid, path)
    assert {:ok, _} = :erlzk.exists(pid, path)
    assert :ok == Util.create_node(pid, path), "Should return :ok if node already exists"
  end

  test "create nodes", context do
    pid = context[:zk_pid]
    prefix = context[:zk_prefix]

    paths = [
      Path.join([prefix, "/x", "/x1"]),
      Path.join([prefix, "/y", "/y1"]),
      Path.join([prefix, "/z", "/z1"]),
    ]

    Enum.each(paths, fn path ->
      assert {:error, :no_node} == :erlzk.exists(pid, path)
    end)

    assert :ok == Util.create_nodes(pid, paths)
    Enum.each(paths, fn path ->
      assert {:ok, _} = :erlzk.exists(pid, path)
    end)
  end

  test "get children", context do
    pid = context[:zk_pid]
    prefix = context[:zk_prefix]

    parent = Path.join prefix, "/parent_get_children_test"
    paths = [
      Path.join([parent, "/c1"]),
      Path.join([parent, "/c2"]),
      Path.join([parent, "/c3"]),
    ]

    assert {:error, :no_node} == Util.get_children(pid, parent)
    assert :ok == Util.create_node(pid, parent)
    assert {:ok, []} == Util.get_children(pid, parent)
    assert {:ok, HashDict.new} == Util.get_children_with_data(pid, parent)

    assert :ok == Util.create_nodes(pid, paths)

    assert {:ok, children} = Util.get_children(pid, parent)
    assert ["c1", "c2", "c3"] == Enum.sort(children)

    assert {:ok, Enum.into([{"c1", ""}, {"c2", ""}, {"c3", ""}], HashDict.new)} == Util.get_children_with_data(pid, parent)

    Enum.each paths, fn path ->
      assert {:ok, _} = :erlzk.set_data(pid, path, path)
    end
    assert {:ok, Enum.map(paths, fn path ->
      {Path.basename(path), path}
    end)|> Enum.into(HashDict.new)} == Util.get_children_with_data(pid, parent)

    refute_received {_, _}, "Should not received any messages from erlzk"
  end

  test "get children with watcher", context do
    pid = context[:zk_pid]
    prefix = context[:zk_prefix]

    parent = Path.join(prefix, "/parent_get_children_with_watcher_test")
    assert {:error, :no_node} == Util.get_children(pid, parent, self)
    assert :ok == Util.create_node(pid, parent)
    refute_receive {:node_created, ^parent}

    child = Path.join(parent, "/ccc")
    assert {:ok, []} == Util.get_children(pid, parent, self)
    assert :ok == Util.create_node(pid, child)
    assert_receive {:node_children_changed, ^parent}

    assert {:ok, ["ccc"]} == Util.get_children(pid, parent, self)
    assert :ok == :erlzk.delete(pid, child)
    assert_receive {:node_children_changed, ^parent}
  end

  test "get data", context do
    pid = context[:zk_pid]
    prefix = context[:zk_prefix]

    parent = Path.join([prefix, "/parent_get_data_test"])
    child  = Path.join([parent, "/ccc1"])
    assert {:error, :no_node} == Util.get_data(pid, child)
    assert :ok == Util.create_nodes(pid, [ parent, child ])
    refute_receive {_, ^child}

    assert {:ok, {"", _stat}} = Util.get_data(pid, child)

    assert {:ok, _} = :erlzk.set_data(pid, child, "hello")
    refute_receive {:node_data_changed, ^child}

    assert {:ok, {"hello", _stat}} = Util.get_data(pid, child)

    refute_received {_, _}, "Should not received any messages from erlzk"
  end

  test "get data with watcher", context do
    pid = context[:zk_pid]
    prefix = context[:zk_prefix]

    parent = Path.join([prefix, "/parent_get_data_with_watcher_test"])
    child  = Path.join([parent, "/ccc1"])

    assert {:error, :no_node} == Util.get_data(pid, child, self)
    assert :ok == Util.create_nodes(pid, [ parent, child ])
    assert_receive {:node_created, ^child}

    assert {:ok, {"", _stat}} = Util.get_data(pid, child, self)

    assert {:ok, _} = :erlzk.set_data(pid, child, "hello")
    assert_receive {:node_data_changed, ^child}

    assert {:ok, {"hello", _stat}} = Util.get_data(pid, child, self)

    assert :ok == :erlzk.delete(pid, child)
    assert_receive {:node_deleted, ^child}
  end
end
