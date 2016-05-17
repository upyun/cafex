defmodule Cafex.Consumer.OffsetManager.Test do
  use ExUnit.Case, async: true

  alias Cafex.Consumer.OffsetManager
  alias Cafex.Consumer.OffsetManager.State

  defmodule Conn do
    use GenServer

    def handle_call({:request, %{topics: [{topic, partitions}]}}, _from, state) do
       ps = Enum.map(partitions, fn({p, _o, _m}) ->
         {p, :no_error}
       end)
     reply = {:ok, %{topics: [{topic, ps}]}}
     {:reply, reply, state}
    end
  end

  defmodule OffsetManagerTest do
    use GenServer

    def get(pid) do
      GenServer.call pid, :get
    end

    def init(state) do
      {:ok, state}
    end

    def handle_call(:get, _from, {_, _, counts} = state) do
      {:reply, counts, state}
    end

    def handle_call(msg, from, {state, pid, cnt}) do
      case OffsetManager.handle_call(msg, from, state) do
        {:reply, reply, state} ->
          {:reply, reply, {state, pid, cnt}}
        {:reply, reply, state, timeout} ->
          {:reply, reply, {state, pid, cnt}, timeout}
        {:noreply, state} ->
          {:noreply, {state, pid, cnt}}
        {:noreply, state, timeout} ->
          {:noreply, {state, pid, cnt}, timeout}
        {:stop, reason, reply, state} ->
          {:stop, reason, reply, {state, pid, cnt}}
        {:stop, reason, state} ->
          {:stop, reason, {state, pid, cnt}}
      end
    end
    def handle_info({:timeout, ref, :do_commit} = msg, {state, pid, %{do_commit: c} = cnt}) do
      cnt = if is_reference(ref) do
        cnt
      else
        %{cnt| do_commit: c + 1}
      end
      do_handle_info(msg, {state, pid, cnt})
    end
    def handle_info(msg, state) do
      do_handle_info(msg, state)
    end

    defp do_handle_info(msg, {state, pid, cnt}) do
      case OffsetManager.handle_info(msg, state) do
        {:noreply, state} ->
          {:noreply, {state, pid, cnt}}
        {:noreply, state, timeout} ->
          {:noreply, {state, pid, cnt}, timeout}
        {:stop, reason, state} ->
          {:stop, reason, {state, pid, cnt}}
      end
    end
  end

  test "Offset Manager" do
    partitions = 10

    to_be_commit = Enum.map(0..partitions, fn(p) ->
      {p, {0, ""}}
    end) |> Enum.into(%{})

   {:ok, conn} = GenServer.start_link(Conn, [])

    state = %State{
      coordinator: {"localhost", 9000},
      topic: "test_topic",
      consumer_group: "test_group",
      partitions: partitions,
      auto_commit: true,
      interval: 500,
      max_buffers: 5,
      conn: conn,
      storage: :kafka,
      to_be_commit: to_be_commit
    }

    {:ok, pid} = GenServer.start_link(OffsetManagerTest, {state, self, %{do_commit: 0}})
    GenServer.call pid, {:commit, 1, 1, ""}

    for partition <- 0..partitions do
      spawn(fn ->
        for offset <- 0..3 do
          GenServer.call pid, {:commit, partition, offset, ""}
        end
      end)
    end

    :timer.sleep(400)
    %{do_commit: commit_times} = OffsetManagerTest.get(pid)
    assert commit_times == 5
  end
end
