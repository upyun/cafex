defmodule Cafex.ConnectionTest do
  use ExUnit.Case, async: true

  require Logger
  Logger.remove_backend(:console)

  alias Cafex.Connection

  defmodule Server do
    use GenServer

    def stop(pid) do
      GenServer.call pid, :stop
    end

    def port(pid) do
      GenServer.call pid, :port
    end

    def received(pid, sock) do
      GenServer.cast pid, {:received, sock}
    end

    def init([]) do
      {:ok, listen_sock} = :gen_tcp.listen(0, [:binary, {:packet, 4}, {:active, false}])
      {:ok, port} = :inet.port listen_sock
      server_pid = self
      spawn fn ->
        accept(server_pid, listen_sock)
      end
      {:ok, {listen_sock, port}}
    end

    def handle_call(:stop, _from, state) do
      {:stop, :normal, :ok, state}
    end

    def handle_call(:port, _from, {_, port} = state) do
      {:reply, port, state}
    end

    def handle_cast({:received, sock}, state) do
      spawn fn ->
        do_receive sock
      end
      {:noreply, state}
    end

    defp accept(pid, listen_sock) do
      {:ok, sock} = :gen_tcp.accept(listen_sock)
      Server.received(pid, sock)
      accept(pid, listen_sock)
    end

    defp do_receive(sock) do
      case :gen_tcp.recv(sock, 0) do
        {:ok, bin} ->
          case bin do
            << -1 :: 16-signed, _rest :: binary >> ->
              # Trigger server to close tcp connection
              :gen_tcp.close(sock)
            <<_api_key :: 16-signed, _api_version :: 16, correlation_id :: 32, client_len :: 16, _client_id :: size(client_len)-binary, id :: 32, msg_len :: 16, msg :: size(msg_len)-binary>> ->
              reply = <<correlation_id :: 32, id :: 32, Cafex.Protocol.encode_string(msg) :: binary>>
              :gen_tcp.send(sock, reply)
              do_receive(sock)
            _ ->
              :gen_tcp.close(sock)
          end
        {:error, :closed} ->
          :gen_tcp.close(sock)
      end
    end
  end

  defmodule Decoder do
    @behaviour Cafex.Protocol.Decoder

    defmodule Request do
      use Cafex.Protocol

      @api_key 0
      defstruct test_id: nil, test_msg: nil

      def encode(request) do
        Decoder.encode(request)
      end
    end

    defmodule BadRequest do
      use Cafex.Protocol

      @api_key -1
      defstruct test_id: nil, test_msg: nil

      def encode(request) do
        Decoder.encode(request)
      end
    end

    def decode(<<id :: 32, msg_len :: 16, msg :: size(msg_len)-binary>>), do: {id, msg}

    def encode(%{test_id: id, test_msg: msg}) do
      <<id :: 32, Cafex.Protocol.encode_string(msg) :: binary>>
    end
  end

  setup do
    {:ok, pid} = GenServer.start Server, []
    {:ok, pid: pid, port: Server.port(pid)}
  end

  test "connect and close", context do
    port = context[:port]
    {:ok, pid} = Connection.start "localhost", port

    assert Process.alive?(pid)
    assert :ok == Connection.close(pid)

    refute Process.alive?(pid)

    assert {:error, _reason} = Connection.start("unknown_host", 8080)
  end

  test "request", context do
    port = Server.port context[:pid]
    {:ok, pid} = Connection.start "localhost", port

    assert Process.alive?(pid)

    request1 = %Decoder.Request{test_id: 1, test_msg: "hello"}
    request2 = %Decoder.Request{test_id: 2, test_msg: "hello"}

    assert {:ok, {1, "hello"}} == Connection.request(pid, request1, Decoder)
    assert {:ok, {2, "hello"}} == Connection.request(pid, request2, Decoder)

    assert :ok == Connection.close(pid)

    refute Process.alive?(pid)

    {:ok, pid} = Connection.start "localhost", port
    request3 = %Decoder.BadRequest{test_id: 3, test_msg: "hello"}
    assert {:closed, _} = catch_exit Connection.request(pid, request3, Decoder)
    refute Process.alive?(pid)

    {:ok, pid} = Connection.start "localhost", port
    server_pid = context[:pid]
    assert Process.alive?(pid)
    assert :ok == Server.stop(server_pid)
    assert Process.alive?(pid)
    assert {:closed, _} = catch_exit Connection.request(pid, request1, Decoder)
  end

  test "async request", context do
    port = Server.port context[:pid]
    {:ok, pid} = Connection.start "localhost", port

    assert Process.alive?(pid)

    request = %Decoder.Request{test_id: 3, test_msg: "hello"}

    Connection.async_request(pid, request, Decoder, spawn(fn ->
      assert_receive {:ok, {3, "hello"}}
    end))

    assert :ok == Connection.close(pid)

    refute Process.alive?(pid)
  end
end
