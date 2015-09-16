defmodule Cafex.Socket do
  def open(host, port) do
    :gen_tcp.connect :erlang.bitstring_to_list(host), port,
                     [:binary, {:packet, 4}, {:sndbuf, 10000000}]
  end

  def close(socket) do
    :gen_tcp.close(socket)
  end

  def send_sync_request(socket, data, timeout \\ 1000) do
    case :gen_tcp.send(socket, data) do
      :ok ->
        recv_response(socket, timeout)
      error ->
        error
    end
  end

  defp recv_response(socket, timeout) do
    receive do
      {:tcp, ^socket, data} ->
        {:ok, data}
      {:tcp_closed, ^socket} ->
        {:error, :closed}
    after
      timeout ->
        {:error, :timeout}
    end
  end
end
