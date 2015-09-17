defmodule Cafex.Topic.Server do
  @moduledoc false

  use GenServer

  require Logger

  defmodule State do
    defstruct name: nil,
              metadata: nil,
              brokers: [],
              dead_brokers: [],
              socket: nil,
              client_id: nil,
              correlation_id: 0,
              timer: nil
  end

  alias Cafex.Socket
  alias Cafex.Protocol
  alias Cafex.Protocol.Metadata

  # ===================================================================
  # API
  # ===================================================================

  def start_link(name, brokers, opts \\ []) when is_binary(name) do
    GenServer.start_link __MODULE__, [name, brokers, opts]
  end

  def metadata(pid) do
    GenServer.call pid, :metadata
  end

  # ===================================================================
  #  GenServer callbacks
  # ===================================================================

  def init([name, brokers, opts]) do
    Logger.info fn -> "Starting topic server: #{name}" end

    client_id = Keyword.get(opts, :client_id, Protocol.default_client_id)

    :random.seed(:os.timestamp)

    state = %State{name: name,
                   brokers: Enum.shuffle(brokers),
                   client_id: client_id} |> fetch_metadata

    {:ok, state}
  end

  def handle_call(:metadata, _from, state) do
    %{metadata: metadata} = state = fetch_metadata(state)
    {:reply, metadata, state}
  end

  def handle_info({:timeout, tref, :close_socket}, %{timer: tref} = state) do
    {:noreply, close_socket(%{state | timer: nil})}
  end

  def terminate(_, state) do
    close_socket(state)
    :ok
  end

  # ===================================================================
  #  Internal functions
  # ===================================================================

  defp schedule_close_socket(%{timer: nil} = state) do
    tref = :erlang.start_timer(5000, self, :close_socket)
    %{state | timer: tref}
  end
  defp schedule_close_socket(%{timer: tref} = state) do
    :erlang.cancel_timer(tref)
    schedule_close_socket(%{state | timer: nil})
  end

  defp close_socket(%{socket: nil} = state), do: state
  defp close_socket(%{socket: socket, brokers: [broker|_], timer: timer} = state) do
    Logger.debug fn -> "Closing socket to: #{inspect broker}" end
    Socket.close(socket)
    if timer, do: :erlang.cancel_timer(timer)
    %{state | socket: nil, timer: nil, correlation_id: 0}
  end

  defp open_socket(%{socket: nil, brokers: [], dead_brokers: deads} = state) do
    :timer.sleep(1000)
    open_socket(%{state | brokers: deads,
                     dead_brokers: []})
  end
  defp open_socket(%{socket: nil, brokers: [{host, port}|rest], dead_brokers: deads} = state) do
    case Socket.open(host, port) do
      {:ok, socket} ->
        Logger.debug fn -> "Socket opened: #{host}:#{port}" end
        schedule_close_socket(%{state | socket: socket})
      {:error, reason} ->
        Logger.warn "Failed to open connection to broker: #{host}:#{port}, reason: #{inspect reason}"
        open_socket(%{state | brokers: rest,
                         dead_brokers: [{host, port}|deads]})
    end
  end
  defp open_socket(%{socket: socket} = state) when is_port(socket) do
    schedule_close_socket(state)
  end

  defp fetch_metadata(state) do
    state = %{name: topic,
              brokers: [broker|rest],
              dead_brokers: deads,
              socket: socket,
              client_id: client_id,
              correlation_id: correlation_id} = open_socket(state)

    request = Metadata.create_request(correlation_id, client_id, topic)

    case Socket.send_sync_request(socket, request) do
      {:ok, data} ->
        metadata = Metadata.parse_response(data)
        brokers = extract_brokers(metadata)
        %{state | metadata: metadata, brokers: brokers, correlation_id: correlation_id + 1}
      {:error, reason} ->
        Logger.error "Failed to get metadata from: #{inspect broker}, reason: #{inspect reason}"
        %{state | brokers: rest, dead_brokers: [broker|deads], correlation_id: correlation_id + 1}
        |> close_socket
        |> fetch_metadata
    end
  end

  defp extract_brokers(%{brokers: brokers}) do
    brokers
    |> Enum.map(fn b -> {b.host, b.port} end)
    |> Enum.shuffle
  end
end
