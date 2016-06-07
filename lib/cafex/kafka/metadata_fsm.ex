defmodule Cafex.Kafka.MetadataFsm do
  @moduledoc false

  alias Cafex.Connection

  require Logger

  defmodule State do
    @moduledoc false
    @type t :: %__MODULE__{}
    defstruct from: nil,
              request: nil,
              conn: nil,
              error: nil,
              result: nil,
              feed_brokers: nil,
              dead_brokers: nil
  end

  @callback do_request(State.t) :: State.t
  @callback make_request(request :: term) :: request :: term

  defmacro __using__(_opts) do
    quote location: :keep do
      @behaviour :gen_fsm
      @behaviour unquote(__MODULE__)
      alias Cafex.Connection
      import unquote(__MODULE__)

      def start_link(brokers, request) do
        :gen_fsm.start_link __MODULE__, [brokers, request], []
      end

      def request(brokers, request) do
        request = make_request(request)
        {:ok, pid} = __MODULE__.start_link(brokers, request)
        :gen_fsm.sync_send_event(pid, :request)
      end

      def make_request(request), do: request

      @doc false
      def init([brokers, request]) do
        state = %State{request: request,
                       feed_brokers: Enum.shuffle(brokers),
                       dead_brokers: []}
        {:ok, :prepared, state}
      end

      @doc false
      def prepared(:request, from, state) do
        %{state | from: from}
        |> open_conn
        |> do_request
        |> finalize
      end

      @doc false
      def handle_event(event, state_name, state) do
        {:stop, {:bad_event, state_name, event}, state}
      end

      @doc false
      def handle_sync_event(event, _from, state_name, state) do
        {:stop, {:bad_sync_event, state_name, event}, state}
      end

      @doc false
      def handle_info({_port, :closed}, :stopped, state) do
        {:stop, :shutdown, state}
      end

      @doc false
      def code_change(_old, state_name, state_data, _extra) do
        {:ok, state_name, state_data}
      end

      @doc false
      def terminate(_reason, _state_name, state_data) do
        close_conn(state_data)
        :ok
      end

      def do_request(%{conn: conn, request: request} = state) do
        case send_request(state) do
          {:ok, result} -> %{state | result: result}
          {:error, error} -> %{state | error: error}
        end
      end

      defoverridable [do_request: 1, make_request: 1]
    end
  end

  def finalize(%State{from: from, error: nil, result: result} = state) do
    :gen_fsm.reply from, {:ok, result}
    {:stop, :normal, state}
  end
  def finalize(%State{from: from, error: error} = state) do
    :gen_fsm.reply from, {:error, error}
    {:stop, :normal, state}
  end

  def open_conn(%State{conn: nil,
                        feed_brokers: [],
                        dead_brokers: deads} = state) do
    :timer.sleep(1000)
    open_conn(%{state | feed_brokers: Enum.shuffle(deads),
                        dead_brokers: []})
  end
  def open_conn(%State{conn: nil,
                        feed_brokers: [{host, port}|rest],
                        dead_brokers: deads} = state) do
    case Connection.start(host, port) do
      {:ok, pid} ->
        %{state|conn: pid}
      {:error, reason} ->
        Logger.warn "Open connection to #{host}:#{port} error: #{inspect reason}, try others"
        open_conn(%{state | conn: nil, feed_brokers: rest,
                            dead_brokers: [{host, port}|deads]})
    end
  end
  def open_conn(%State{conn: conn} = state) when is_pid(conn), do: state

  def close_conn(%State{conn: nil} = state), do: state
  def close_conn(%State{conn: conn} = state) do
    Connection.close(conn)
    %{state|conn: nil}
  end

  def send_request(%State{conn: conn, request: request}) do
    Connection.request(conn, request)
  end

end
