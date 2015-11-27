defmodule Cafex.Protocol do
  @moduledoc """
  This module provide encode/decode functions for common structures in Kafka protocol.

  And also provide conveniences for implementing API request and the `Cafex.Protocol.Request`.

  ## APIs

    * `Cafex.Protocol.Metadata`
      - `Cafex.Protocol.Metadata.Request`
      - `Cafex.Protocol.Metadata.Response`
    * `Cafex.Protocol.Produce`
      - `Cafex.Protocol.Produce.Request`
      - `Cafex.Protocol.Produce.Response`
    * `Cafex.Protocol.Fetch`
      - `Cafex.Protocol.Fetch.Request`
      - `Cafex.Protocol.Fetch.Response`
    * `Cafex.Protocol.Offset`
      - `Cafex.Protocol.Offset.Request`
      - `Cafex.Protocol.Offset.Response`
    * `Cafex.Protocol.ConsumerMetadata`
      - `Cafex.Protocol.ConsumerMetadata.Request`
      - `Cafex.Protocol.ConsumerMetadata.Response`
    * `Cafex.Protocol.OffsetCommit`
      - `Cafex.Protocol.OffsetCommit.Request`
      - `Cafex.Protocol.OffsetCommit.Response`
    * `Cafex.Protocol.OffsetFetch`
      - `Cafex.Protocol.OffsetFetch.Request`
      - `Cafex.Protocol.OffsetFetch.Response`
  """

  @type api_version :: 0 | 1 | 2
  @type api_key :: 0..16
  @type error :: Cafex.Protocol.Errors.t

  alias Cafex.Protocol.Request

  defmacro __using__(opts) do
    {opts, []} = Code.eval_quoted(opts, [], __CALLER__)
    api_key = Keyword.get opts, :api_key
    api_version = Keyword.get opts, :api_version, 0
    mod = __CALLER__.module

    if api_key == nil do
      raise "To use #{inspect __MODULE__}, `api_key` must be set"
    end

    Module.put_attribute mod, :api_key, api_key
    Module.put_attribute mod, :api_version, api_version

    quote do
      import unquote(__MODULE__), only: [defrequest: 1, defresponse: 1]
      import Cafex.Protocol.Codec
      @behaviour Cafex.Protocol.Codec
      @before_compile unquote(__MODULE__)

      def has_response?(_), do: true
      def decoder(_), do: __MODULE__
      def api_key(_), do: unquote(api_key)
      def api_version(_) do
        case unquote(api_version) do
          nil -> 0
          version -> version
        end
      end

      defoverridable [has_response?: 1, api_version: 1, api_key: 1, decoder: 1]
    end
  end

  defmacro __before_compile__(env) do
    # TODO check defrequest and defresponse
    mod = env.module
    request = Module.get_attribute mod, :request
    response = Module.get_attribute mod, :response

    quoted = []

    quoted = quoted ++ if request == nil do
      [quote do
        unquote(__MODULE__).defrequest
      end]
    end

    if response == nil do
      raise "use #{__MODULE__} must call `defresponse`"
    end

    quoted
  end

  defmacro defrequest(opts \\ []) do
    block = Keyword.get(opts, :do)
    mod = __CALLER__.module

    impl_protocol = impl_request_protocol(mod)

    quote do
      defmodule Request do
        import unquote(__MODULE__), only: [field: 3, field: 2]

        Module.register_attribute(__MODULE__, :fields, accumulate: true)
        Module.register_attribute(__MODULE__, :struct_fields, accumulate: true)

        unquote(block)
        unquote(impl_protocol)

        Module.eval_quoted __ENV__, [
          Cafex.Protocol.__struct__(@struct_fields),
          Cafex.Protocol.__typespec__(__MODULE__)
        ]
      end
      Module.put_attribute __MODULE__, :request, true
    end
  end

  defmacro defresponse(do: block) do
    quote do
      defmodule Response do
        Module.register_attribute(__MODULE__, :fields, accumulate: true)
        Module.register_attribute(__MODULE__, :struct_fields, accumulate: true)
        import unquote(__MODULE__), only: [field: 3, field: 2]

        unquote(block)

        Module.eval_quoted __ENV__, [
          Cafex.Protocol.__struct__(@struct_fields),
          Cafex.Protocol.__typespec__(__MODULE__)
        ]
      end
      Module.put_attribute __MODULE__, :response, true
    end
  end

  defmacro field(name, opts \\ [], type) do
    type = Macro.escape(type)
    quote do
      Cafex.Protocol.__field__(__MODULE__, unquote(name), unquote(type), unquote(opts))
    end
  end

  defdelegate encode_request(client_id, correlation_id, request), to: Cafex.Protocol.Codec
  defdelegate encode_string(data), to: Cafex.Protocol.Codec
  defdelegate has_response?(request), to: Cafex.Protocol.Request

  @doc false
  def __typespec__(mod) do
    types = Module.get_attribute(mod, :fields)

    {:%, [], [name, {:%{}, [], _}]} = quote do
      %unquote(mod){}
    end

    type_specs = {:%, [], [name, {:%{}, [], types}]}

    quote do
      @type t :: unquote(type_specs)
    end
  end

  @doc false
  def __field__(mod, name, type, opts) do
    default = Keyword.get(opts, :default)
    Module.put_attribute(mod, :fields, {name, type})
    put_struct_field(mod, name, default)
  end

  @doc false
  def __struct__(struct_fields) do
    quote do
      defstruct unquote(Macro.escape(struct_fields))
    end
  end

  defp put_struct_field(mod, name, assoc) do
    fields = Module.get_attribute(mod, :struct_fields)

    if List.keyfind(fields, name, 0) do
      raise ArgumentError, "field #{inspect name} is already set on #{inspect mod}"
    end

    Module.put_attribute(mod, :struct_fields, {name, assoc})
  end

  defp impl_request_protocol(mod) do
    impls = [:api_key, :api_version, :has_response?, :encode, :decoder]
    |> Enum.map(fn func ->
      quote do
        def unquote(func)(req), do: unquote(mod).unquote(func)(req)
      end
    end)

    quote do
      defimpl Cafex.Protocol.Request do
        unquote(impls)
      end
    end
  end
end
