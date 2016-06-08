defmodule Cafex.Protocol.Test do
  use ExUnit.Case, async: true

  test "api keys" do
    assert Cafex.Protocol.api_key(:produce)           == 0
    assert Cafex.Protocol.api_key(:fetch)             == 1
    assert Cafex.Protocol.api_key(:offset)            == 2
    assert Cafex.Protocol.api_key(:metadata)          == 3
    assert Cafex.Protocol.api_key(:offset_commit)     == 8
    assert Cafex.Protocol.api_key(:offset_fetch)      == 9
    assert Cafex.Protocol.api_key(:group_coordinator) == 10
    assert Cafex.Protocol.api_key(:join_group)        == 11
    assert Cafex.Protocol.api_key(:heartbeat)         == 12
    assert Cafex.Protocol.api_key(:leave_group)       == 13
    assert Cafex.Protocol.api_key(:sync_group)        == 14
    assert Cafex.Protocol.api_key(:describe_groups)   == 15
    assert Cafex.Protocol.api_key(:list_groups)       == 16
  end

  test "macros compile" do
    message = ~r"To use Cafex.Protocol, `api` must be set"
    assert_raise CompileError, message, fn -> Code.compile_quoted(quote do
      defmodule BadApi do
        use Cafex.Protocol
        defrequest do
        end

        defresponse do
        end
      end
    end) end

    message = ~r"Unsupported api: -1"
    assert_raise CompileError, message, fn -> Code.compile_quoted(quote do
      defmodule BadApi do
        use Cafex.Protocol, api: -1
        defrequest do
        end

        defresponse do
        end
      end
    end) end

    message = ~r"Use Cafex.Protocol must call `defresponse`"
    assert_raise CompileError, message, fn -> Code.compile_quoted(quote do
      defmodule BadApi do
        use Cafex.Protocol, api: :produce
        defrequest do
        end
      end
    end) end

    assert [
      {TestApi.Response, _},
      {Cafex.Protocol.Request.TestApi.Request, _},
      {TestApi.Request, _},
      {TestApi, _}
    ] = Code.compile_quoted(quote do
      defmodule TestApi do
        use Cafex.Protocol, api: :metadata
        defresponse do
        end

        def decode(_), do: %TestApi.Response{}
      end
    end)

    assert [
      {Cafex.Protocol.Request.TestApi2.Request, _},
      {TestApi2.Request, _},
      {TestApi2.Response, _},
      {TestApi2, _}
    ] = Code.compile_quoted(quote do
      defmodule TestApi2 do
        use Cafex.Protocol, api: :metadata

        defrequest do
          field :name, binary
        end

        defresponse do
        end

        def decode(_), do: %TestApi2.Response{}
        def encode(%TestApi2.Request{}), do: <<>>
      end
    end)
  end

  defmodule TestApi do
    use Cafex.Protocol, api: :metadata

    defresponse do
    end

    def decode(<<>>), do: %TestApi.Response{}
  end

  test "generated kafka API module" do
    req = %TestApi.Request{}
    res = %TestApi.Response{}
    assert TestApi.encode(req) == <<>>
    assert TestApi.decode(<<>>) == res
    assert TestApi.api_key(req) == Cafex.Protocol.api_key(:metadata)
    assert TestApi.api_version(req) == 0
    assert TestApi.has_response?(req) == true
    assert :ok == Protocol.assert_impl!(Cafex.Protocol.Request, TestApi.Request)
  end
end

