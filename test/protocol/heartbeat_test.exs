defmodule Cafex.Protocol.Heartbeat.Test do
  use ExUnit.Case, async: true

  alias Cafex.Protocol.Heartbeat
  alias Cafex.Protocol.Heartbeat.Request
  alias Cafex.Protocol.Heartbeat.Response

  test "encode a valid Heartbeat request" do
    good_request = << 19 :: 16-signed,
                      "cafex_test_consumer" :: binary,
                      2 :: 32-signed,
                      42 :: 16-signed,
                      "cafex-e6f5cd5e-116b-49a3-a3ca-8746779f3cc5" :: binary>>

    request = %Request{
      group_id: "cafex_test_consumer",
      generation_id: 2,
      member_id: "cafex-e6f5cd5e-116b-49a3-a3ca-8746779f3cc5"
    }

    assert good_request == Cafex.Protocol.Request.encode(request)
  end

  test "parse_response correctly parse a valid response" do
    response = <<0 :: 16-signed>>
    expected_response = %Response{error: :no_error}

    assert expected_response == Heartbeat.decode(response)
  end
end
