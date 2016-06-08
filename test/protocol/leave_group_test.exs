defmodule Cafex.Protocol.LeaveGroup.Test do
  use ExUnit.Case, async: true

  alias Cafex.Protocol.LeaveGroup
  alias Cafex.Protocol.LeaveGroup.Request
  alias Cafex.Protocol.LeaveGroup.Response

  test "LeaveGroup protocol implementation" do
    req = %Request{}
    assert LeaveGroup.has_response?(req) == true
    assert LeaveGroup.decoder(req) == LeaveGroup
    assert LeaveGroup.api_key(req) == Cafex.Protocol.api_key(:leave_group)
    assert LeaveGroup.api_version(req) == 0
  end

  test "encode a valid LeaveGroup request" do
    good_request = << 19 :: 16-signed,
                      "cafex_test_consumer" :: binary,
                      42 :: 16-signed,
                      "cafex-e6f5cd5e-116b-49a3-a3ca-8746779f3cc5" :: binary>>

    request = %Request{
      group_id: "cafex_test_consumer",
      member_id: "cafex-e6f5cd5e-116b-49a3-a3ca-8746779f3cc5"
    }

    assert good_request == Cafex.Protocol.Request.encode(request)
  end

  test "parse_response correctly parse a valid response" do
    response = <<0 :: 16-signed>>
    expected_response = %Response{error: :no_error}

    assert expected_response == LeaveGroup.decode(response)
  end
end
