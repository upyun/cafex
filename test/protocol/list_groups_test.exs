defmodule Cafex.Protocol.ListGroups.Test do
  use ExUnit.Case, async: true

  alias Cafex.Protocol.ListGroups
  alias Cafex.Protocol.ListGroups.Request
  alias Cafex.Protocol.ListGroups.Response

  test "encode creates a valid ListGroups request" do
    good_request = <<>>

    request = %Request{}
    assert good_request == Cafex.Protocol.Request.encode(request)
  end

  test "parse_response correctly parse a valid response" do
    response = << 0 :: 16-signed,
                  1 :: 32-signed,
                  19 :: 16-signed,
                  "cafex_test_consumer" :: binary,
                  8 :: 16-signed,
                  "consumer" :: binary >>

    expected_response = %Response{error: :no_error,
      groups: [{"cafex_test_consumer", "consumer"}]}

    assert expected_response == ListGroups.decode(response)
  end
end
