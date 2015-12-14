defmodule Cafex.Protocol.GroupCoordinator.Test do
  use ExUnit.Case, async: true

  alias Cafex.Protocol.GroupCoordinator
  alias Cafex.Protocol.GroupCoordinator.Request
  alias Cafex.Protocol.GroupCoordinator.Response

  test "create_request creates a valid consumer metadata request" do
    good_request = << 2 :: 16, "we" >>

    request = %Request{group_id: "we"}

    assert good_request == GroupCoordinator.encode(request)
  end

  test "parse_response correctly parses a valid response" do
    response = <<0, 0, 0, 0, 192, 6, 0, 14, 49, 57, 50, 46, 49, 54, 56, 46, 53, 57, 46, 49, 48, 51, 0, 0, 192, 6>>

    assert GroupCoordinator.decode(response) == %Response{ coordinator_id: 49158,
                                                           coordinator_host: "192.168.59.103",
                                                           coordinator_port: 49158,
                                                           error: :no_error }
  end
end
