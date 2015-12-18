defmodule Cafex.Protocol.SyncGroup.Test do
  use ExUnit.Case, async: true

  alias Cafex.Protocol.SyncGroup
  alias Cafex.Protocol.SyncGroup.Request
  alias Cafex.Protocol.SyncGroup.Response

  test "encode creates a valid non-leader SyncGroup request" do
    good_request = << 19 :: 16,
                      "cafex_test_consumer" :: binary,
                      1 :: 32,
                      42 :: 16,
                      "cafex-e6f5cd5e-116b-49a3-a3ca-8746779f3cc5",
                      0 :: 32>>

    request = %Request{
      group_id: "cafex_test_consumer",
      generation_id: 1,
      member_id: "cafex-e6f5cd5e-116b-49a3-a3ca-8746779f3cc5",
      group_assignment: []
    }

    assert good_request == Cafex.Protocol.Request.encode(request)
  end

  test "encode creates a valid leader SyncGroup request" do
    good_request = << 19 :: 16,
                      "cafex_test_consumer" :: binary,
                      1 :: 32,
                      42 :: 16,
                      "cafex-e6f5cd5e-116b-49a3-a3ca-8746779f3cc5" :: binary,

                      1 :: 32,

                      42 :: 16,
                      "cafex-e6f5cd5e-116b-49a3-a3ca-8746779f3cc5" :: binary,

                      46 :: 32,

                      0 :: 16,

                      1 :: 32,
                      10 :: 16,
                      "cafex_test" :: binary,

                      5 :: 32,
                      0 :: 32,
                      1 :: 32,
                      2 :: 32,
                      3 :: 32,
                      4 :: 32,

                      0 :: 32
                      >>

    request = %Request{
      group_id: "cafex_test_consumer",
      generation_id: 1,
      member_id: "cafex-e6f5cd5e-116b-49a3-a3ca-8746779f3cc5",
      group_assignment: [
        {"cafex-e6f5cd5e-116b-49a3-a3ca-8746779f3cc5", {0, [{"cafex_test", [0, 1, 2, 3, 4]}], ""}}
      ]
    }

    assert good_request == Cafex.Protocol.Request.encode(request)
  end

  test "parse_response correctly parse a valid response" do
    response = << 0 :: 16,
                  46 :: 32,
                  0 :: 16,

                  1 :: 32,
                  10 :: 16,
                  "cafex_test" :: binary,

                  5 :: 32,
                  0 :: 32,
                  1 :: 32,
                  2 :: 32,
                  3 :: 32,
                  4 :: 32,
                  0 :: 32>>

    expected_response = %Response{
      error: :no_error,
      member_assignment: {0, [{"cafex_test", [0, 1, 2, 3, 4]}], ""}}

    assert expected_response == SyncGroup.decode(response)
  end
end
