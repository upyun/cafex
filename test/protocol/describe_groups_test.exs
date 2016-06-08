defmodule Cafex.Protocol.DescribeGroups.Test do
  use ExUnit.Case, async: true

  alias Cafex.Protocol.DescribeGroups
  alias Cafex.Protocol.DescribeGroups.Request
  alias Cafex.Protocol.DescribeGroups.Response

  test "DescribeGroup protocol implementation" do
    req = %Request{}
    assert DescribeGroups.has_response?(req) == true
    assert DescribeGroups.decoder(req) == DescribeGroups
    assert DescribeGroups.api_key(req) == Cafex.Protocol.api_key(:describe_groups)
    assert DescribeGroups.api_version(req) == 0
  end

  test "encode creates a valid DescribeGroups request" do
    good_request = <<1 :: 32, 19 :: 16, "cafex_test_consumer" :: binary >>

    request = %Request{groups: ["cafex_test_consumer"]}
    assert good_request == Cafex.Protocol.Request.encode(request)
  end

  test "parse_response correctly parse a valid response" do
    response = << 1 :: 32,

                  0 :: 16,
                  19 :: 16,
                  "cafex_test_consumer" :: binary,
                  6 :: 16-signed,
                  "Stable" :: binary,
                  8 :: 16,
                  "consumer" :: binary,
                  5 :: 16,
                  "cafex" :: binary,

                  1 :: 32,

                  42 :: 16,
                  "cafex-4e8326b6-66d5-4ba5-877e-9feb4182462e" :: binary,
                  5 :: 16,
                  "cafex" :: binary,
                  13 :: 16,
                  "/192.168.99.1" :: binary,
                  22 :: 32, # metadata length
                  0 :: 16,
                  1 :: 32,
                  10 :: 16,
                  "cafex_test" :: binary,
                  0 :: 32,

                  34 :: 32, # assignment length
                  0 :: 16,
                  1 :: 32,
                  10 :: 16,
                  "cafex_test" :: binary,
                  2 :: 32,
                  3 :: 32,
                  4 :: 32,
                  0 :: 32
                  >>

    expected_response = %Response{
      groups: [
        %{error: :no_error,
          group_id: "cafex_test_consumer",
          state: "Stable",
          protocol_type: "consumer",
          protocol: "cafex",
          members: [%{
              member_id: "cafex-4e8326b6-66d5-4ba5-877e-9feb4182462e",
              client_id: "cafex",
              client_host: "/192.168.99.1",
              member_metadata: {0, ["cafex_test"], ""},
              member_assignment: {0, [{"cafex_test", [3, 4]}], ""}
            }]}
      ]}

    assert expected_response == DescribeGroups.decode(response)
  end
end
