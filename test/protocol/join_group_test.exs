defmodule Cafex.Protocol.JoinGroup.Test do
  use ExUnit.Case, async: true

  alias Cafex.Protocol.JoinGroup
  alias Cafex.Protocol.JoinGroup.Request
  alias Cafex.Protocol.JoinGroup.Response

  test "JoinGroup protocol implementation" do
    req = %Request{}
    assert JoinGroup.has_response?(req) == true
    assert JoinGroup.decoder(req) == JoinGroup
    assert JoinGroup.api_key(req) == Cafex.Protocol.api_key(:join_group)
    assert JoinGroup.api_version(req) == 0
  end

  test "encode creates a valid JoinGroup request" do
    good_request = << 19 :: 16,
                      "cafex_test_consumer" :: binary,

                      7000 :: 32, # session_timeout
                      -1 :: 16-signed,   # member_id: nil

                      8 :: 16,
                      "consumer" :: binary,

                      1 :: 32, # group_protocols length

                      5 :: 16,
                      "cafex" :: binary,

                      22 :: 32, # group_protocols_metadata length
                      0 :: 16,
                      1 :: 32,
                      10 :: 16,
                      "test_topic" :: binary,
                      0 :: 32-signed>>

    request = %Request{
      group_id: "cafex_test_consumer",
      session_timeout: 7000,
      protocol_type: "consumer",
      group_protocols: [{"cafex", {0, ["test_topic"], ""}}]
    }

    assert good_request == Cafex.Protocol.Request.encode(request)
  end

  test "parse_response correctly parse a valid non-leader response" do
    response = << 0 :: 16, # error_code
                  2 :: 32, # generation_id
                  5 :: 16,
                  "cafex", # group_protocol

                  42 :: 16,
                  "cafex-e6f5cd5e-116b-49a3-a3ca-8746779f3cc5",  # leader_id

                  42 :: 16,
                  "cafex-4e8326b6-66d5-4ba5-877e-9feb4182462e",  # member_id

                  0 :: 32-signed # members
                  >>

    expected_response = %Response{
      error: :no_error,
      generation_id: 2,
      group_protocol: "cafex",
      leader_id: "cafex-e6f5cd5e-116b-49a3-a3ca-8746779f3cc5",
      member_id: "cafex-4e8326b6-66d5-4ba5-877e-9feb4182462e",
      members: []}

    assert expected_response == JoinGroup.decode(response)
  end

  test "parse_response correctly parse a valid leader response" do
    response = << 0 :: 16-signed, # error_code
                  3 :: 32, # generation_id
                  5 :: 16,
                  "cafex", # group_protocol

                  42 :: 16,
                  "cafex-4e8326b6-66d5-4ba5-877e-9feb4182462e",  # leader_id

                  42 :: 16,
                  "cafex-4e8326b6-66d5-4ba5-877e-9feb4182462e",  # member_id

                  1 :: 32, # members

                  42 :: 16, # member_id
                  "cafex-4e8326b6-66d5-4ba5-877e-9feb4182462e",

                  27 :: 32,
                  0 :: 16, # version
                  1 :: 32, # subscriptions
                  10 :: 16,
                  "cafex_test",
                  5 :: 32,
                  "[3,4]"
                  >>

    expected_response = %Response{
      error: :no_error,
      generation_id: 3,
      group_protocol: "cafex",
      leader_id: "cafex-4e8326b6-66d5-4ba5-877e-9feb4182462e",
      member_id: "cafex-4e8326b6-66d5-4ba5-877e-9feb4182462e",
      members: [
        {"cafex-4e8326b6-66d5-4ba5-877e-9feb4182462e", {0, ["cafex_test"], "[3,4]"}}]}

    assert expected_response == JoinGroup.decode(response)
  end
end
