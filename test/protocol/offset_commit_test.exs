defmodule Cafex.Protocol.OffsetCommit.Test do
  use ExUnit.Case, async: true

  alias Cafex.Protocol.OffsetCommit
  alias Cafex.Protocol.OffsetCommit.Request
  alias Cafex.Protocol.OffsetCommit.Response

  test "create_request creates a valid offset commit message with default version 0" do
    offset_commit_request_default = %Request{consumer_group: "bar",
                                             topics: [{"foo", [{0, 10, "baz"}]}]}
    offset_commit_request_v0      = %Request{api_version: 0,
                                             consumer_group: "bar",
                                             topics: [{"foo", [{0, 10, "baz"}]}]}

    good_request = << 3 :: 16, "bar", 1 :: 32, 3 :: 16, "foo", 1 :: 32, 0 :: 32, 10 :: 64, 3 :: 16, "baz" >>

    request_default = OffsetCommit.encode(offset_commit_request_default)
    request_v0      = OffsetCommit.encode(offset_commit_request_v0)

    assert request_default == good_request
    assert request_v0 == good_request
  end

  test "create_request creates a valid offset commit message with version 1" do
    offset_commit_request_v1      = %Request{api_version: 1,
                                             consumer_group: "bar",
                                             consumer_group_generation_id: 5,
                                             consumer_id: "consumer1",
                                             topics: [{"foo", [{0, 10, 1447049805, "baz"}]}]}

    good_request = << 3 :: 16, "bar", 5 :: 32, 9 :: 16, "consumer1", 1 :: 32, 3 :: 16, "foo", 1 :: 32, 0 :: 32, 10 :: 64, 1447049805 :: 64, 3 :: 16, "baz" >>

    request_v1      = OffsetCommit.encode(offset_commit_request_v1)

    assert request_v1 == good_request
  end

  test "create_request creates a valid offset commit message with version 2" do
    offset_commit_request_v2      = %Request{api_version: 2,
                                             consumer_group: "bar",
                                             consumer_group_generation_id: 5,
                                             consumer_id: "consumer1",
                                             retention_time: 1447049805,
                                             topics: [{"foo", [{0, 10, "baz"}]}]}

    good_request = << 3 :: 16, "bar", 5 :: 32, 9 :: 16, "consumer1", 1447049805 :: 64, 1 :: 32, 3 :: 16, "foo", 1 :: 32, 0 :: 32, 10 :: 64, 3 :: 16, "baz" >>

    request_v2      = OffsetCommit.encode(offset_commit_request_v2)

    assert request_v2 == good_request
  end

  test "parse_response correctly parses a valid response" do
    response = <<0, 0, 0, 1, 0, 4, 102, 111, 111, 100, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0>>
    assert OffsetCommit.decode(response) == %Response{topics: [{"food", [{0, :no_error}]}]}
  end
end
