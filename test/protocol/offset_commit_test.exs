defmodule Cafex.Protocol.OffsetCommit.Test do
  use ExUnit.Case, async: true

  alias Cafex.Protocol.OffsetCommit
  alias Cafex.Protocol.OffsetCommit.Request
  alias Cafex.Protocol.OffsetCommit.Response

  test "create_request creates a valid offset commit message" do
    offset_commit_request = %Request{consumer_group: "bar",
                                     topics: [{"foo", [{0, 10, "baz"}]}]}

    good_request = << 3 :: 16, "bar", 1 :: 32, 3 :: 16, "foo", 1 :: 32, 0 :: 32, 10 :: 64, 3 :: 16, "baz" >>

    request = OffsetCommit.encode(offset_commit_request)

    assert request == good_request
  end

  test "parse_response correctly parses a valid response" do
    response = <<0, 0, 0, 1, 0, 4, 102, 111, 111, 100, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0>>
    assert OffsetCommit.decode(response) == %Response{topics: [{"food", [{0, 0}]}]}
  end
end
