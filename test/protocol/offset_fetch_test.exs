defmodule Cafex.Protocol.OffsetFetch.Test do
  use ExUnit.Case, async: true

  alias Cafex.Protocol.OffsetFetch
  alias Cafex.Protocol.OffsetFetch.Request
  alias Cafex.Protocol.OffsetFetch.Response

  test "create_request creates a valid offset commit message" do
    offset_commit_request = %Request{ consumer_group: "bar",
                                      topics: [{"foo", [0]}] }
    good_request = << 3 :: 16, "bar" :: binary, 1 :: 32, 3 :: 16, "foo" :: binary, 1 :: 32, 0 :: 32 >>
    request = OffsetFetch.encode(offset_commit_request)
    assert request == good_request
  end

  test "parse_response correctly parses a valid response" do
    response = <<0, 0, 0, 1, 0, 4, 102, 111, 111, 100, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0>>
    assert OffsetFetch.decode(response) == %Response{topics: [{"food", [{0, 9, "", 0}]}]}
  end
end
