defmodule Cafex.Protocol.OffsetFetch.Test do
  use ExUnit.Case, async: true

  alias Cafex.Protocol.OffsetFetch
  alias Cafex.Protocol.OffsetFetch.Request
  alias Cafex.Protocol.OffsetFetch.Response

  test "OffsetFetch protocol implementation" do
    req = %Request{}
    assert OffsetFetch.has_response?(req) == true
    assert OffsetFetch.decoder(req) == OffsetFetch
    assert OffsetFetch.api_key(req) == Cafex.Protocol.api_key(:offset_fetch)
    assert OffsetFetch.api_version(%{req | api_version: 0}) == 0
    assert OffsetFetch.api_version(%{req | api_version: 1}) == 1
  end

  test "create_request creates a valid offset commit message with default version 0" do
    offset_commit_request_default = %Request{ consumer_group: "bar",
                                              topics: [{"foo", [0]}] }
    offset_commit_request_v0      = %Request{ api_version: 0,
                                              consumer_group: "bar",
                                              topics: [{"foo", [0]}] }
    offset_commit_request_v1      = %Request{ api_version: 1,
                                              consumer_group: "bar",
                                              topics: [{"foo", [0]}] }
    offset_commit_request_v2      = %Request{ api_version: 2,
                                              consumer_group: "bar",
                                              topics: [{"foo", [0]}] }
    good_request = << 3 :: 16, "bar" :: binary, 1 :: 32, 3 :: 16, "foo" :: binary, 1 :: 32, 0 :: 32 >>
    request_default = OffsetFetch.encode(offset_commit_request_default)
    request_v0 = OffsetFetch.encode(offset_commit_request_v0)
    request_v1 = OffsetFetch.encode(offset_commit_request_v1)
    request_v2 = OffsetFetch.encode(offset_commit_request_v2)
    assert request_default == good_request
    assert request_v0 == good_request
    assert request_v1 == good_request
    assert request_v2 == good_request
  end

  test "parse_response correctly parses a valid response" do
    response1 = <<0, 0, 0, 1, 0, 4, 102, 111, 111, 100, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0>>
    # response1 = <<1 :: 32, 4 :: 16, "food", 1 :: 32, 0 :: 32, 9 :: 64, 0 :: 16, 0 :: 16>>
    assert OffsetFetch.decode(response1) == %Response{topics: [{"food", [{0, 9, "", :no_error}]}]}

    response2 = <<0, 0, 0, 1, 0, 4, 102, 111, 111, 100, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 4, 255, 255, 0, 0>>
    # response2 = << 1 :: 32, 4 :: 16, "food", 1 :: 32, 1 :: 32, 4 :: 64, -1 :: 16, 0 :: 16>>
    assert OffsetFetch.decode(response2) == %Response{topics: [{"food", [{1, 4, "", :no_error}]}]}
  end
end
