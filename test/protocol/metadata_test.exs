defmodule Cafex.Protocol.Metadata.Test do
  use ExUnit.Case, async: true

  alias Cafex.Protocol.Metadata
  alias Cafex.Protocol.Metadata.Request
  alias Cafex.Protocol.Metadata.Response

  test "create_request with no topics creates a valid metadata request" do
    good_request = << 0 :: 32 >>
    request = %Request{}
    assert good_request == Cafex.Protocol.Request.encode(request)
  end

  test "create_request with a single topic creates a valid metadata request" do
    good_request = << 1 :: 32, 3 :: 16, "bar" :: binary >>
    request = %Request{topics: ["bar"]}
    assert good_request == Cafex.Protocol.Request.encode(request)
  end

  test "create_request with a multiple topics creates a valid metadata request" do
    good_request = << 3 :: 32, 3 :: 16, "bar" :: binary, 3 :: 16, "baz" :: binary, 4 :: 16, "food" :: binary >>
    request = %Request{topics: ["bar", "baz", "food"]}
    assert good_request == Cafex.Protocol.Request.encode(request)
  end

  test "parse_response correctly parses a valid response" do
    response = << 1 :: 32, 0 :: 32, 3 :: 16, "foo" :: binary, 9092 :: 32,
                  1 :: 32, 0 :: 16, 3 :: 16, "bar" :: binary,
                  1 :: 32, 0 :: 16, 0 :: 32, 0 :: 32, 0 :: 32, 1 :: 32, 0 :: 32 >>

    expected_response = %Response{
      brokers: [%{host: "foo", node_id: 0, port: 9092}],
      topics: [
        %{name: "bar", error: :no_error, partitions: [
          %{error: :no_error, isrs: [0], leader: 0, partition_id: 0, replicas: []}
        ]}
      ]
    }

    assert expected_response == Metadata.decode(response)
  end
end
