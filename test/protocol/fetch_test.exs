defmodule Cafex.Protocol.Fetch.Test do
  use ExUnit.Case, async: true

  alias Cafex.Protocol.Message
  alias Cafex.Protocol.Fetch
  alias Cafex.Protocol.Fetch.Request
  alias Cafex.Protocol.Fetch.Response

  test "encode creates a valid fetch request" do
    good_request = << -1 :: 32, 10 :: 32, 1 :: 32,
                      1 :: 32, 3 :: 16, "bar" :: binary,
                        1 :: 32, 0 :: 32, 1 :: 64, 10000 :: 32 >>
    request = %Request{ replica_id: -1,
                        max_wait_time: 10,
                        min_bytes: 1,
                        topics: [{"bar", [{0, 1, 10000}]}] }
    assert good_request == Fetch.encode(request)
  end

  test "parse_response correctly parses a valid response with a key and a value" do
    response = << 1 :: 32, 3 :: 16, "bar" :: binary,
                  1 :: 32, 0 :: 32, 0 :: 16, 10 :: 64,
                  36 :: 32,
                    1 :: 32,
                    1 :: 64, 20 :: 32, 0 :: 32, 0 :: 8, 0 :: 8,
                    3 :: 32, "foo" :: binary,
                    3 :: 32, "bar" :: binary >>

    expected_response = %Response{topics: [{"bar", [
                          %{error: :no_error,
                            hwm_offset: 10,
                            partition: 0,
                            messages: [%Message{attributes: 0,
                                                key: "foo",
                                                offset: 1,
                                                value: "bar"}]
                           }]}]}
    assert expected_response == Fetch.decode(response)
  end

  test "parse_response correctly parses a response with excess bytes" do
    response = << 1 :: 32, 4 :: 16, "food" :: binary,
                  1 :: 32, 0 :: 32, 0 :: 16, 56 :: 64,
                  91 :: 32,
                    3 :: 32,
                    0 :: 64, 17 :: 32, 0 :: 32, 0 :: 8, 0 :: 8,
                    -1 :: 32, 3 :: 32, "hey" :: binary,
                    1 :: 64, 17 :: 32, 0 :: 32, 0 :: 8, 0 :: 8,
                    -1 :: 32, 3 :: 32, "hey" :: binary,
                    2 :: 64, 17 :: 32, 0 :: 32, 0 :: 8, 0 :: 8,
                    -1 :: 32, 3 :: 32, "hey" :: binary >>

    expected_response = %Response{topics: [{"food", [
          %{error: :no_error, hwm_offset: 56, partition: 0, messages: [
              %Message{attributes: 0, key: nil, offset: 0, value: "hey"},
              %Message{attributes: 0, key: nil, offset: 1, value: "hey"},
              %Message{attributes: 0, key: nil, offset: 2, value: "hey"}
            ]}]}]}
    assert expected_response == Fetch.decode(response)
  end

  test "parse_response correctly parses a valid response with a nil key and a value" do
    response = << 1 :: 32, 3 :: 16, "bar" :: binary,
                  1 :: 32, 0 :: 32, 0 :: 16, 10 :: 64,
                  33 :: 32,
                    1 :: 32,
                    1 :: 64, 17 :: 32, 0 :: 32, 0 :: 8, 0 :: 8, -1 :: 32, 3 :: 32, "bar" :: binary >>
    expected_response = %Response{topics: [{"bar", [
            %{error: :no_error, hwm_offset: 10, partition: 0, messages: [
                %Message{attributes: 0, key: nil, offset: 1, value: "bar"}
              ]}]}]}
    assert expected_response == Fetch.decode(response)
  end
end
