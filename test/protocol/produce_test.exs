defmodule Cafex.Protocol.Produce.Test do
  use ExUnit.Case, async: true

  alias Cafex.Protocol.Message
  alias Cafex.Protocol.Produce
  alias Cafex.Protocol.Produce.Request

  test "create_request creates a valid payload" do
    expected_request = <<0,1,0,0,0,10,0,0,0,1,0,4,102,111,111,100,0,0,0,1,0,0,0,0,0,0,0,29,0,0,0,0,0,0,0,0,0,0,0,17,254,46,107,157,0,0,255,255,255,255,0,0,0,3,104,101,121>>

    request = %Request{ required_acks: 1,
                        timeout: 10,
                        messages: [
                          Message.from_tuple({"food", 0, "hey", ""})
                        ] }

    assert expected_request == Produce.encode(request)
  end

  test "create_request correctly batches multiple request messages" do
    expected_request = <<0,1,0,0,0,10,0,0,0,1,0,4,102,111,111,100,0,0,0,1,0,0,0,0,0,0,0,88,0,0,0,0,0,0,0,0,0,0,0,17,254,46,107,157,0,0,255,255,255,255,0,0,0,3,104,101,121,0,0,0,0,0,0,0,0,0,0,0,16,253,110,189,219,0,0,255,255,255,255,0,0,0,2,104,105,0,0,0,0,0,0,0,0,0,0,0,19,135,167,122,178,0,0,255,255,255,255,0,0,0,5,104,101,108,108,111>>

    request = %Request{ required_acks: 1,
                        timeout: 10,
                        messages: [
                          Message.from_tuple({"food", 0, "hey", ""}),
                          Message.from_tuple({"food", 0, "hi", ""}),
                          Message.from_tuple({"food", 0, "hello", ""})
                        ] }

    assert expected_request == Produce.encode(request)
  end

  test "decode_response correctly parses a valid response with single topic and partition" do
    response = << 1 :: 32, 3 :: 16, "bar" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 10 :: 64 >>
    expected_response = [{"bar", [%{error_code: 0, offset: 10, partition: 0}]}]
    assert expected_response == Produce.decode(response)
  end

  test "parse_response correctly parses a valid response with multiple topics and partitions" do
    response = << 2 :: 32,
                  3 :: 16, "bar" :: binary,
                    2 :: 32, 0 :: 32, 0 :: 16, 10 :: 64,
                             1 :: 32, 0 :: 16, 20 :: 64,
                  3 :: 16, "baz" :: binary,
                    2 :: 32, 0 :: 32, 0 :: 16, 30 :: 64,
                             1 :: 32, 0 :: 16, 40 :: 64 >>
    expected_response = [{"bar", [%{error_code: 0, offset: 10, partition: 0},
                                  %{error_code: 0, offset: 20, partition: 1}]},
                         {"baz", [%{error_code: 0, offset: 30, partition: 0},
                                  %{error_code: 0, offset: 40, partition: 1}]}]
    assert expected_response == Produce.decode(response)
  end
end
