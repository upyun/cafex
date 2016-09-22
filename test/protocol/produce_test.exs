defmodule Cafex.Protocol.Produce.Test do
  use ExUnit.Case, async: true

  alias Cafex.Protocol.Message
  alias Cafex.Protocol.Produce
  alias Cafex.Protocol.Produce.Request
  alias Cafex.Protocol.Produce.Response

  test "Produce protocol implementation" do
    req = %Request{}
    assert Produce.has_response?(%{req | required_acks: 0}) == false
    assert Produce.has_response?(%{req | required_acks: 1}) == true
    assert Produce.has_response?(%{req | required_acks: 2}) == true
    assert Produce.decoder(req) == Produce
    assert Produce.api_key(req) == Cafex.Protocol.api_key(:produce)
    assert Produce.api_version(req) == 0
  end

  test "create_request creates a valid payload with nil key" do
    required_acks = 1
    timeout = 10
    topic = "food"
    value = "hey"
    key = nil
    partition = 0

    sub = << 1 :: 8, 0 :: 8, -1 :: 32, byte_size(value) :: 32, value :: binary>>
    crc = :erlang.crc32(sub)
    msg = <<crc :: 32,  sub :: binary>>
    msg_bin = <<0 :: 64, byte_size(msg) :: 32, msg :: binary >>
    expected_request = << 1 :: 16,
                          10 :: 32,
                          1 :: 32,
                          byte_size(topic) :: 16,
                          topic :: binary,
                          1 :: 32,
                          partition :: 32,
                          byte_size(msg_bin) :: 32,
                          msg_bin :: binary >>

    request = %Request{ required_acks: required_acks,
                        timeout: timeout,
                        messages: [
                          Message.from_tuple({topic, partition, value, key})
                        ] }

    assert expected_request == Produce.encode(request)
  end

  test "create_request creates a valid payload with empty string key with message version 0" do
    required_acks = 1
    timeout = 10
    topic = "food"
    partition = 0
    value = "hey"
    key = ""

    sub = << 0 :: 8, 0 :: 8, 0 :: 32, byte_size(value) :: 32, value :: binary>>
    crc = :erlang.crc32(sub)
    msg = <<crc :: 32,  sub :: binary>>
    msg_bin = <<0 :: 64, byte_size(msg) :: 32, msg :: binary >>
    expected_request = << 1 :: 16,
                          10 :: 32,
                          1 :: 32,
                          byte_size(topic) :: 16,
                          topic :: binary,
                          1 :: 32,
                          partition :: 32,
                          byte_size(msg_bin) :: 32,
                          msg_bin :: binary >>

    request = %Request{ required_acks: required_acks,
                        timeout: timeout,
                        messages: [
                          # Message.from_tuple({topic, partition, value, key})
                          %Message{topic: topic,
                                   partition: partition,
                                   key: key,
                                   value: value,
                                   magic_byte: 0}
                        ] }

    assert expected_request == Produce.encode(request)
  end

  test "create_request creates a valid payload with empty string key with message version 1" do
    required_acks = 1
    timeout = 10
    topic = "food"
    partition = 0
    value = "hey"
    key = ""

    sub = << 1 :: 8, 0 :: 4, 0 :: 1, 0 :: 3, 0 :: 64 , 0 :: 32, byte_size(value) :: 32, value :: binary>>
    crc = :erlang.crc32(sub)
    msg = <<crc :: 32,  sub :: binary>>
    msg_bin = <<0 :: 64, byte_size(msg) :: 32, msg :: binary >>
    expected_request = << 1 :: 16,
                          10 :: 32,
                          1 :: 32,
                          byte_size(topic) :: 16,
                          topic :: binary,
                          1 :: 32,
                          partition :: 32,
                          byte_size(msg_bin) :: 32,
                          msg_bin :: binary >>

    request = %Request{ required_acks: required_acks,
                        timeout: timeout,
                        messages: [
                          %Message{topic: topic,
                                   partition: partition,
                                   key: key,
                                   value: value,
                                   magic_byte: 1,
                                   timestamp_type: :create_time}
                        ] }

    assert expected_request == Produce.encode(request)
  end

  test "create_request correctly batches multiple request messages" do
    required_acks = 1
    timeout = 10
    topic = "food"
    partition = 0
    value1 = "{\"id\":1, \"name\": \"user1\"}"
    value2 = "{\"id\":2, \"name\": \"user2\"}"
    value3 = "{\"id\":3, \"name\": \"user3\"}"
    key = ""

    sub = << 1 :: 8, 0 :: 8, 0 :: 32, byte_size(value1) :: 32, value1 :: binary>>
    crc = :erlang.crc32(sub)
    msg = <<crc :: 32,  sub :: binary>>
    msg_bin = <<0 :: 64, byte_size(msg) :: 32, msg :: binary >>

    sub = << 1 :: 8, 0 :: 8, 0 :: 32, byte_size(value2) :: 32, value2 :: binary>>
    crc = :erlang.crc32(sub)
    msg = <<crc :: 32,  sub :: binary>>
    msg_bin = msg_bin <> <<1 :: 64, byte_size(msg) :: 32, msg :: binary >>

    sub = << 1 :: 8, 0 :: 8, 0 :: 32, byte_size(value3) :: 32, value3 :: binary>>
    crc = :erlang.crc32(sub)
    msg = <<crc :: 32,  sub :: binary>>
    orig_msg_bin = msg_bin <> <<2 :: 64, byte_size(msg) :: 32, msg :: binary >>

    expected_request = << 1 :: 16,
                          10 :: 32,
                          1 :: 32,
                          byte_size(topic) :: 16,
                          topic :: binary,
                          1 :: 32,
                          partition :: 32,
                          byte_size(orig_msg_bin) :: 32,
                          orig_msg_bin :: binary >>

    request = %Request{ required_acks: required_acks,
                        timeout: timeout,
                        messages: [
                          %Message{topic: topic, partition: partition, key: key, value: value1, offset: 0},
                          %Message{topic: topic, partition: partition, key: key, value: value2, offset: 1},
                          %Message{topic: topic, partition: partition, key: key, value: value3, offset: 2},
                        ] }

    assert expected_request == Produce.encode(request)

    compressed = :zlib.gzip(orig_msg_bin)

    sub = << 1 :: 8, 1 :: 8, -1 :: 32, byte_size(compressed) :: 32, compressed :: binary>>
    crc = :erlang.crc32(sub)
    msg = <<crc :: 32,  sub :: binary>>
    msg_bin = <<0 :: 64, byte_size(msg) :: 32, msg :: binary >>

    expected_request = << 1 :: 16,
                          10 :: 32,
                          1 :: 32,
                          byte_size(topic) :: 16,
                          topic :: binary,
                          1 :: 32,
                          partition :: 32,
                          byte_size(msg_bin) :: 32,
                          msg_bin :: binary >>

    request = %{ request | compression: :gzip }

    assert expected_request == Produce.encode(request)

    {:ok, compressed} = :snappy.compress(orig_msg_bin)
    sub = << 1 :: 8, 2 :: 8, -1 :: 32, byte_size(compressed) :: 32, compressed :: binary>>
    crc = :erlang.crc32(sub)
    msg = <<crc :: 32,  sub :: binary>>
    msg_bin = <<0 :: 64, byte_size(msg) :: 32, msg :: binary >>

    expected_request = << 1 :: 16,
                          10 :: 32,
                          1 :: 32,
                          byte_size(topic) :: 16,
                          topic :: binary,
                          1 :: 32,
                          partition :: 32,
                          byte_size(msg_bin) :: 32,
                          msg_bin :: binary >>

    request = %{ request | compression: :snappy }

    assert expected_request == Produce.encode(request)
  end

  test "decode_response correctly parses a valid response with single topic and partition" do
    response = << 1 :: 32, 3 :: 16, "bar" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 10 :: 64 >>
    expected_response = %Response{topics: [{"bar", [%{error: :no_error, offset: 10, partition: 0}]}]}
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
    expected_response = %Response{topics: [{"bar", [%{error: :no_error, offset: 10, partition: 0},
                                  %{error: :no_error, offset: 20, partition: 1}]},
                         {"baz", [%{error: :no_error, offset: 30, partition: 0},
                                  %{error: :no_error, offset: 40, partition: 1}]}]}
    assert expected_response == Produce.decode(response)
  end
end
