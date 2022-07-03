defmodule UUIDTest do
  use ExUnit.Case
  use ExUnit.Parameterized
  use Bitwise, operators_only: true
  doctest UUID

  test "zero" do
    uuid = UUID.zero()
    assert uuid == %UUID{hi: 0, lo: 0, scheme: :name, variety: 0}
    assert UUID.is_zero?(uuid)
    assert to_string(uuid) == "0"
  end

  test "now" do
    uuid = UUID.now()
    assert uuid == %UUID{hi: 0, lo: 0, scheme: :event, variety: 0}
    assert to_string(uuid) == "0"
  end

  test "name" do
    uuid = %UUID{hi: 824_893_205_576_155_136, lo: 0, scheme: :name, variety: 0}
    assert to_string(uuid) == "inc"
  end

  test "parse 1" do
    assert {:ok, {uuid, ""}} = UUID.parse("1")
    assert uuid == %UUID{hi: 1 <<< 54, lo: 0, scheme: :name, variety: 0}
    assert to_string(uuid) == "1"
  end

  test "parse prefix 1" do
    assert {:ok, {uuid_a, ""}} = UUID.parse("1")
    assert {:ok, {uuid, ""}} = UUID.parse(")1", uuid_a)
    assert uuid == %UUID{hi: 1 <<< 54 ||| 1, lo: 0, scheme: :name, variety: 0}
    assert to_string(uuid) == "1000000001"
  end

  test "parse prefix in context" do
    assert {:ok, {uuid_hello, ""}} = UUID.parse("hello-111")
    assert {:ok, {uuid, ""}} = UUID.parse("[world-111", uuid_hello)
    assert {:ok, {uuid_hello_world, ""}} = UUID.parse("helloworld-111")
    assert uuid.scheme == :event
    assert uuid.variety == 0
    assert uuid == uuid_hello_world
    assert to_string(uuid) == "helloworld-111"
  end

  test "parse prefixes with no context" do
    assert {:ok, {uuid, ""}} = UUID.parse("[1s9L3-[Wj8oO")
    assert uuid.scheme == :event
    assert uuid.variety == 0
    assert to_string(uuid) == "[1s9L3-[Wj8oO"
  end

  test "parse error" do
    assert {:ok, {uuid, _}} = UUID.parse("erro_error$~~~~~~~~~~")
    assert uuid.scheme == :name
    assert uuid.variety == 0
    assert to_string(uuid) == "erro_error$~~~~~~~~~~"
  end

  test "parse ISBN" do
    assert {:ok, {uuid, ""}} = UUID.parse("1/978$1400075997")
    assert uuid.hi == 164_135_095_794_401_280
    assert uuid.lo == 19_140_298_535_113_287
    assert uuid.scheme == :name
    assert uuid.variety == 1
    assert to_string(uuid) == "1/978$1400075997"
  end

  test_with_params "parse and format",
                   fn ctx_str, uuid_str, expected ->
                     assert {:ok, {context, ""}} = UUID.parse(ctx_str)
                     assert {:ok, {uuid, ""}} = UUID.parse(uuid_str, context)
                     result = to_string(uuid)
                     assert result == expected
                     zipped = UUID.format_with_context(uuid, context)
                     assert zipped == uuid_str
                   end do
    [
      # 0
      {"0", "1", "1"},
      {"1-x", ")1", "1000000001-x"},
      {"test-1", "-", "test-1"},
      {"hello-111", "[world", "helloworld-111"},
      {"helloworld-111", "[", "hello-111"},
      # 5
      {"100001-orig", "[", "1-orig"},
      {"1+orig", "(2-", "10002-orig"},
      {"time+orig", "(1(2", "time1+orig2"},
      # TODO		{"name$user", "$scoped", "scoped$user"},
      {"any-thing", "hash%here", "hash%here"},
      {"[1s9L3-[Wj8oO", "-(2Biejq", "[1s9L3-(2Biejq"},
      # 10
      {"0123456789-abcdefghij", ")~)~", "012345678~-abcdefghi~"},
      {"(2-[1jHH~", "-[00yAl", "(2-}yAl"},
      {"0123G-abcdb", "(4566(efF", "01234566-abcdefF"}
    ]
  end
end
