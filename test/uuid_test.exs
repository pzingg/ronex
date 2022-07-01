defmodule UUIDTest do
  use ExUnit.Case
  use Bitwise, operators_only: true
  doctest UUID

  test "now" do
    assert UUID.now() == %UUID{hi: 0, lo: 0}
  end

  test "zero" do
    assert UUID.zero() |> to_string == "0"
    assert UUID.zero() |> UUID.is_zero?()
  end

  test "names" do
    assert %UUID{hi: 824_893_205_576_155_136, lo: 0, scheme: :name} |> to_string == "inc"
  end

  test "parse 1" do
    assert {:ok, {uuid, ""}} = UUID.parse("1")
    assert uuid.scheme == :name
    assert uuid.variety == 0
    assert uuid.hi == 1 <<< 54
    assert uuid.lo == 0
  end

  test "parse prefix 1" do
    assert {:ok, {uuid_a, ""}} = UUID.parse("1")
    assert {:ok, {uuid, ""}} = UUID.parse(")1", uuid_a, :nil)
    assert uuid.scheme == :name
    assert uuid.variety == 0
    assert uuid.hi == (1 <<< 54 ||| 1)
    assert uuid.lo == 0
  end

  test "parse prefix 2" do
    assert {:ok, {uuid_hello, ""}} = UUID.parse("hello-111")
    assert {:ok, {uuid, ""}} = UUID.parse("[world-111", uuid_hello, :nil)
    assert {:ok, {uuid_hello_world, ""}} = UUID.parse("helloworld-111")
    assert uuid.scheme == :event
    assert uuid.variety == 0
    assert uuid == uuid_hello_world
  end

  test "fail parse error" do
    assert {:ok, {uuid, _}} = UUID.parse("erro_error$~~~~~~~~~~")
    assert uuid.scheme == :name
    assert uuid.variety == 0
  end

  test "parse ISBN" do
    assert {:ok, {uuid, ""}} = UUID.parse("1/978$1400075997")
    assert uuid.scheme == :name
    assert uuid.variety == 1
    assert uuid.hi == 164_135_095_794_401_280
    assert uuid.lo == 19_140_298_535_113_287
  end


end
