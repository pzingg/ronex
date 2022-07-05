defmodule FrameTest do
  use ExUnit.Case
  doctest Frame

  test "basic decode" do
    txt =
      "  \n*rga#1UQ8p+bart@1UQ8yk+lisa:0!\n    @(s+bart'H'@[r'e'@(t'l'@[T'l'@[i'o'\n    @(w+lisa' '@(x'w'@(y'o'@[1'r'@{a'l'@[2'd'@[k'!'"

    {:ok, frame} = Frame.parse(txt)
    assert length(frame) == 13

    rga = UUID.name("rga")
    {:ok, {bart, ""}} = UUID.parse("1UQ8p+bart")

    op = Enum.at(frame, 0)
    assert op.term == :header
    assert op.type == rga
    assert op.object == bart
    assert {:ok, {op.event, ""}} == UUID.parse("1UQ8yk+lisa")
    assert UUID.zero?(op.location)
    assert op.atoms == []

    op = Enum.at(frame, 1)
    assert op.term == :reduced
    assert op.type == rga
    assert op.object == bart
    assert {:ok, {op.event, ""}} == UUID.parse("1UQ8s+bart")
    assert UUID.zero?(op.location)
    assert op.atoms == ["H"]

    op = Enum.at(frame, 2)
    assert op.term == :reduced
    assert op.type == rga
    assert op.object == bart
    assert {:ok, {op.event, ""}} == UUID.parse("1UQ8sr+bart")
    assert UUID.zero?(op.location)
    assert op.atoms == ["e"]

    op = Enum.at(frame, 3)
    assert op.term == :reduced
    assert op.type == rga
    assert op.object == bart
    assert {:ok, {op.event, ""}} == UUID.parse("1UQ8t+bart")
    assert UUID.zero?(op.location)
    assert op.atoms == ["l"]

    op = Enum.at(frame, 4)
    assert op.term == :reduced
    assert op.type == rga
    assert op.object == bart
    assert {:ok, {op.event, ""}} == UUID.parse("1UQ8tT+bart")
    assert UUID.zero?(op.location)
    assert op.atoms == ["l"]

    op = Enum.at(frame, 5)
    assert op.term == :reduced
    assert op.type == rga
    assert op.object == bart
    assert {:ok, {op.event, ""}} == UUID.parse("1UQ8ti+bart")
    assert UUID.zero?(op.location)
    assert op.atoms == ["o"]

    op = Enum.at(frame, 6)
    assert op.term == :reduced
    assert op.type == rga
    assert op.object == bart
    assert {:ok, {op.event, ""}} == UUID.parse("1UQ8w+lisa")
    assert UUID.zero?(op.location)
    assert op.atoms == [" "]

    op = Enum.at(frame, 7)
    assert op.term == :reduced
    assert op.type == rga
    assert op.object == bart
    assert {:ok, {op.event, ""}} == UUID.parse("1UQ8x+lisa")
    assert UUID.zero?(op.location)
    assert op.atoms == ["w"]

    op = Enum.at(frame, 8)
    assert op.term == :reduced
    assert op.type == rga
    assert op.object == bart
    assert {:ok, {op.event, ""}} == UUID.parse("1UQ8y+lisa")
    assert UUID.zero?(op.location)
    assert op.atoms == ["o"]

    op = Enum.at(frame, 9)
    assert op.term == :reduced
    assert op.type == rga
    assert op.object == bart
    assert {:ok, {op.event, ""}} == UUID.parse("1UQ8y1+lisa")
    assert UUID.zero?(op.location)
    assert op.atoms == ["r"]

    op = Enum.at(frame, 10)
    assert op.term == :reduced
    assert op.type == rga
    assert op.object == bart
    assert {:ok, {op.event, ""}} == UUID.parse("1UQ8y1a+lisa")
    assert UUID.zero?(op.location)
    assert op.atoms == ["l"]

    op = Enum.at(frame, 11)
    assert op.term == :reduced
    assert op.type == rga
    assert op.object == bart
    assert {:ok, {op.event, ""}} == UUID.parse("1UQ8y2+lisa")
    assert UUID.zero?(op.location)
    assert op.atoms == ["d"]

    op = Enum.at(frame, 12)
    assert op.term == :reduced
    assert op.type == rga
    assert op.object == bart
    assert {:ok, {op.event, ""}} == UUID.parse("1UQ8yk+lisa")
    assert UUID.zero?(op.location)
    assert op.atoms == ["!"]
  end

  test "decode first" do
    txt = "*lww#test1!\n    *lww#test1@time:a'A';"
    {:ok, frame} = Frame.parse(txt)
    assert [op1 | [op2 | []]] = frame

    assert op1.term == :header
    assert op1.type == UUID.name("lww")
    assert op1.object == UUID.name("test1")
    assert UUID.zero?(op1.event)
    assert UUID.zero?(op1.location)
    assert op1.atoms == []

    assert op2.term == :raw
    assert op2.type == UUID.name("lww")
    assert op2.object == UUID.name("test1")
    assert {:ok, {op2.event, ""}} == UUID.parse("time-0")
    assert op2.location == UUID.name("a")
    assert op2.atoms == ["A"]
  end

  test "decode nothing" do
    {:ok, frame} = Frame.parse("")
    assert length(frame) == 0

    {:ok, frame} = Frame.parse(".")
    assert length(frame) == 0
  end

  test "decode error" do
    {:error, _} = Frame.parse("XX")
  end

  test "split frame" do
    txt = "*lww#test1!\n    *lww#test1@time:a'A';"
    split_frame = Frame.parse!(txt) |> Frame.split()
    assert [f1 | [f2 | []]] = split_frame

    assert length(f1) == 1
    op1 = hd(f1)
    assert op1.term == :header
    assert op1.type == UUID.name("lww")
    assert op1.object == UUID.name("test1")
    assert UUID.zero?(op1.event)
    assert UUID.zero?(op1.location)
    assert op1.atoms == []

    assert length(f2) == 1
    op2 = hd(f2)
    assert op2.term == :raw
    assert op2.type == UUID.name("lww")
    assert op2.object == UUID.name("test1")
    assert {:ok, {op2.event, ""}} == UUID.parse("time-0")
    assert op2.location == UUID.name("a")
    assert op2.atoms == ["A"]
  end
end
