defmodule SetTest do
  use ExUnit.Case, async: false
  use ExUnit.Parameterized
  doctest Crdt.Set

  test_with_params "ron-test 04-set-basic", fn i, st, upd, out -> test_set(i, st, upd, out) end do
    [
      {1, "*set#test1@1=1;", "*set#test1@2=2;", "*set#test1@2:1!:0=2@1=1"},
      {2, "*set#test2@1!@=1", "*set#test2@2:1;", "*set#test2@2!:1,"},
      {3, "*set#test3@3:1;", "*set#test3@4:2;", "*set#test3@4:d!:2,@3:1,"},
      {4, "*set#test4@2!@=2@1=1", "*set#test4@5!@=5@3:2,@4:1,", "*set#test4@5!@=5@3:2,@4:1,"},
      {5, "*set#test5@2!@=2@1=1", ["*set#test5@3!@:2,@4:1,", "*set#test5@5!@=5"],
       "*set#test5@5!@=5@3:2,@4:1,"},
      {6, "*set#test6@3!@:2,@4:1,", ["*set#test6@5!@=5", "*set#test6@2!@=2@1=1"],
       "*set#test6@5000000001!@5=5@3:2,@4:1,"},
      {"mice", "*set#mice@1YKDY54a01+1YKDY5!>mouse$1YKDY5",
       "*set#mice@1YKDXO3201+1YKDXO?!@>mouse$1YKDXO@(WBF901(WBY>mouse$1YKDWBY@[67H01[6>mouse$1YKDW6@(Uh4j01(Uh>mouse$1YKDUh@(S67V01(S6>mouse$1YKDS6@(Of(N3:1YKDN3DS01+1YKDN3,@(MvBV01(IuJ:0>mouse$1YKDIuJ@(LF:1YKDIuEY01+1YKDIuJ,:{A601,@(Io5l01[oA:0>mouse$1YKDIoA@[l7_01[l>mouse$1YKDIl@(57(4B:1YKD4B3f01+1YKD4B,@(0bB401+1YKCsd:0>mouse$1YKCsd@1YKCu6+:1YKCsd7Q01+1YKCsd,",
       "*set#mice@1YKDXO3201+1YKDXO!@(Y54a01(Y5>mouse$1YKDY5@(XO3201(XO>mouse$1YKDXO@(WBF901(WBY>mouse$1YKDWBY@[67H01[6>mouse$1YKDW6@(Uh4j01(Uh>mouse$1YKDUh@(S67V01(S6>mouse$1YKDS6@(Of(N3:1YKDN3DS01+1YKDN3,@(MvBV01(IuJ:0>mouse$1YKDIuJ@(LF:1YKDIuEY01+1YKDIuJ,:{A601,@(Io5l01[oA:0>mouse$1YKDIoA@[l7_01[l>mouse$1YKDIl@(57(4B:1YKD4B3f01+1YKD4B,@(0bB401+1YKCsd:0>mouse$1YKCsd@1YKCu6+:1YKCsd7Q01+1YKCsd,"}
    ]
  end

  test_with_params "ron set test", fn i, st, upd, out -> test_set(i, st, upd, out) end do
    [
      {7, "*set#test7@1=1", "*set#test7@2=2", "*set#test7@2:d!:0=2@1=1"},
      {8, "*set#test8@1!@=1", "*set#test8@2:1;", "*set#test8@2!:1,"},
      {9, "*set#test9@3:1;", "*set#test9@4:2;", "*set#test9@4:d!:2,@3:1,"},
      {10, "*set#test10@2!@=2@1=1", "*set#test10@5!@=5@3:2,@4:1,", "*set#test10@5!@=5@3:2,@4:1,"},
      {11, "*set#test11@2!@=2@1=1", ["*set#test11@3!@:2,@4:1,", "*set#test11@5!@=5"],
       "*set#test11@5!@=5@3:2,@4:1,"},
      {12, "*set#test12@3!@:2,@4:1", "*set#test12@5!@=5.*set#test12@2!@=2@1=1",
       "*set#test12@2!@5=5@3:2,@4:1,"}
    ]
  end

  defp test_set(i, st, upd, out) do
    state = Frame.parse!(st)
    updates = List.wrap(upd) |> Enum.map(&Frame.parse!/1)
    output = Frame.parse!(out)

    red = Crdt.Set.reduce(state, updates)
    final = Crdt.Set.map(red)
    expected = Crdt.Set.map(output)

    if final != expected do
      IO.puts("#{i}: red #{Frame.format(red)}")
      IO.puts("#{i}: output #{Frame.format(output)}")
      IO.puts("#{i}: final #{inspect(final)}")
      IO.puts("#{i}: expected #{inspect(expected)}")
    end

    assert final == expected
  end
end
