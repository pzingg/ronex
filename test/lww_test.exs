defmodule LwwTest do
  use ExUnit.Case
  use ExUnit.Parameterized

  alias Crdt.Lww

  doctest Lww

  test_with_params "ron-test 01-lww-basic", fn i, st, upd, out -> test_lww(i, st, upd, out) end do
    [
      {0, "*lww#test1!", "*lww#test1@time:a'A';", "*lww#test1@time:0!:a'A',"},
      {1, "*lww#test2@1:0!:a'A'", "*lww#test2@2:b'B';", "*lww#test2@2:0!@1:a'A',@2:b'B',"},
      {2, "*lww#test3@1:a'A1';", "*lww#test3@2:a'A2';", "*lww#test3@2:1!\:a'A2',"},
      {3, "*lww#test4@2:1!:a'A1':b'B1':c'C1'", "*lww#test4@3:1!:a'A2':b'B2'",
       "*lww#test4@3:1!:a'A2',:b'B2',@2:c'C1',"},
      {4, "*lww#array@1:0!:0%0=0,:)1%0 =-1", "*lww#array@2:0!:0%)1'1',:)1%0 =1,:)1%)1=65536\n",
       "*lww#array@2:0!@1:0%0=0,@2:0%0000000001'1',:0000000001%0=1,:0000000001%0000000001=65536,"},
      {5, "*lww#weird@0:0!",
       [
         "*lww#weird@1:longString'While classic databases score 0 on the ACID\\' scale, I should probably reserve the value of -1 for one data sync system based on Operational Transforms.\n Because of the way its OT mechanics worked, even minor glitches messed up the entire database through offset corruption. That was probably the worst case I observed in the wild. Some may build on quicksand, others need solid bedrock… but that system needed a diamond plate to stay still.';",
         "*lww#weird@2:pi^3.141592653589793;",
         "*lww#weird@3:minus=-9223372036854775808;"
       ],
       "*lww#weird@3:0!@1:longString'While classic databases score 0 on the ACID\\' scale, I should probably reserve the value of -1 for one data sync system based on Operational Transforms.\n Because of the way its OT mechanics worked, even minor glitches messed up the entire database through offset corruption. That was probably the worst case I observed in the wild. Some may build on quicksand, others need solid bedrock… but that system needed a diamond plate to stay still.',@3:minus =-9223372036854775808,@2:pi^3.141593e+00,"},
      {6, "*lww#raw@1:one=1;@2:two^2.0:three'три'", "",
       "*lww#raw@2:1!@1:one=1,@2:three'три',:two^2.000000e+00,"}
    ]
  end

  defp test_lww(i, st, upd, out) do
    state = Frame.parse!(st)
    updates = List.wrap(upd) |> Enum.map(&Frame.parse!/1)
    output = Frame.parse!(out)

    red = Lww.reduce(state, updates)
    final = Lww.map(red)
    expected = Lww.map(output)

    if final != expected do
      IO.puts("#{i}: red #{Frame.format(red)}")
      IO.puts("#{i}: output #{Frame.format(output)}")
      IO.puts("#{i}: final #{inspect(final)}")
      IO.puts("#{i}: expected #{inspect(expected)}")
    end

    assert final == expected
  end
end
