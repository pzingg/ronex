defmodule RgaTest do
  use ExUnit.Case

  alias Crdt.Rga

  doctest Rga

  test "ron-test 05-rga empty state + an op" do
    st1 = "*rga#text_0a!"
    up1 = "*rga#text_0a@time'A';"
    out1 = "*rga#text_0a@time:0!'A'"
    test_rga(1, st1, up1, out1)
  end

  test "ron-test 05-rga a state plus an op" do
    st2 = "*rga#text_sa@1!@'A'"
    up2 = "*rga#text_sa@2:1'B';"
    out2 = "*rga#text_sa@2:0!@1 'A' ,@2 'B' ,"
    test_rga(2, st2, up2, out2)
  end

  test "ron-test 05-rga an op plus another op" do
    st3 = "*rga#text_ab@2:1'B';"
    up3 = "*rga#text_ab@3:2'C';"
    # a subtree patch
    out3 = "*rga#text_ab@3:1!@2 :0 'B' ,@3 'C' ,"
    test_rga(3, st3, up3, out3)
  end

  test "ron-test 05-rga a state plus a patch" do
    st4 = "*rga#text_sp@1:0!@'A'"
    up4 = "*rga#text_sp@2:1!:0'B'"
    # a merged state
    out4 = "*rga#text_sp@2:0!	@1Crdt. 'A' ,	@2 'B' ,"
    test_rga(4, st4, up4, out4)
  end

  test "ron-test 05-rga a patch plus a patch" do
    st5 = "*rga#text_pp@2:1!:0'B'"
    up5 = "*rga#text_pp@3:2!:0'C'"
    # a merged patch
    out5 = "*rga#text_pp@3:1!@2:0'B',@3'C',"
    test_rga(5, st5, up5, out5)
  end

  test "ron-test 05-rga a state plus a later state" do
    st6 = "*rga#text_st@1:0!@'A'"
    up6 = "*rga#text_st@2:0!@1'A'@2'B'"
    # the later state
    out6 = "*rga#text_st@2:0!@1'A',@2'B',"
    test_rga(6, st6, up6, out6)
  end

  test "ron-test 05-rga two diverged states" do
    st7 = "*rga#text_sS@2:0!@1'A'@2'B'"
    up7 = "*rga#text_sS@3:0!@1'A'@3'C'"
    # a merged state
    out7 = "*rga#text_sS@3:0!@1'A',@2'B',@3'C',"
    test_rga(7, st7, up7, out7)
  end

  test "ron-test 05-rga state + state with a new rm" do
    st8 = "*rga#text_sz@2:0!@1'A'@2'B'"
    up8 = "*rga#text_sz@4:0!@1:4'A'@3:0'C'"
    # rm applied
    out8 = "*rga#text_sz@4:0!@1:4'A',@2:0'B',@3'C',"
    test_rga(8, st8, up8, out8)
  end

  test "ron-test 05-rga 'an op and a backspace rm" do
    st = "*rga#text@2:1'B';"
    up = "*rga#text@3:2;"
    # a patch, rm applied
    out = "*rga#text@3:1!@2:3'B',"
    test_rga(9, st, up, out)
  end

  test "ron-test 05-rga a patch and a backspace rm" do
    st10 = "*rga#text_pd@3:1!@2:0'B'@3'C'"
    up10 = "*rga#text_pd@4:2;"
    # a patch with the rm applied
    out10 = "*rga#text_pd@4:1!@2:4'B',@3:0'C',"
    test_rga(10, st10, up10, out10)
  end

  test "ron-test 05-rga a state and an rm-patch" do
    st11 = "*rga#text_sr@2:0!@1'A'@2'B'"
    up11 = "*rga#text_sr@4:3-!@3:1,@4:2,"
    # a state with all rms applied
    out11 = "*rga#text_sr@4:0!@1:3'A',@2:4'B',"
    test_rga(11, st11, up11, out11)
  end

  test "ron-test 05-rga diverged states with concurrent rms and stuff" do
    st12 = "*rga#text_sx@5:0!@1:4a'A'@2:5'B'"
    up12 = "*rga#text_sx@4:0!@1:4'A'@3:0'C'"
    # a merged state
    out12 = "*rga#text_sx@5:0!@1:4a'A',@3:0'C',@2:5'B',"
    test_rga(12, st12, up12, out12)
  end

  test "ron-test 05-rga two states diverged in a convoluted way" do
    st13 = "*rga#text_sw@3:0!@1:4a'A'@3:0'C'@2:5'B'"
    up13 = "*rga#text_sw@4:0!@1:4a'A'@3:0'C'@4:0'D'@2:5'B'"
    # merged
    out13 = "*rga#text_sw@4:0!@1:4a'A',@3:0'C',@4'D',@2:5'B',"
    test_rga(13, st13, up13, out13)
  end

  test "ron-test 05-rga even more convoluted divergence" do
    st14 = "*rga#text_SW@5:0!@1:4a'A'@5:0'E'@3:0'C'@2:5'B'"
    up14 = "*rga#text_SW@7:0!@1:4a'A'@6:0'F'@3:7'C'@4:0'D'@2:5'B'"
    # merged
    out14 = "*rga#text_SW@7:0!@1:4a'A',@6:0'F',@5'E',@3:7'C',@4:0'D',@2:5'B',"
    test_rga(14, st14, up14, out14)
  end

  test "ron-test 05-rga a state and an insert op" do
    st15 = "*rga#text_zi@2:0!@1'A'@2'B'"
    up15 = "*rga#text_zi@3:1'-';"
    # inserted properly
    out15 = "*rga#text_zi@3:0!@1'A',@3'-',@2'B',"
    test_rga(15, st15, up15, out15)
  end

  test "ron-test 05-rga rm eclipsed by a concurrent rm" do
    st16 = "*rga#text_dd@4:0!@1'A'@2:4'B'"
    up16 = "*rga#text_dd@3:2;"
    # skipped
    out16 = "*rga#text_dd@4000000001:0!@1'A',@2:4'B',"
    test_rga(16, st16, up16, out16)
  end

  test "ron-test 05-rga reorders: unapplicable remove" do
    st17 = "*rga#test@2!@1'A'@2'B'"
    up17 = "*rga#test@4:3;"
    # rm that is stashed in a separate rm frame
    out17 = "*rga#test@4!@1'A'@2'B'*rga#test@4:rm!:3,"
    test_rga(17, st17, up17, out17)
  end

  test "ron-test 05-rga for a stashed remove, the target arrives" do
    st18 = "*rga#text_~a@4:0!@1'A'@2'B'"
		up18 = ["*rga#text_~a@4:4-!:3,", "*rga#text_~a@3:2'C';"]
    # target removed
    out18 = "*rga#text_~a@4000000001:0!@1'A',@2'B',@3:4'C',"
    test_rga(18, st18, up18, out18)
  end

  test "ron-test 05-rga unapplicable patch" do
    st19 = "*rga#test@2!@1'A'@2'B'"
    up19 = "*rga#test@5:3!@4:0'D'@5'E'"
    # the patch goes into a separate frame'!
    out19 = "*rga#test@5!@1'A'@2'B'*rga#test@5:3!@4:0'D'@5'E'"
    test_rga(19, st19, up19, out19)
  end

  test "ron-test 05-rga the stashed patch becomes applicable (the missing link arrives)" do
    st20 = "*rga#test@2!@1'A'@2'B'"
    up20 = ["*rga#text_~b@5:3!@4:0'D'@5'E'", "*rga#test@3:2'C';"]
    # the patch is applied
    out20 = "*rga#text_~b@5:0!@1'A',@2'B',@3'C',@4'D',@5'E',"
    test_rga(20, st20, up20, out20)
  end

  test "ron-test 05-rga an unapplicable patch with its own rm stash" do
    st21 = "*rga#test@2!@1'A'@2'B'"
    up21 = ["*rga#text_~r@6:3!@4:0'D'@5'E'", "*rga#text_~r@6:6-!:3,"]
    # all separate frames
    out21 = "*rga#text+~r@6!@1'A'@2'B' *rga#text_~r@6:3!@4:0'D'@5'E' *rga#text_~r@6:0-0!:3,"
    test_rga(21, st21, up21, out21)
  end

  test "ron-test 05-rga unapplied frames become applicable" do
    st22 = "*rga#test@6!@1'A'@2'B' *#@6:3!@4:0'D'@5'E' *#@6:rm!:3,"
    up22 = "*rga#test@3:2!@'C'"
    # all applied
    out22 = "*rga#test@3!@1'A'@2'B'@3:6'C'@4:0'D'@5'E'"
    test_rga(22, st22, up22, out22)
  end

  defp test_rga(i, st, upd, out) do
    state = Frame.parse!(st)
    updates = List.wrap(upd) |> Enum.map(&Frame.parse!/1)
    expected = output = Frame.parse!(out) |> Frame.split()

    final = red = Rga.reduce(state, updates)
    # final = Rga.map(red)
    # expected = Rga.map(output)

    if final != expected do
      IO.puts("#{i}: state #{inspect(state)}")
      IO.puts("#{i}: updates #{inspect(updates)}")
      # IO.puts("#{i}: red #{inspect(red)}"))
      # IO.puts("#{i}: output #{inspect(output)}"))
      IO.puts("#{i}: final #{inspect(final)}")
      IO.puts("#{i}: expected #{inspect(expected)}")
    end

    assert final == expected
  end
end
