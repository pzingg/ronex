defmodule ChronofoldTest do
  use ExUnit.Case

  alias Crdt.Chronofold

  doctest Chronofold

  @tag :skip
  test "Section A example - log alpha" do
    st = "*cfold#test!@]1+alice=0,"

    upd = [
      "*cfold#test!@]2+bob<]1+alice'M',",
      "*cfold#test!@]3+george<]2+bob=-1,",
      "*cfold#test!@]3+bob<]2'I',",
      "*cfold#test!@]5+alice<]3+bob'N',",
      "*cfold#test!@]5+george<]3'P',@]7<]5+alice'S',",
      "*cfold#test!@]8+bob<]7+george'K',"
    ]

    test_section_a("log alpha", st, upd)
  end

  @tag :skip
  test "Section A example - log beta" do
    st = "*cfold#test!@]1+alice=0,"

    upd = [
      "*cfold#test!@]2+bob<]1+alice'M',@]3+bob<]2'I',",
      "*cfold#test!@]3+george<]2+bob=-1,@]5+george<]3'P',",
      "*cfold#test!@]5+alice<]3+bob'N',",
      "*cfold#test!@]7+george<]5+alice'S',",
      "*cfold#test!@]8+bob<]7+george'K',"
    ]

    test_section_a("log beta", st, upd)
  end

  @tag :skip
  test "Section A example - log gamma" do
    st = "*cfold#test!@]1+alice=0,"

    upd = [
      "*cfold#test!@]2+bob<]1+alice'M',",
      "*cfold#test!@]3+george<]2+bob=-1,",
      "*cfold#test!@]3+bob<]2'I',",
      "*cfold#test!@]5+george<]3'P',",
      "*cfold#test!@]5+alice<]3+bob'N',",
      "*cfold#test!@]7+george<]5+alice'S',"
    ]

    test_section_a("log gamma", st, upd)
  end

  def test_section_a(label, st, upd) do
    alice = Chronofold.author("alice")
    state = Frame.parse!(st)
    updates = Enum.map(upd, &Frame.parse!/1)
    red = Chronofold.reduce(state, updates)

    IO.puts("section a #{label}")
    IO.puts("red #{Frame.format(red)}")
    # IO.puts("vals #{inspect(Chronofold.vals(red))}")
    # IO.puts("refs #{inspect(Chronofold.refs(red))}")
    IO.puts("log(alice) #{inspect(Chronofold.log(red, alice))}")

    {final, cfd} = Chronofold.map(red)
    IO.puts("final #{Frame.format(final)}")
    IO.puts("cfd #{Chronofold.format(cfd)}")

    assert Chronofold.map_result(cfd) == "PINSK"
  end

  test "Figure 4 example - a6g14b8" do
    st = "*cfold#test!@}01+alice=0,"

    upd = [
      "*cfold#test!@}02+alice<}01'P',@}03<}02'I',@}04<}03'N',@}05<}04'S',@}06<}05'K',",
      "*cfold#test!@}07+george<}06+alice=-1,@}08<}07=-1,@}09<}08=-1,@}10<}09=-1,@}11<}10'i',@}12<}11'n',@}13<}12's',@}14<}13'k',",
      "*cfold#test!@}07+bob<}02+alice=-1,@}08<}07'M',"
    ]

    alice = Chronofold.author("alice")
    state = Frame.parse!(st)
    updates = Enum.map(upd, &Frame.parse!/1)
    red = Chronofold.reduce(state, updates)

    IO.puts("red #{Frame.format(red)}")
    IO.puts("vals #{inspect(Chronofold.vals(red))}")
    IO.puts("refs #{inspect(Chronofold.refs(red))}")
    IO.puts("log(alice) #{inspect(Chronofold.log(red, alice))}")

    {final, cfd} = Chronofold.map(red)
    IO.puts("final #{Frame.format(final, jumps: true)}")
    IO.puts("cfd #{Chronofold.format(cfd)}")

    assert Chronofold.map_result(cfd) == "Minsk"
  end
end
