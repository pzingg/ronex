defmodule ChronofoldTest do
  use ExUnit.Case

  alias Crdt.Chronofold

  doctest Chronofold

  test "minsk example - Section A" do
    st = "*cfold#test!@]1+alice=0,"
    upd = ["*cfold#test!@]5+alice<]3+bob'N',",
      "*cfold#test!@]2+bob<]1+alice'M',@]3'I',@]8+bob<]7+george'K',",
      "*cfold#test!@]3+george<]2+bob=-1,@]5'P',@]7<]5+alice'S',",
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
    IO.puts("final #{Frame.format(final)}")
    IO.puts("cfd #{Chronofold.format(cfd)}")

    assert Chronofold.map_result(cfd) == "PINSK"
  end
end
