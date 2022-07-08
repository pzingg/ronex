defmodule ChronofoldTest do
  use ExUnit.Case

  alias Crdt.Chronofold

  doctest Chronofold

  test "Figure 1 example" do
    st = "*cfold#test!@]1+alice=0,@]2'M',@]3'I',@4'N',@5'S',@6'K',"
    upd = "*cfold#test@]7+bob<]2+alice=-1,@8'P',"

    test_cf("figure 1", st, upd, "PINSK")
  end

  test "Figure 2 example" do
    st =
      "*cfold#test!@}01+alice=0,@}02'S',@}03'T',@}04'A',@}05'N',@}06'I',@}07'S',@}08'L',@}09'A',@}10'V',@}11'S',@}12'K',@}13'Y',"

    upd = [
      "*cfold#test@}10+bob<}09+alice=-1,@}11=-1,@}12=-1,@}13=-1,@}14=-1,@}15=-1,@}16=-1,@}17=-1,",
      "*cfold#test@}18+bob<}17'L',@}19'O',@}20'B',@}21'A',@}22'C',@}23'H',@}23'E',"
    ]

    test_cf("figure 1", st, upd, "LOBACHEVSKY")
  end

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

    test_cf("log alpha", st, upd, "PINSK")
  end

  test "Section A example - log beta" do
    st = "*cfold#test!@]1+alice=0,"

    upd = [
      "*cfold#test!@]2+bob<]1+alice'M',@]3+bob<]2'I',",
      "*cfold#test!@]3+george<]2+bob=-1,@]5+george<]3'P',",
      "*cfold#test!@]5+alice<]3+bob'N',",
      "*cfold#test!@]7+george<]5+alice'S',",
      "*cfold#test!@]8+bob<]7+george'K',"
    ]

    test_cf("log beta", st, upd, "PINSK")
  end

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

    test_cf("log gamma", st, upd, "PINS")
  end

  test "Figure 4 example - a6g14b8" do
    st = "*cfold#test!@}01+alice=0,"

    upd = [
      "*cfold#test!@}02+alice<}01'P',@}03<}02'I',@}04<}03'N',@}05<}04'S',@}06<}05'K',",
      "*cfold#test!@}07+george<}06+alice=-1,@}08<}07=-1,@}09<}08=-1,@}10<}09=-1,@}11<}10'i',@}12<}11'n',@}13<}12's',@}14<}13'k',",
      "*cfold#test!@}07+bob<}02+alice=-1,@}08<}07'M',"
    ]

    test_cf("figure 4", st, upd, "Minsk")
  end

  def test_cf(label, st, upd, expected, opts \\ []) do
    state = Frame.parse!(st)
    updates = List.wrap(upd) |> Enum.map(&Frame.parse!/1)
    cf = Chronofold.parse_input(state, updates)

    show_all = Keyword.get(opts, :verbose)
    Chronofold.dump_input(cf)

    cf =
      Enum.reduce(2..Enum.count(cf.input), cf, fn _i, cf ->
        cf = Chronofold.process_op(cf)
        if show_all do
          Chronofold.dump_log(cf)
        end
        cf
      end)

    if !show_all do
      Chronofold.dump_log(cf)
    end

    result = Chronofold.map(cf)
    IO.puts("\n#{label} result: #{result}")
    assert result == expected
  end
end
