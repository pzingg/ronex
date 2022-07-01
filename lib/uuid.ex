defmodule UUID do
  use Bitwise, operators_only: true

  defstruct hi: 0, lo: 0, scheme: :event, variety: nil

  @int60_mask 0x0FFF_FFFF_FFFF_FFFF

  def zero(), do: %UUID{hi: 0, lo: 0, scheme: :name}
  def now(), do: %UUID{hi: 0, lo: 0, scheme: :event}
  def now_from(origin), do: %UUID{hi: 0, lo: origin, scheme: :event}

  def timestamp(dt_utc) do
    value = encode_calendar(dt_utc)
    %UUID{hi: value, lo: 0, scheme: :event}
  end

  def timestamp_from(dt_utc, origin) do
    value = encode_calendar(dt_utc)
    %UUID{hi: value, lo: origin, scheme: :event}
  end

  def name(name) do
    with {:ok, {uuid, ""}} <- UUID.parse(name) do
      %UUID{hi: uuid.hi, lo: 0, scheme: :name}
    else
      _ -> error()
    end
  end

  def scoped_name(name, scope) do
    with {:ok, {name_uuid, ""}} <- UUID.parse(name),
         {:ok, {scope_uuid, ""}} <- UUID.parse(scope) do
      %UUID{hi: name_uuid.hi, lo: scope_uuid.hi, scheme: :name}
    else
      _ -> error()
    end
  end

  @doc """
  String representation: `~~~~~~~~~~`
  """
  def error() do
    %UUID{hi: 1_152_921_504_606_846_975, lo: 0, scheme: :name}
  end

  @doc """
  String representation: `~-0`
  """
  def never() do
    %UUID{hi: 1_134_907_106_097_364_992, lo: 0, scheme: :event}
  end

  def is_less?(%UUID{hi: hi1, lo: lo1}, %UUID{hi: hi2, lo: lo2}) do
    if hi1 == hi2 do
      lo1 < lo2
    else
      hi1 < hi2
    end
  end

  def is_zero?(%UUID{hi: 0}), do: true
  def is_zero?(_), do: false

  def is_error?(%UUID{} = uuid), do: uuid == error()
  def is_error?(_), do: false

  def new(value, origin, scheme) when is_atom(scheme) do
    new(value, origin, scheme, 0)
  end

  def new(value, origin, scheme, variety) when is_atom(scheme) do
    case decode_scheme(scheme) do
      nil ->
        error()

      sch ->
        %UUID{
          hi: (value &&& @int60_mask) ||| (variety &&& 15) <<< 60,
          lo: (origin &&& @int60_mask) ||| sch <<< 60,
          scheme: scheme
        }
    end
  end

  def derived(%UUID{hi: hi, lo: lo, scheme: scheme} = uuid) do
    if scheme == :event do
      new(hi, lo, :derived)
    else
      uuid
    end
  end

  def parse(str), do: parse(str, UUID.zero(), UUID.zero())

  def parse(str, prev_column, prev_row) do
    init = {0, 0, 0, 0, :name, nil}
    parse(str, prev_column, prev_row, init)
  end

  defp parse("", prev_column, _, {_, 0, _, 0, _, _}) do
    {:ok, {prev_column, ""}}
  end

  defp parse("", _, _, {hi, _, lo, _, sch, var}) do
    {:ok, {%UUID{hi: hi, lo: lo, scheme: sch, variety: var || 0}, ""}}
  end

  defp parse(str, prev_column, prev_row, {hi, hi_bits, lo, lo_bits, sch, var}) do
    str = String.trim_leading(str)
    car = String.first(str) |> :binary.first()
    cdr = String.slice(str, 1..-1)

    is_parse_char =
      car in ?0..?9 or car in ?a..?z or
        car in ?A..?Z or car == ?~ or car == ?_

    case car do
      _ when is_parse_char ->
        val =
          case car do
            car when car in ?0..?9 -> car - ?0
            car when car in ?A..?Z -> car - ?A + 10
            ?_ -> 36
            car when car in ?a..?z -> car - ?a + 37
            ?~ -> 63
          end

        if is_nil(var) && hi_bits == 0 && val <= 15 && String.starts_with?(cdr, "/") do
          cdr = String.slice(cdr, 1..-1)
          parse(cdr, prev_column, prev_row, {hi, hi_bits, lo, lo_bits, sch, val})
        else
          state =
            case hi_bits do
              10 ->
                if lo_bits in 0..9 do
                  i = 9 - lo_bits
                  {hi, hi_bits, lo ||| val <<< (6 * i), lo_bits + 1, sch, var}
                else
                  {:error, "lo overflow"}
                end

              x when x in 0..9 ->
                i = 9 - hi_bits
                {hi ||| val <<< (6 * i), hi_bits + 1, lo, lo_bits, sch, var}

              _ ->
                {:error, "hi overflow"}
            end

          case state do
            {:error, reason} ->
              {:error, reason}

            _ ->
              parse(cdr, prev_column, prev_row, state)
          end
        end

      ?( when prev_column != nil and hi_bits == 0 ->
        parse(
          cdr,
          prev_column,
          prev_row,
          {prev_column.hi >>> 0xFFF_FFF0_0000_0000, 4, 0, 0, sch, var}
        )

      ?( when prev_column != nil and lo_bits == 0 ->
        parse(
          cdr,
          prev_column,
          prev_row,
          {hi, hi_bits, prev_column.lo >>> 0xFFF_FFF0_0000_0000, 4, sch, var}
        )

      ?( ->
        {:error, "( prefix inside UUID."}

      ?[ when prev_column != nil and hi_bits == 0 ->
        parse(
          cdr,
          prev_column,
          prev_row,
          {prev_column.hi &&& 0xFFF_FFFF_C000_0000, 5, 0, 0, sch, var}
        )

      ?[ when prev_column != nil and lo_bits == 0 ->
        parse(
          cdr,
          prev_column,
          prev_row,
          {hi, hi_bits, prev_column.lo &&& 0xFFF_FFFF_C000_0000, 5, sch, var}
        )

      ?[ ->
        {:error, "[ prefix inside UUID."}

      ?{ when prev_column != nil and hi_bits == 0 ->
        parse(
          cdr,
          prev_column,
          prev_row,
          {prev_column.hi &&& 0xFFF_FFFF_FF00_0000, 6, 0, 0, sch, var}
        )

      ?{ ->
        {:error, "{ prefix inside UUID."}

      ?} when prev_column != nil and hi_bits == 0 ->
        parse(
          cdr,
          prev_column,
          prev_row,
          {prev_column.hi &&& 0xFFF_FFFF_FFFC_0000, 7, 0, 0, sch, var}
        )

      ?} ->
        {:error, "} prefix inside UUID."}

      ?] when prev_column != nil and hi_bits == 0 ->
        parse(
          cdr,
          prev_column,
          prev_row,
          {prev_column.hi &&& 0xFFF_FFFF_FFFF_F000, 8, 0, 0, sch, var}
        )

      ?] ->
        {:error, "] prefix inside UUID."}

      ?) when prev_column != nil and hi_bits == 0 ->
        parse(
          cdr,
          prev_column,
          prev_row,
          {prev_column.hi &&& 0xFFF_FFFF_FFFF_FFC0, 9, 0, 0, sch, var}
        )

      ?) when prev_column != nil and lo_bits == 0 ->
        parse(
          cdr,
          prev_column,
          prev_row,
          {hi, hi_bits, prev_column.lo &&& 0xFFF_FFFF_FFFF_FC00, 9, sch, var}
        )

      ?) ->
        {:error, ") prefix inside UUID."}

      ?` when prev_row != nil and hi_bits == 0 ->
        {:ok, {prev_row, str}}

      ?` ->
        {:error, "` prefix inside UUID."}

      ?+ when lo_bits == 0 ->
        parse(cdr, prev_column, prev_row, {hi, 10, 0, 0, :derived, var})

      ?% when lo_bits == 0 ->
        parse(cdr, prev_column, prev_row, {hi, 10, 0, 0, :hash, var})

      ?- when lo_bits == 0 ->
        parse(cdr, prev_column, prev_row, {hi, 10, 0, 0, :event, var})

      ?$ when lo_bits == 0 ->
        parse(cdr, prev_column, prev_row, {hi, 10, 0, 0, :name, var})

      _ ->
        cond do
          hi == 0 and lo == 0 and hi_bits == 0 and lo_bits == 0 ->
            {:ok, {prev_column, str}}

          hi == 0 and lo == 0 ->
            {:ok, {UUID.zero(), str}}

          true ->
            {:ok, {%UUID{hi: hi, lo: lo, scheme: sch, variety: var || 0}, str}}
        end
    end
  end

  def decode_scheme(sch) do
    case sch do
      :name -> 0
      :hash -> 1
      :event -> 2
      :derived -> 3
      _ -> nil
    end
  end

  def encode_scheme(sch) do
    Enum.at([:name, :hash, :event, :derived], sch &&& 3)
  end

  def encode_calendar(dt_utc) do
    %{
      year: year,
      month: month,
      day: day,
      hour: hour,
      minute: minute,
      second: second,
      microsecond: {microsecond, _precision}
    } = DateTime.truncate(dt_utc, :microsecond)

    if year < 2010 do
      0
    else
      i = (year - 2010) * 12 + month - 1
      i = i <<< 6 ||| day - 1
      i = i <<< 6 ||| hour
      i = i <<< 6 ||| minute
      i = i <<< 6 ||| second
      i <<< 24 ||| microsecond
    end
  end
end

defimpl String.Chars, for: UUID do
  use Bitwise, operators_only: true

  def to_string(%UUID{hi: 0, lo: 0}), do: "0"

  def to_string(%UUID{hi: hi, lo: 0, scheme: :name, variety: variety}) do
    encode_variety(variety) <> encode_int60(hi)
  end

  def to_string(%UUID{hi: hi, lo: lo, scheme: scheme, variety: variety}) do
    encode_variety(variety) <> encode_int60(hi) <> encode_scheme(scheme) <> encode_int60(lo)
  end

  defp encode_variety(nil), do: ""
  defp encode_variety(0), do: ""

  defp encode_variety(val) do
    chars =
      case val do
        x when x in 0..9 -> [?0 + x, ?/]
        x when x in 10..35 -> [?A + x - 10, ?/]
        _ -> []
      end

    List.to_string(chars)
  end

  defp encode_scheme(:name), do: "$"
  defp encode_scheme(:hash), do: "%"
  defp encode_scheme(:event), do: "-"
  defp encode_scheme(:derived), do: "+"

  defp encode_int60(int) do
    str =
      Enum.map(0..10, fn idx ->
        idx = (9 - idx) * 6
        val = int >>> idx &&& 63

        case val do
          x when x in 0..9 -> ?0 + x
          x when x in 10..35 -> ?A + x - 10
          36 -> ?_
          x when x in 37..62 -> ?a + x - 37
          63 -> ?~
        end
      end)
      |> List.to_string()
      |> String.trim_trailing("0")

    if String.length(str) == 0 do
      "0"
    else
      str
    end
  end
end

defimpl Inspect, for: UUID do
  def inspect(uuid, _), do: to_string(uuid)
end
