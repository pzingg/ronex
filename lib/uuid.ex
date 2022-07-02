defmodule UUID do
  use Bitwise

  defstruct hi: 0, lo: 0, scheme: :name, variety: 0

  @u60_mask 0x0FFF_FFFF_FFFF_FFFF

  def zero(), do: %UUID{}

  @doc """
  String representation: `~~~~~~~~~~`
  """
  def error() do
    %UUID{hi: 1_152_921_504_606_846_975, lo: 0}
  end

  def now(), do: %UUID{hi: 0, lo: 0, scheme: :event}
  def now_from(origin), do: %UUID{hi: 0, lo: origin, scheme: :event}

  @doc """
  String representation: `~-0`
  """
  def never() do
    %UUID{hi: 1_134_907_106_097_364_992, lo: 0, scheme: :event}
  end

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
      %UUID{hi: uuid.hi, lo: 0}
    else
      _ -> error()
    end
  end

  def scoped_name(name, scope) do
    with {:ok, {name_uuid, ""}} <- UUID.parse(name),
         {:ok, {scope_uuid, ""}} <- UUID.parse(scope) do
      %UUID{hi: name_uuid.hi, lo: scope_uuid.hi}
    else
      _ -> error()
    end
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
          hi: value &&& @u60_mask,
          lo: origin &&& @u60_mask,
          scheme: scheme,
          variety: variety
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
    init = {nil, 0, nil, 0, nil, nil}
    parse(str, prev_column, prev_row, init)
  end

  defp parse("", %UUID{} = prev_column, _, {_, 0, _, 0, _, _}) do
    {:ok, {prev_column, ""}}
  end

  defp parse("", %UUID{hi: chi, lo: clo, scheme: csch}, _, {nil, _, _, 0, sch, var}) do
    {:ok, {%UUID{hi: chi, lo: clo, scheme: sch || csch, variety: var || 0}, ""}}
  end

  defp parse("", %UUID{lo: clo, scheme: csch}, _, {hi, _, _, 0, sch, var}) do
    {:ok, {%UUID{hi: hi || 0, lo: clo, scheme: sch || csch, variety: var || 0}, ""}}
  end

  defp parse("", %UUID{scheme: csch}, _, {hi, _, lo, _, sch, var}) do
    {:ok, {%UUID{hi: hi || 0, lo: lo || 0, scheme: sch || csch, variety: var || 0}, ""}}
  end

  defp parse("", _, _, {hi, _, lo, _, sch, var}) do
    {:ok, {%UUID{hi: hi || 0, lo: lo || 0, scheme: sch || :name, variety: var || 0}, ""}}
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
                  {hi, hi_bits, lo || 0 ||| val <<< (6 * i), lo_bits + 1, sch, var}
                else
                  {:error, "lo overflow"}
                end

              x when x in 0..9 ->
                i = 9 - hi_bits
                {hi || 0 ||| val <<< (6 * i), hi_bits + 1, lo, lo_bits, sch, var}

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

      ?( when not is_nil(prev_column) and hi_bits == 0 ->
        parse(
          cdr,
          prev_column,
          prev_row,
          {prev_column.hi &&& 0xFFF_FFF0_0000_0000, 4, 0, 0, sch, var}
        )

      ?( when not is_nil(prev_column) and lo_bits == 0 ->
        parse(
          cdr,
          prev_column,
          prev_row,
          {hi, 10, prev_column.lo &&& 0xFFF_FFF0_0000_0000, 4, sch, var}
        )

      ?( ->
        {:error, "( prefix inside UUID."}

      ?[ when not is_nil(prev_column) and hi_bits == 0 ->
        parse(
          cdr,
          prev_column,
          prev_row,
          {prev_column.hi &&& 0xFFF_FFFF_C000_0000, 5, 0, 0, sch, var}
        )

      ?[ when not is_nil(prev_column) and lo_bits == 0 ->
        parse(
          cdr,
          prev_column,
          prev_row,
          {hi, 10, prev_column.lo &&& 0xFFF_FFFF_C000_0000, 5, sch, var}
        )

      ?[ ->
        {:error, "[ prefix inside UUID."}

      ?{ when not is_nil(prev_column) and hi_bits == 0 ->
        parse(
          cdr,
          prev_column,
          prev_row,
          {prev_column.hi &&& 0xFFF_FFFF_FF00_0000, 6, 0, 0, sch, var}
        )

      ?{ when not is_nil(prev_column) and lo_bits == 0 ->
        parse(
          cdr,
          prev_column,
          prev_row,
          {hi, 10, prev_column.lo &&& 0xFFF_FFFF_FF00_0000, 6, sch, var}
        )

      ?{ ->
        {:error, "{ prefix inside UUID."}

      ?} when not is_nil(prev_column) and hi_bits == 0 ->
        parse(
          cdr,
          prev_column,
          prev_row,
          {prev_column.hi &&& 0xFFF_FFFF_FFFC_0000, 7, 0, 0, sch, var}
        )

      ?} when not is_nil(prev_column) and lo_bits == 0 ->
        parse(
          cdr,
          prev_column,
          prev_row,
          {hi, 10, prev_column.lo &&& 0xFFF_FFFF_FFFC_0000, 7, sch, var}
        )

      ?} ->
        {:error, "} prefix inside UUID."}

      ?] when not is_nil(prev_column) and hi_bits == 0 ->
        parse(
          cdr,
          prev_column,
          prev_row,
          {prev_column.hi &&& 0xFFF_FFFF_FFFF_F000, 8, 0, 0, sch, var}
        )

      ?] when not is_nil(prev_column) and lo_bits == 0 ->
        parse(
          cdr,
          prev_column,
          prev_row,
          {hi, 10, prev_column.lo &&& 0xFFF_FFFF_FFFF_F000, 8, sch, var}
        )

      ?] ->
        {:error, "] prefix inside UUID."}

      ?) when not is_nil(prev_column) and hi_bits == 0 ->
        parse(
          cdr,
          prev_column,
          prev_row,
          {prev_column.hi &&& 0xFFF_FFFF_FFFF_FFC0, 9, 0, 0, sch, var}
        )

      ?) when not is_nil(prev_column) and lo_bits == 0 ->
        parse(
          cdr,
          prev_column,
          prev_row,
          {hi, 10, prev_column.lo &&& 0xFFF_FFFF_FFFF_FFC0, 9, sch, var}
        )

      ?) ->
        {:error, ") prefix inside UUID."}

      ?` when prev_row != nil and hi_bits == 0 ->
        {:ok, {prev_row, str}}

      ?` ->
        {:error, "` prefix inside UUID."}

      ?+ when hi_bits == 0 ->
        parse(cdr, prev_column, prev_row, {prev_column.hi, 10, 0, 0, :derived, var})

      ?+ when lo_bits == 0 ->
        parse(cdr, prev_column, prev_row, {hi, 10, 0, 0, :derived, var})

      ?% when hi_bits == 0 ->
        parse(cdr, prev_column, prev_row, {prev_column.hi, 10, 0, 0, :hash, var})

      ?% when lo_bits == 0 ->
        parse(cdr, prev_column, prev_row, {hi, 10, 0, 0, :hash, var})

      ?- when hi_bits == 0 ->
        parse(cdr, prev_column, prev_row, {prev_column.hi, 10, 0, 0, :event, var})

      ?- when lo_bits == 0 ->
        parse(cdr, prev_column, prev_row, {hi, 10, 0, 0, :event, var})

      ?$ when hi_bits == 0 ->
        parse(cdr, prev_column, prev_row, {prev_column.hi, 10, 0, 0, :name, var})

      ?$ when lo_bits == 0 ->
        parse(cdr, prev_column, prev_row, {hi, 10, 0, 0, :name, var})

      _ ->
        cond do
          hi == 0 and lo == 0 and hi_bits == 0 and lo_bits == 0 ->
            {:ok, {prev_column, str}}

          hi == 0 and lo == 0 ->
            {:ok, {%UUID{}, str}}

          true ->
            {:ok, {%UUID{hi: hi || 0, lo: lo || 0, scheme: sch || :name, variety: var || 0}, str}}
        end
    end
  end

  def format(%UUID{hi: 0, lo: 0}), do: "0"

  def format(%UUID{hi: hi, lo: 0, scheme: :name, variety: variety}) do
    UUID.variety_to_string(variety) <> UUID.u64_to_string(hi)
  end

  def format(%UUID{hi: hi, lo: lo, scheme: scheme, variety: variety}) do
    UUID.variety_to_string(variety) <>
      UUID.u64_to_string(hi) <> UUID.scheme_to_string(scheme) <> UUID.u64_to_string(lo)
  end

  # FormatZipUUID
  def format_with_context(uuid), do: format_with_context(uuid, %UUID{})

  def format_with_context(%UUID{hi: 0, lo: 0}, _context), do: "0"

  def format_with_context(
        %UUID{hi: hi, lo: lo, scheme: sch, variety: var} = uuid,
        %UUID{hi: chi, lo: clo, scheme: csch, variety: cvar} = context
      ) do
    if var != cvar do
      # don't want to optimize this; a rare case anyway
      format(uuid)
    else
      {value_part, vtype} = u64_to_string_with_context(hi, chi)

      if lo == 0 do
        # transcendent name
        value_part
      else
        # sometimes, we may skip UUID separator (+-%$)
        {origin_part, otype} = u64_to_string_with_context(lo, clo)

        cond do
          sch == csch && otype == :empty && vtype != :empty ->
            value_part

          sch == csch && otype == :prefix && vtype == :prefix ->
            value_part <> origin_part

          true ->
            value_part <> scheme_to_string(sch) <> origin_part
        end
      end
    end
  end

  # FormatZipInit
  def u64_to_string_with_context(value, context) do
    prefix_bits = 60 - len64(bxor(value, context) &&& @u60_mask)

    case prefix_bits do
      60 ->
        {"", :empty}

      prefix when prefix in 24..59 ->
        prefix = prefix - Integer.mod(prefix, 6)
        pchar = String.at("([{}])", Integer.floor_div(prefix, 6) - 4)
        value = value <<< prefix &&& @u60_mask

        if value == 0 do
          {pchar, :prefix}
        else
          {pchar <> u64_to_string(value), :prefix}
        end

      _ ->
        {u64_to_string(value), :normal}
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

  def scheme_to_string(:name), do: "$"
  def scheme_to_string(:hash), do: "%"
  def scheme_to_string(:event), do: "-"
  def scheme_to_string(:derived), do: "+"

  def variety_to_string(nil), do: ""
  def variety_to_string(0), do: ""

  def variety_to_string(val) do
    chars =
      case val do
        x when x in 0..9 -> [?0 + x, ?/]
        x when x in 10..35 -> [?A + x - 10, ?/]
        _ -> []
      end

    List.to_string(chars)
  end

  def u64_to_string(value, opts \\ []) do
    str = u64_to_string_impl(value)

    if Keyword.get(opts, :trim, true) do
      str = String.trim_trailing(str, "0")

      if String.length(str) == 0 do
        "0"
      else
        str
      end
    else
      str
    end
  end

  def u64_to_string_impl(value) do
    Enum.map(0..10, fn idx ->
      idx = (9 - idx) * 6
      val = value >>> idx &&& 63

      case val do
        x when x in 0..9 -> ?0 + x
        x when x in 10..35 -> ?A + x - 10
        36 -> ?_
        x when x in 37..62 -> ?a + x - 37
        63 -> ?~
      end
    end)
    |> List.to_string()
  end

  def len64(0), do: 0

  def len64(value) do
    {x, n} =
      Enum.reduce([32, 16, 8, 4, 2, 1], {value, 1}, fn i, {x, n} ->
        if x >= 1 <<< i do
          {x >>> i, n + i}
        else
          {x, n}
        end
      end)

    n
  end
end

defimpl String.Chars, for: UUID do
  def to_string(uuid), do: UUID.format_with_context(uuid)
end

defimpl Inspect, for: UUID do
  def inspect(uuid, _), do: UUID.format_with_context(uuid)
end
