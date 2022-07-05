defmodule UUID do
  use Bitwise

  defstruct hi: 0, lo: 0, scheme: :name, variety: 0

  @u60_mask 0x0FFF_FFFF_FFFF_FFFF
  @error_hi (1 <<< 60) - 1
  @never_hi 63 <<< 54

  @doc """
  String representation: `0`
  """
  def zero(), do: %UUID{}

  @doc """
  String representation: `~~~~~~~~~~`
  """
  def error() do
    %UUID{hi: @error_hi, lo: 0}
  end

  @doc """
  String representation: `~-`
  """
  def never() do
    %UUID{hi: @never_hi, lo: 0, scheme: :event}
  end

  def now(), do: %UUID{hi: 0, lo: 0, scheme: :event}
  def now_from(origin), do: %UUID{hi: 0, lo: origin, scheme: :event}

  def timestamp(dt_utc \\ nil) do
    value = encode_calendar(dt_utc || DateTime.utc_now())
    %UUID{hi: value, lo: 0, scheme: :event}
  end

  def timestamp_from(origin, dt_utc \\ nil) do
    value = encode_calendar(dt_utc || DateTime.utc_now())
    %UUID{hi: value, lo: origin, scheme: :event}
  end

  def name(name, variety \\ 0) do
    with {:ok, {uuid, ""}} <- UUID.parse(name) do
      %UUID{hi: uuid.hi, lo: 0, variety: variety}
    else
      _ -> error()
    end
  end

  def scoped_name(name, scope, variety \\ 0) do
    with {:ok, {name_uuid, ""}} <- UUID.parse(name),
         {:ok, {scope_uuid, ""}} <- UUID.parse(scope) do
      %UUID{hi: name_uuid.hi, lo: scope_uuid.hi, variety: variety}
    else
      _ -> error()
    end
  end

  def compare(%UUID{hi: hi1, lo: lo1}, %UUID{hi: hi2, lo: lo2}) do
    cond do
      hi1 == hi2 ->
        cond do
          lo1 == lo2 -> :eq
          lo1 < lo2 -> :lt
          true -> :gt
        end

      hi1 < hi2 ->
        :lt

      true ->
        :gt
    end
  end

  def equals?(%UUID{} = uuid1, %UUID{} = uuid2) do
    compare(uuid1, uuid2) == :eq
  end

  def less_than_or_equal_to?(%UUID{} = uuid1, %UUID{} = uuid2) do
    compare(uuid1, uuid2) != :gt
  end

  def zero?(%UUID{hi: 0}), do: true
  def zero?(_), do: false

  def error?(%UUID{hi: @error_hi}), do: true
  def error?(_), do: false

  def new(value, origin, scheme, variety \\ 0)
      when is_integer(value) and is_integer(origin) and is_integer(variety) and is_atom(scheme) do
    case encode_scheme(scheme) do
      nil ->
        error()

      _sch ->
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

  def to_bitstring(%UUID{hi: hi, lo: lo, scheme: scheme, variety: variety}) do
    hi = hi || 0 ||| (variety || 0) <<< 60
    lo = lo || 0 ||| (encode_scheme(scheme) || 0) <<< 60
    <<hi::unsigned-big-integer-size(64), lo::unsigned-big-integer-size(64)>>
  end

  def parse(str), do: parse(str, UUID.zero(), UUID.zero())

  def parse(str, prev_column, prev_row \\ nil) do
    init = {nil, 0, nil, 0, nil, nil}
    parse_impl(str, prev_column, prev_row, init)
  end

  defp parse_impl("", %UUID{} = prev_column, _, {nil, _, nil, _, _, _}) do
    {:ok, {prev_column, ""}}
  end

  defp parse_impl(
         "",
         %UUID{hi: chi, lo: clo, scheme: csch, variety: cvar},
         _,
         {hi, _, lo, _, sch, var}
       ) do
    {:ok,
     {%UUID{hi: hi || chi, lo: lo || clo, scheme: sch || csch, variety: var || cvar || 0}, ""}}
  end

  defp parse_impl(str, prev_column, prev_row, {hi, hi_bits, lo, lo_bits, sch, var}) do
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
          parse_impl(cdr, prev_column, prev_row, {hi, hi_bits, lo, lo_bits, sch, val})
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
              parse_impl(cdr, prev_column, prev_row, state)
          end
        end

      ?( when not is_nil(prev_column) and hi_bits == 0 ->
        parse_impl(
          cdr,
          prev_column,
          prev_row,
          {prev_column.hi &&& 0xFFF_FFF0_0000_0000, 4, lo, lo_bits, sch, var}
        )

      ?( when not is_nil(prev_column) and lo_bits == 0 ->
        parse_impl(
          cdr,
          prev_column,
          prev_row,
          {hi, 10, prev_column.lo &&& 0xFFF_FFF0_0000_0000, 4, sch, var}
        )

      ?( ->
        {:error, "( prefix inside UUID."}

      ?[ when not is_nil(prev_column) and hi_bits == 0 ->
        parse_impl(
          cdr,
          prev_column,
          prev_row,
          {prev_column.hi &&& 0xFFF_FFFF_C000_0000, 5, lo, lo_bits, sch, var}
        )

      ?[ when not is_nil(prev_column) and lo_bits == 0 ->
        parse_impl(
          cdr,
          prev_column,
          prev_row,
          {hi, 10, prev_column.lo &&& 0xFFF_FFFF_C000_0000, 5, sch, var}
        )

      ?[ ->
        {:error, "[ prefix inside UUID."}

      ?{ when not is_nil(prev_column) and hi_bits == 0 ->
        parse_impl(
          cdr,
          prev_column,
          prev_row,
          {prev_column.hi &&& 0xFFF_FFFF_FF00_0000, 6, lo, lo_bits, sch, var}
        )

      ?{ when not is_nil(prev_column) and lo_bits == 0 ->
        parse_impl(
          cdr,
          prev_column,
          prev_row,
          {hi, 10, prev_column.lo &&& 0xFFF_FFFF_FF00_0000, 6, sch, var}
        )

      ?{ ->
        {:error, "{ prefix inside UUID."}

      ?} when not is_nil(prev_column) and hi_bits == 0 ->
        parse_impl(
          cdr,
          prev_column,
          prev_row,
          {prev_column.hi &&& 0xFFF_FFFF_FFFC_0000, 7, lo, lo_bits, sch, var}
        )

      ?} when not is_nil(prev_column) and lo_bits == 0 ->
        parse_impl(
          cdr,
          prev_column,
          prev_row,
          {hi, 10, prev_column.lo &&& 0xFFF_FFFF_FFFC_0000, 7, sch, var}
        )

      ?} ->
        {:error, "} prefix inside UUID."}

      ?] when not is_nil(prev_column) and hi_bits == 0 ->
        parse_impl(
          cdr,
          prev_column,
          prev_row,
          {prev_column.hi &&& 0xFFF_FFFF_FFFF_F000, 8, lo, lo_bits, sch, var}
        )

      ?] when not is_nil(prev_column) and lo_bits == 0 ->
        parse_impl(
          cdr,
          prev_column,
          prev_row,
          {hi, 10, prev_column.lo &&& 0xFFF_FFFF_FFFF_F000, 8, sch, var}
        )

      ?] ->
        {:error, "] prefix inside UUID."}

      ?) when not is_nil(prev_column) and hi_bits == 0 ->
        parse_impl(
          cdr,
          prev_column,
          prev_row,
          {prev_column.hi &&& 0xFFF_FFFF_FFFF_FFC0, 9, lo, lo_bits, sch, var}
        )

      ?) when not is_nil(prev_column) and lo_bits == 0 ->
        parse_impl(
          cdr,
          prev_column,
          prev_row,
          {hi, 10, prev_column.lo &&& 0xFFF_FFFF_FFFF_FFC0, 9, sch, var}
        )

      ?) ->
        {:error, ") prefix inside UUID."}

      ?` when not is_nil(prev_row) and hi_bits == 0 ->
        {:ok, {prev_row, str}}

      ?` ->
        {:error, "` prefix inside UUID."}

      ?+ when hi_bits == 0 ->
        parse_impl(cdr, prev_column, prev_row, {prev_column.hi, 10, lo, lo_bits, :derived, var})

      ?+ when lo_bits == 0 ->
        parse_impl(cdr, prev_column, prev_row, {hi, 10, lo, 0, :derived, var})

      ?% when hi_bits == 0 ->
        parse_impl(cdr, prev_column, prev_row, {prev_column.hi, 10, lo, lo_bits, :hash, var})

      ?% when lo_bits == 0 ->
        parse_impl(cdr, prev_column, prev_row, {hi, 10, lo, 0, :hash, var})

      ?- when hi_bits == 0 ->
        parse_impl(cdr, prev_column, prev_row, {prev_column.hi, 10, lo, lo_bits, :event, var})

      ?- when lo_bits == 0 ->
        parse_impl(cdr, prev_column, prev_row, {hi, 10, lo, 0, :event, var})

      ?$ when hi_bits == 0 ->
        parse_impl(cdr, prev_column, prev_row, {prev_column.hi, 10, lo, lo_bits, :name, var})

      ?$ when lo_bits == 0 ->
        parse_impl(cdr, prev_column, prev_row, {hi, 10, lo, 0, :name, var})

      _ ->
        # Terminate parsing if illegal char encountered
        %UUID{hi: chi, lo: clo, scheme: csch, variety: cvar} = prev_column

        {:ok,
         {%UUID{hi: hi || chi, lo: lo || clo, scheme: sch || csch, variety: var || cvar || 0},
          str}}
    end
  end

  def decode_scheme(sch) do
    Enum.at([:name, :hash, :event, :derived], sch &&& 3)
  end

  def encode_scheme(sch) do
    case sch do
      :name -> 0
      :hash -> 1
      :event -> 2
      :derived -> 3
      _ -> nil
    end
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

  @doc """
  Formats a UUID with all the bits:
  1. Variety prefix (if `:variety` is not zero)
  2. Value string, 10 chars
  3. Scheme separator, even for transcendent names
  4. Origin string, 10 chars
  """
  def format(%UUID{hi: 0, lo: 0}), do: "0"

  def format(%UUID{hi: hi, lo: 0, scheme: :name, variety: variety}) do
    UUID.variety_to_string(variety) <> UUID.u64_to_string(hi)
  end

  def format(%UUID{hi: hi, lo: lo, scheme: scheme, variety: variety}) do
    UUID.variety_to_string(variety) <>
      UUID.u64_to_string(hi) <> UUID.scheme_to_string(scheme) <> UUID.u64_to_string(lo)
  end

  @doc """
  Formats a "zipped" UUID without the scheme separator. Used
  for formatting Ops, where the known context implies the scheme.
  """
  def format_as_zipped_name(%UUID{} = uuid) do
    format_with_context(%UUID{uuid | scheme: :name, variety: 0})
  end

  @doc """
  Formats a "zipped" UUID with the scheme separator (except for
  transcendental names).
  """
  def format_with_context(uuid), do: format_with_context(uuid, %UUID{})

  @doc """
  Formats a "zipped" UUID within a context UUID.
  Compresses most significant bits of value and origin parts,
  and removes the separator if the scheme is the same as the context.
  """
  def format_with_context(%UUID{hi: 0, lo: 0}, _context), do: "0"

  def format_with_context(
        %UUID{hi: hi, lo: lo, scheme: sch, variety: var} = uuid,
        %UUID{hi: chi, lo: clo, scheme: csch, variety: cvar}
      ) do
    if var != cvar do
      # don't want to optimize this; a rare case anyway
      format(uuid)
    else
      {value_part, vtype} = u64_to_string_with_context(hi, chi)

      if lo == 0 do
        if sch == :name do
          # transcendental name
          value_part
        else
          value_part <> scheme_to_string(sch)
        end
      else
        # sometimes, we may skip UUID separator (+-%$)
        {origin_part, otype} = u64_to_string_with_context(lo, clo)

        cond do
          sch == csch && otype == :empty && vtype != :empty ->
            value_part

          sch == csch && otype == :prefixed && vtype == :prefixed ->
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
          {pchar, :prefixed}
        else
          {pchar <> u64_to_string(value), :prefixed}
        end

      _ ->
        {u64_to_string(value), :full}
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

  @doc """
  Number of bits required to encode a 64-bit value in base 2.
  """
  def len64(0), do: 0

  def len64(value) do
    {_, n} =
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
