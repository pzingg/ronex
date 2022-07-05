defmodule Frame do
  defstruct type: nil, data: nil

  def parse(str) do
    case parse_impl(str, [], nil) do
      {:ok, ops} -> {:ok, Enum.reverse(ops)}
      err -> err
    end
  end

  # Parse until EOF
  defp parse_impl("", ops, _), do: {:ok, ops}
  # "." can be used in a single string to separate frames
  defp parse_impl("." <> cdr, ops, _), do: parse_impl(cdr, ops, nil)
  # Swallow all whitespace
  defp parse_impl("\t" <> cdr, ops, prev), do: parse_impl(cdr, ops, prev)
  defp parse_impl("\n" <> cdr, ops, prev), do: parse_impl(cdr, ops, prev)
  defp parse_impl("\r" <> cdr, ops, prev), do: parse_impl(cdr, ops, prev)
  defp parse_impl("\v" <> cdr, ops, prev), do: parse_impl(cdr, ops, prev)
  defp parse_impl(" " <> cdr, ops, prev), do: parse_impl(cdr, ops, prev)

  # Prepend ops onto a empty list.  Last op is on top of list
  defp parse_impl(str, ops, prev) do
    prev =
      prev ||
        %Op{
          type: UUID.zero(),
          event: UUID.zero(),
          object: UUID.zero(),
          location: UUID.zero()
        }

    case Op.parse(str, prev) do
      {:ok, {op, cdr}} ->
        # prev = List.last(ops)
        # %Op{ term: prev_term } = prev
        # %Op{ term: term } = op

        # if prev_term == :raw or term != :reduced do
        #  {:ok, ops, str}
        parse_impl(cdr, [op | ops], op)

      err = {:error, _} ->
        err
    end
  end

  def parse!(str) do
    case parse(str) do
      {:ok, ops} -> ops
      {:error, msg} -> raise msg
    end
  end

  def split(frame) do
    Enum.chunk_while(
      frame,
      [],
      fn
        elem, [] ->
          {:cont, [elem]}

        elem = %Op{term: :header}, chunk ->
          {:cont, chunk, [elem]}

        elem = %Op{term: :query}, chunk ->
          {:cont, chunk, [elem]}

        elem = %Op{term: :raw}, chunk ->
          {:cont, chunk, [elem]}

        elem = %Op{term: :reduced}, chunk = [%Op{term: :header} | _] ->
          {:cont, chunk ++ [elem]}

        elem = %Op{term: :reduced}, chunk ->
          {:cont, chunk, [elem]}
      end,
      fn
        [] -> {:cont, []}
        chunk -> {:cont, chunk, []}
      end
    )
  end

  def format_chunks(chunks) do
    n = Enum.count(chunks)

    header =
      case n do
        0 -> "<empty>"
        1 -> "1 chunk\n"
        _ -> "#{n} chunks\n"
      end

    {_, str} =
      Enum.reduce(chunks, {0, header}, fn frame, {i, acc} ->
        {i + 1, acc <> "[#{i}] " <> format(frame)}
      end)

    str
  end

  def format(frame) do
    if frame == [] do
      "<empty>\n"
    else
      Enum.reduce(frame, "", fn
        op, "" -> Kernel.to_string(op) <> "\n"
        op, acc -> acc <> " " <> Kernel.to_string(op) <> "\n"
      end)
    end
  end
end

defimpl String.Chars, for: Frame do
  def to_string(frame), do: Frame.format(frame)
end
