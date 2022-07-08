defmodule LogOp do
  @moduledoc """
  Logical operation for use with chronofold.
  """

  alias Crdt.Chronofold

  defstruct andx: nil,
            auth: nil,
            val: nil,
            ndx: nil

  def format_ts({andx, auth}) do
    "⟨" <>
      String.pad_trailing(auth, 6) <> " " <> String.pad_leading(Integer.to_string(andx), 2) <> "⟩"
  end

  def format(
        %LogOp{
          andx: andx,
          auth: auth,
          val: val,
          ndx: ndx
        } = log_op,
        cf
      ) do
    val_str =
      case val do
        :root -> " :root"
        :del -> " :del "
        s -> String.pad_trailing(" '#{s}'", 6)
      end

    ref = Chronofold.ref_ndx(cf, log_op.ndx)

    ref_str =
      if !is_nil(ref) do
        " ref:#{ref}"
      else
        ""
      end

    next = Chronofold.next_ndx(cf, log_op.ndx)

    next_str =
      case next do
        nil -> ""
        :inf -> " next:inf"
        _ -> " next:#{next}"
      end

    auth_note = Chronofold.auth_note(cf, log_op.ndx)

    auth_str =
      if !is_nil(auth_note) do
        " auth:#{auth_note}"
      else
        ""
      end

    "[#{ndx}]#{format_ts({andx, auth})}#{val_str}#{ref_str}#{next_str}#{auth_str}"
  end
end
