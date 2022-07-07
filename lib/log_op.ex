defmodule LogOp do
  @moduledoc """
  Logical operation for use with chronofold.
  """

  alias Crdt.Chronofold

  defstruct andx: nil,
            auth: nil,
            val: nil,
            ndx: nil

  def format_ts({auth, andx}) do
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

    ref = Chronofold.ref_ndx(log_op, cf)

    ref_str =
      if !is_nil(ref) do
        " ref:#{format_ts(ref)}"
      else
        ""
      end

    next = Chronofold.next_ndx(log_op, cf)

    next_str =
      if !is_nil(next) do
        " next:#{format_ts(next)}"
      else
        ""
      end

    auth_note = Chronofold.auth_note(log_op, cf)

    auth_str =
      if !is_nil(auth_note) do
        " auth:#{auth_note}"
      else
        ""
      end

    "[#{ndx}]#{format_ts({auth, andx})}#{val_str}#{ref_str}#{next_str}#{auth_str}"
  end
end
