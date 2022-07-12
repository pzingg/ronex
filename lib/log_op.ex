defmodule LogOp do
  @moduledoc """
  Logical operation for use with chronofold.
  """

  alias Crdt.Chronofold

  defstruct andx: nil,
            auth: nil,
            val: nil,
            ndx: nil

  def format({andx, auth}) do
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

    ts = {andx, auth}

    ref_str =
      case Chronofold.get_ref(cf, ts) do
        nil -> ""
        ref -> " ref:#{Chronofold.ndx_of(cf.log, ref)}"
      end

    next =
      if Chronofold.is_tail?(cf, ts) do
        :inf
      else
        Chronofold.get_next(cf, ts)
      end

    next_str =
      case next do
        nil -> ""
        :inf -> " next:inf"
        next -> " next:#{Chronofold.ndx_of(cf.log, next)}"
      end

    auth_note = Chronofold.get_auth(cf, ts)

    auth_str =
      if !is_nil(auth_note) do
        " auth:#{auth_note}"
      else
        ""
      end

    "[#{ndx}]#{format(ts)}#{val_str}#{ref_str}#{next_str}#{auth_str}"
  end
end
