defmodule Crdt do
  @moduledoc """
  Conflict-free, Replicated Data Type.
  """

  @doc """
  `module` is an implementing module that has `compare/2`
  `compare_primary/2` function used to sort and prune the `:raw` and `:reduced`
  Ops in the frames.
  """
  def merge(module, state, updates) do
    with [%Op{type: ty, object: obj, location: loc} | _] <- state do
      full_state = UUID.zero?(loc)
      all_ops = state ++ List.flatten(updates)

      {min, max} =
        Enum.min_max_by(
          all_ops,
          fn a -> a end,
          fn a, b -> UUID.less_than_or_equal_to?(a.event, b.event) end
        )

      loc =
        if full_state do
          UUID.zero()
        else
          min.location
        end

      header = %Op{
        type: ty,
        object: obj,
        event: max.event,
        location: loc,
        term: :header,
        atoms: []
      }

      {_, pruned_ops} =
        Enum.reduce(all_ops, [], fn
          %Op{term: :header}, acc -> acc
          %Op{term: :query}, acc -> acc
          %Op{} = op, acc -> [op | acc]
        end)
        |> Enum.sort(module)
        |> Enum.reduce({nil, []}, fn op, acc -> prune(module, op, acc) end)

      [header | Enum.reverse(pruned_ops)]
    else
      _ -> []
    end
  end

  defp prune(module, op, {last_op, state}) do
    case state do
      [] ->
        {op, [op]}

      _ ->
        case Kernel.apply(module, :compare_primary, [op, last_op]) do
          :eq ->
            {op, state}

          _ ->
            {op, [op | state]}
        end
    end
  end
end
