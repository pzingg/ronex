defmodule Crdt.Set do
  @moduledoc """
  2-Phase Set CRDT.

  2-phase sets are sets of atoms.

  Each element is associated with the timestamp of the Op that
  inserted it.

  Removing elements is done my inserting a tombstone.
  Thus, deletions override insertions.
  """

  def reduce(state, updates) do
    Crdt.merge(__MODULE__, state, updates)
  end

  def map(state) do
    Enum.reduce(state, MapSet.new(), fn
      %Op{term: :header}, acc ->
        acc

      %Op{term: :query}, acc ->
        acc

      %Op{location: loc, atoms: [val]}, acc ->
        if UUID.zero?(loc) do
          MapSet.put(acc, val)
        else
          acc
        end

      _, acc ->
        acc
    end)
  end

  def compare_primary(a, b) do
    a = if UUID.zero?(a.location), do: a.event, else: a.location
    b = if UUID.zero?(b.location), do: b.event, else: b.location
    UUID.compare(b, a)
  end

  def compare_secondary(a, b) do
    UUID.compare(b.location, a.location)
  end

  def compare(a, b) do
    case compare_primary(a, b) do
      :eq -> compare_secondary(a, b)
      cmp -> cmp
    end
  end
end
