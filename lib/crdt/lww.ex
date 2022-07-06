defmodule Crdt.Lww do
  @moduledoc """
  Last Writer Wins CRDT.

  LWW is the simplest & most popular distributed data type, but it
  isn’t the most intelligent. An LWW is a key-value object, with
  Unicode-encoded strings as keys, and any arbitrary atoms as values.
  Values may also be atom tuples. The write with a later timestamp
  "wins", overriding any other operations that have happened to the
  same key before. Nested objects are implemented as UUID references.
  """

  def reduce(state, updates) do
    Crdt.merge(__MODULE__, state, updates)
  end

  def map(state) do
    Enum.reduce(state, %{}, fn
      %Op{term: :header}, acc ->
        acc

      %Op{term: :query}, acc ->
        acc

      %Op{location: loc, atoms: [val]}, acc ->
        Map.put_new(acc, loc, Crdt.round_5(val))

      _op, acc ->
        acc
    end)
  end

  def compare_primary(a, b) do
    UUID.compare(a.location, b.location)
  end

  def compare_secondary(a, b) do
    UUID.compare(b.event, a.event)
  end

  def compare(a, b) do
    case compare_primary(a, b) do
      :eq -> compare_secondary(a, b)
      cmp -> cmp
    end
  end
end
