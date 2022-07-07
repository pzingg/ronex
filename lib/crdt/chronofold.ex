defmodule Crdt.Chronofold do
  @moduledoc """
  Chronofold CRDT.

  Replicated Causal Tree.

  The RCT is a tuple `R = ⟨T, val, ref, log⟩`
  such that `T` is a set of timestamps, `val`, `ref` and
  `log` are functions as defined below.

  A chronofold is a subjectively ordered log of tuples
  `⟨val(α i), ndx α (w(α i))⟩`, `i ⩽ lh(α)`, where `w(α i)` is
  the operation following α i in the weave. So, the
  second element of the tuple forms a linked list that
  contains the weave and thus any version of the text.
  """

  @doc """
  Returns the 60-bit unsigned integer that corresponds
  to the string representation for an author.
  """
  def author(str) do
    len = String.length(str)

    if len == 0 || len > 10 do
      raise "author must be a string of length between 1 and 10"
    end

    {:ok, {auth_uuid, ""}} = UUID.parse(str)
    auth_uuid.lo
  end

  @doc """
  `log` is a function from the set `proc(R) ∶= {auth(t) ∶ t ∈ T}`
  to the set of injective sequences in `T`, which associates to
  every process `α ∈ proc(R)` the sequence
  `log(α) = ⟨α 1 , α 2 , . . . , α lh(α)⟩`.
  """
  def log(state, auth, opts \\ []) do
    log_ops(state, auth, opts)
    |> Enum.map(fn op -> op.event end)
  end

  def log_ops(state, _auth, opts \\ []) do
    ops =
      Enum.reduce(state, [], fn
        %Op{term: :header}, acc -> acc
        %Op{term: :query}, acc -> acc
        %Op{event: %UUID{lo: 0}}, acc -> acc
        op, acc -> [op | acc]
      end)
      |> Enum.reverse()

    case Keyword.get(opts, :k) do
      nil -> ops
      k -> Enum.take(ops, k)
    end
  end

  def first_auth!(state) do
    case Enum.find_value(state, fn
           %Op{term: :header} -> false
           %Op{term: :query} -> false
           %Op{event: %UUID{lo: 0}} -> false
           %Op{event: %UUID{lo: auth}} -> auth
         end) do
      nil -> raise "no authors in state"
      auth -> auth
    end
  end

  @doc """
  `LOG k(γ)` is the set `{γ 1 , . . . , γ k}` for
  `γ ∈ proc(R)` and `k ⩽ lh(γ)`.
  """
  def log_k(state, auth, k) do
    log(state, auth, k: k)
  end

  @doc """
  Returns the length of the sequence `log(α)`.
  """
  def lh(state, auth) do
    log(state, auth)
    |> Enum.count()
  end

  def event_matches?(%Op{term: :header}, _reference), do: false
  def event_matches?(%Op{term: :query}, _reference), do: false
  def event_matches?(%Op{event: ev}, reference), do: UUID.equals?(ev, reference)
  def event_matches?(_, _reference), do: false

  @doc """
  `ref` is a function from `T` to `Op`.
  """
  def ref(state, reference) do
    if reference.lo == 0 do
      nil
    else
      case Enum.find(state, &event_matches?(&1, reference)) do
        nil -> nil
        %Op{event: ev} -> ev
      end
    end
  end

  def refs(state) do
    Enum.reduce(state, [], fn
      %Op{term: :header}, acc ->
        acc

      %Op{term: :query}, acc ->
        acc

      %Op{reference: %UUID{lo: 0}}, acc ->
        acc

      %Op{reference: parent}, acc ->
        case ref(state, parent) do
          nil -> acc
          r -> [r | acc]
        end
    end)
    |> Enum.reverse()
  end

  @doc """
  `val` is a function with domain `T`.
  """
  def val(%Op{term: :header}), do: nil
  def val(%Op{term: :query}), do: nil
  def val(%Op{atoms: [0]}), do: :root
  def val(%Op{atoms: [-1]}), do: :del
  def val(%Op{atoms: [val]}) when is_binary(val), do: val
  def val(_), do: nil

  def vals(state) do
    Enum.reduce(state, [], fn op, acc ->
      case val(op) do
        nil -> acc
        v -> [v | acc]
      end
    end)
    |> Enum.reverse()
  end

  def reduce(state, updates) do
    all_ops = state ++ List.flatten(updates)

    case all_ops do
      [header | [%Op{event: %UUID{lo: auth}} = root | rest]] ->
        # Root is always self-referential
        root = %Op{root | reference: root.event}

        [header | [root | rest]]
        |> log_ops(auth)

      empty ->
        empty
    end
  end

  @doc """
   A chronofold is a subjectively ordered log of tuples
  `⟨val(α i), ndx α (w(α i))⟩`, `i ⩽ lh(α)`, where `w(α i)` is
  the operation following `α i` in the weave.

  Once process α receives an op `⟨i, β⟩`, it appends an entry to its
  chronofold. Next, it has to find the op's position in the weave and
  relink the linked list to include the new op at that position. It
  locates the new op's CT parent `ref(⟨i, β⟩) = ⟨k, γ⟩ = α j` at
  the index `j` in the local log. Here, `k < i` and `k ≤ j`; most of
  the time we simply have `j = k`. It inserts the op after its parent,
  unless it finds preemptive CT siblings at that location (those are
  ops with greater timestamps also having `⟨k, γ⟩` as their parent).
  If found, the new op is inserted after preemptive siblings and
  their CT subtrees.
  """
  def map(state) do
    Enum.reduce(state, {[], []}, fn op, acc -> receive_op(op, acc) end)
  end

  def map_result(cfd) do
    Enum.reduce(cfd, "", fn
      {ch, _ndx}, acc when is_binary(ch) -> acc <> ch
      {:root, _ndx}, _acc -> ""
      {:del, _ndx}, acc -> String.slice(acc, 0, String.length(acc) - 1)
    end)
  end

  def format(cfd) do
    Enum.map(cfd, fn {val, ndx} -> "⟨#{val}, #{ndx}⟩" end)
  end

  def receive_op(%Op{term: :header}, acc), do: acc
  def receive_op(%Op{term: :query}, acc), do: acc
  def receive_op(%Op{event: %UUID{lo: 0}}, acc), do: acc

  def receive_op(%Op{event: ev, reference: parent} = op, {state, cfd}) do
    value = val(op)
    IO.puts("receive_op ev #{ev} parent #{parent} val #{value}")

    if is_nil(value) || UUID.zero?(parent) do
      IO.puts("no change to state")
      {state, cfd}
    else
      {head, rest, next} =
        if value == :root do
          # Root
          {state, [], nil}
        else
          split_at_parent!(state, ev, parent)
        end

      ndx = Enum.count(head)
      cfd = insert_tuple(cfd, value, ndx)

      state = head ++ [%Op{op | next: next}] ++ rest
      IO.puts("state now #{Frame.format(state, jumps: true)}")
      {state, cfd}
    end
  end

  def receive_op(_, _, acc), do: acc

  def insert_tuple(cfd, value, ndx) do
    {head, rest} = Enum.split(cfd, ndx)
    head ++ [{value, ndx + 1}] ++ rest
  end

  def split_at_parent!(state, ev, parent) do
    case Enum.reduce(state, {[], [], nil}, fn %Op{event: ts} = op, {head, tail, last} ->
           ts_cmp = UUID.compare_hi(ts, ev)

           cond do
             val(op) == :root ->
               {head ++ [op], tail, ts}

             is_nil(last) ->
               {head, tail ++ [op], nil}

             UUID.equals?(last, parent) && UUID.compare_lo(last, ev) == :eq ->
               {head, tail ++ [op], nil}

             ts_cmp == :lt ->
               {head ++ [op], tail, ts}

             true ->
               {head, tail ++ [op], nil}
           end
         end) do
      {state, [], _} ->
        IO.puts("adding to tail")
        {state, [], nil}

      {head, rest, _} ->
        {head, next} = set_next_if_changed_author(head, ev, parent)

        {sibs, tail} =
          Enum.split_with(rest, fn op -> preemptive_sibling_event?(op, ev, parent) end)

        IO.puts("head count #{Enum.count(head)} sib count #{Enum.count(sibs)}")
        {head ++ sibs, tail, next}
    end
  end

  def set_next_if_changed_author(head, ev, parent) do
    case List.last(head) do
      nil ->
        head

      %Op{event: last_ev} ->
        if UUID.compare_lo(last_ev, ev) == :eq do
          {head, nil}
        else
          case Enum.reduce(head, {[], :not_found}, fn
                 op, {state, %Op{} = next} -> {state ++ [op], next}
                 %Op{event: ^parent} = op, {state, _} -> {state ++ [%Op{op | next: ev}], :found}
                 %Op{event: next} = op, {state, :found} -> {state ++ [op], next}
                 op, {state, next} -> {state ++ [op], next}
               end) do
            {state, %Op{} = next} -> {state, next}
            {state, _} -> {state, nil}
          end
        end
    end
  end

  def preemptive_sibling_event?(%Op{term: :header}, _ev, _parent), do: false
  def preemptive_sibling_event?(%Op{term: :query}, _ev, _parent), do: false

  def preemptive_sibling_event?(%Op{event: sib_ev, reference: sib_parent} = op, ev, parent) do
    if !UUID.equals?(sib_parent, parent) do
      false
    else
      case UUID.compare_hi(sib_ev, ev) do
        :eq -> val(op) == :del
        :lt -> true
        _ -> false
      end
    end
  end

  def preemptive_sibling_event?(_, _reference, _event), do: false

  def compare_primary(a, b) do
    cond do
      b.event.lo == a.event.lo -> :eq
      b.event.lo > a.event.lo -> :lt
      true -> :gt
    end
  end

  def compare_secondary(a, b) do
    UUID.compare(a.event, b.event)
  end

  def compare(a, b) do
    case compare_primary(a, b) do
      :eq -> compare_secondary(a, b)
      cmp -> cmp
    end
  end
end
