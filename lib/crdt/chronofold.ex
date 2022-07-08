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

  ####### LogOp stuff

  alias __MODULE__

  defstruct input: [], lh: 0, log: [], notes: %{}, andx_map: nil, ref_map: %{}

  @doc """
  `:input` - a list of `LogOp`s to be processed.
  `:andx_map` - a map from old UUID `value` to the new
    `:andx` logical sequence
  `:ref_map` - a map from `{andx, auth}` timestamp to its parent
    `{andx, auth}` timestamp
  `:input` - a list of `LogOp`s to be processed.
  `:log` - a list of processed `LogOp`s. A function from the set
    `proc(R) ∶= {auth(t) ∶ t ∈ T}` to the set of injective sequences in
    `T`, which associates to every process `α ∈ proc(R)` the sequence
    `log(α) = ⟨α 1 , α 2 , . . . , α lh(α)⟩`
  `:lh` - the number of `LogOp`s in the log
  `:notes` - a map from the `:ndx` of a log to a keyword list
    (the allowed keys are `:ref`, `:next`, and `:auth`)
  """
  def new() do
    %Chronofold{andx_map: MapSet.new()}
  end

  @doc """
  Length of local process's log.
  """
  def lh(%Chronofold{log: lh}), do: lh

  @doc """
  Access the op with `ndx = i`.
  """
  def at(log, i) do
    Enum.find(log, fn %LogOp{ndx: ndx} -> i == ndx end)
  end

  def at_ts(log, {andx, auth}) do
    Enum.find(log, fn
      %LogOp{andx: ^andx, auth: ^auth} -> true
      _ -> false
    end)
  end

  def ts_at(log, i) do
    case at(log, i) do
      nil -> nil
      %LogOp{andx: andx, auth: auth} -> {andx, auth}
    end
  end

  def ref_ndx(%Chronofold{notes: notes}, op) do
    case Map.get(notes, op.ndx) do
      nil -> nil
      note -> Keyword.get(note, :ref)
    end
  end

  def next_ndx(%Chronofold{notes: notes}, op) do
    case Map.get(notes, op.ndx) do
      nil -> nil
      note -> Keyword.get(note, :next)
    end
  end

  def auth_note(%Chronofold{notes: notes}, op) do
    case Map.get(notes, op.ndx) do
      nil -> nil
      note -> Keyword.get(note, :auth)
    end
  end

  def add_note(%Chronofold{notes: notes} = cf, ndx, key, val) do
    next_note = Map.get(notes, ndx, []) |> Keyword.put(key, val)
    %Chronofold{cf | notes: Map.put(notes, ndx, next_note)}
  end

  def remove_note(%Chronofold{notes: notes} = cf, ndx, key) do
    next_notes =
      case Map.get(notes, ndx) do
        nil ->
          notes

        note ->
          if Keyword.has_key?(note, key) do
            case Keyword.delete(note, key) do
              [] -> Map.delete(notes, ndx)
              next_note -> Map.put(notes, ndx, next_note)
            end
          else
            notes
          end
      end

    %Chronofold{cf | notes: next_notes}
  end

  @doc """
  Converts RON Ops to logical ops, and builds the co-structure.
  """
  def parse_input(state, updates) do
    all_ops = state ++ List.flatten(updates)

    cf = Chronofold.new()

    case all_ops do
      [_header | [%Op{event: event} = root | rest]] ->
        # Root is always self-referential
        root = %Op{root | reference: event}

        Enum.reduce([root | rest], cf, fn op, cf -> add_op(op, cf) end)
        |> finalize_input()

      _empty ->
        {:error, "no root"}
    end
  end

  def add_op(%Op{term: :header}, cf), do: cf
  def add_op(%Op{term: :query}, cf), do: cf
  def add_op(%Op{event: %UUID{lo: 0}}, cf), do: cf
  def add_op(%Op{reference: %UUID{lo: 0}}, cf), do: cf

  def add_op(%Op{event: ev, reference: ref, atoms: [0]}, cf) do
    register_op(cf, ev, ref, :root)
  end

  def add_op(%Op{event: ev, reference: ref, atoms: [-1]}, cf) do
    register_op(cf, ev, ref, :del)
  end

  def add_op(%Op{event: ev, reference: ref, atoms: [s]}, cf) when is_binary(s) do
    register_op(cf, ev, ref, String.slice(s, 0, 1))
  end

  def add_op(_, cf), do: cf

  def register_op(
        %Chronofold{input: input, andx_map: andx_map, ref_map: ref_map} = cf,
        %UUID{hi: andx, lo: auth},
        %UUID{hi: ref_andx, lo: ref_auth},
        val
      ) do
    auth = parse_auth(auth)
    andx = parse_andx(andx)
    op = %LogOp{andx: andx, auth: auth, val: val}

    ref_auth = parse_auth(ref_auth)
    ref_andx = parse_andx(ref_andx)

    %Chronofold{
      cf
      | input: [op | input],
        andx_map: MapSet.put(andx_map, andx),
        ref_map: Map.put(ref_map, {andx, auth}, {ref_andx, ref_auth})
    }
  end

  defp parse_auth(auth), do: UUID.u64_to_string(auth)

  defp parse_andx(andx) do
    UUID.u64_to_string(andx, trim: false) |> String.to_integer()
  end

  def finalize_input(%Chronofold{input: input, andx_map: andx_map, ref_map: ref_map} = cf) do
    # Convert to 1-based map, sorted by original times
    andx_map =
      MapSet.to_list(andx_map)
      |> Enum.sort()
      |> Enum.with_index(1)
      |> Enum.into(%{})

    # Update andx in input, add ndx
    input =
      Enum.reverse(input)
      |> Enum.with_index(1)
      |> Enum.map(fn {%LogOp{andx: old_andx} = op, ndx} ->
        %LogOp{op | ndx: ndx, andx: Map.fetch!(andx_map, old_andx)}
      end)

    # Update andx in refs
    ref_map =
      Map.to_list(ref_map)
      |> Enum.map(fn {{old_andx, auth}, {old_ref_andx, ref_auth}} ->
        {{Map.fetch!(andx_map, old_andx), auth}, {Map.fetch!(andx_map, old_ref_andx), ref_auth}}
      end)
      |> Enum.into(%{})

    root = at(input, 1)

    %Chronofold{cf | input: input, log: [root], lh: 1, andx_map: andx_map, ref_map: ref_map}
    |> add_note(1, :next, :inf)
  end

  def process_op(%Chronofold{input: input, log: log, lh: lh} = cf) do
    ndx = lh + 1

    case at(input, ndx) do
      nil ->
        nil

      op ->
        last_op = at(log, lh)
        # auth
        cf =
          if last_is_same_auth?(last_op, op) do
            cf
          else
            add_note(cf, ndx, :auth, op.auth)
          end

        # weave

        from_ref = new_tree_ref(cf, last_op, op)

        cf =
          case from_ref do
            nil ->
              case ref_ndx(cf, last_op) do
                nil ->
                  cf

                :inf ->
                  cf

                rndx ->
                  cf
                  |> remove_note(last_op.ndx, :next)
                  |> add_note(ndx, :next, rndx)
              end

            rndx ->
              cf
              |> add_note(rndx, :next, ndx)
              |> add_note(ndx, :next, rndx + 1)
          end

        cf =
          if is_nil(from_ref) do
            case next_ndx(cf, last_op) do
              nil ->
                cf

              # :inf or an ndx
              nndx ->
                cf
                |> remove_note(last_op.ndx, :next)
                |> add_note(ndx, :next, nndx)
            end
          else
            cf
          end

        insert_op(cf, op, ndx)
    end
  end

  def insert_op(%Chronofold{} = cf, op, ndx) do
    log = cf.log ++ [%LogOp{op | ndx: ndx}]
    lh = Enum.count(log)
    %Chronofold{cf | log: log, lh: lh}
  end

  def last_is_same_auth?(last_op, op) do
    cond do
      last_op.val == :root -> false
      last_op.auth != op.auth -> false
      true -> true
    end
  end

  def new_tree_ref(cf, last_op, op) do
    {ref_andx, ref_auth} = get_ref!(cf, op)
    in_seq = last_op.auth == op.auth && last_op.andx == ref_andx && last_op.auth == ref_auth

    if in_seq do
      nil
    else
      case at_ts(cf.log, {ref_andx, ref_auth}) do
        nil -> nil
        op -> op.ndx
      end
    end
  end

  def get_ref!(%Chronofold{ref_map: ref_map}, op) do
    Map.fetch!(ref_map, {op.andx, op.auth})
  end

  def map(%Chronofold{} = cf, ndx \\ 1, acc \\ "") do
    case do_op(cf, ndx, acc) do
      {nil, acc} -> acc
      {nndx, acc} -> map(cf, nndx, acc)
    end
  end

  def do_op(%Chronofold{} = cf, ndx, acc) do
    op = at(cf.log, ndx)
    if is_nil(op) do
      {nil, acc}
    else
      nndx = follow(cf, op)
      case op do
        %LogOp{val: :root} -> {nndx, acc}
        %LogOp{val: :del} -> {nndx, String.slice(acc, 0, String.length(acc) - 1)}
        %LogOp{val: s} -> {nndx, acc <> s}
      end
    end
  end

  def follow(%Chronofold{} = cf, op) do
    case next_ndx(cf, op) do
      nil -> op.ndx + 1
      :inf -> nil
      nndx -> nndx
    end
  end

  def dump_input(cf) do
    IO.puts("\ninput:")

    for ndx <- 1..Enum.count(cf.input) do
      IO.puts(LogOp.format(at(cf.input, ndx), cf))
    end
  end

  def dump_log(cf) do
    IO.puts("\nlog:")

    for ndx <- 1..cf.lh do
      IO.puts(LogOp.format(at(cf.log, ndx), cf))
    end
  end

  ####### Older stuff

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
  """
  def old_log(state, _auth, opts \\ []) do
    filter_ops(state, opts)
    |> Enum.map(fn op -> op.event end)
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
    old_log(state, auth, k: k)
  end

  @doc """
  Returns the length of the sequence `log(α)`.
  """
  def lh(state, auth) do
    old_log(state, auth)
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

  def old_reduce(state, updates) do
    all_ops = state ++ List.flatten(updates)

    case all_ops do
      [header | [%Op{event: event} = root | rest]] ->
        # Root is always self-referential
        root = %Op{root | reference: event}

        [header | [root | rest]]
        |> filter_ops()

      empty ->
        empty
    end
  end

  defp filter_ops(state, opts \\ []) do
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
  def old_map(state) do
    Enum.reduce(state, {[], []}, fn op, acc -> receive_op(op, acc) end)
  end

  def old_map_result(cfd) do
    Enum.reduce(cfd, "", fn
      {ch, _ndx}, acc when is_binary(ch) -> acc <> ch
      {:root, _ndx}, _acc -> ""
      {:del, _ndx}, acc -> String.slice(acc, 0, String.length(acc) - 1)
    end)
  end

  def old_format(cfd) do
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
