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

  defstruct input: [],
            log: [],
            root: nil,
            tail: nil,
            notes: %{},
            andx_map: nil,
            ref_map: []

  @doc """
  `:input` - a list of `LogOp`s to be processed.
  `:log` - a list of processed `LogOp`s. A function from the set
    `proc(R) ∶= {auth(t) ∶ t ∈ T}` to the set of injective sequences in
    `T`, which associates to every process `α ∈ proc(R)` the sequence
    `log(α) = ⟨α 1 , α 2 , . . . , α lh(α)⟩`
  `:root` - The `⟨i, α⟩` timestamp of the root op
  `:tail` - The `⟨i, α⟩` timestamp of the end of the chronofold (part
    of the "co-structure" of the log).
  `:notes` - A map from an `⟨i, α⟩` timestamp
    tuple of a log entry to a keyword list
    (the allowed keys are `:ref`, `:next`, and `:auth`).
    Also part of the co-structure.
  `:andx_map` - a map from old UUID `value` to the new `:andx` logical
    sequence, built from the parsed RON input
  `:ref_map` - an ordered list of tuples where the first element of
    the tuple is a `⟨i, α⟩` timestamp, and the second element is
    its parent timestamp, built from the parsed RON input
  """
  def new() do
    %Chronofold{andx_map: MapSet.new()}
  end

  @doc """
  Length of local process's log.
  """
  def lh(%Chronofold{log: log}), do: Enum.count(log)

  @doc """
  Access the op with `ndx = i`.
  """
  def at(log, ndx) when is_integer(ndx) do
    Enum.find(log, fn
      %LogOp{ndx: ^ndx} -> true
      _ -> false
    end)
  end

  def at(log, {andx, auth}) do
    Enum.find(log, fn
      %LogOp{andx: ^andx, auth: ^auth} -> true
      _ -> false
    end)
  end

  def at!(log, ndx) when is_integer(ndx) do
    case at(log, ndx) do
      %LogOp{} = op -> op
      nil -> raise "LogOp not found at ndx #{ndx}"
    end
  end

  def at!(log, {andx, auth}) do
    case at(log, {andx, auth}) do
      %LogOp{} = op -> op
      nil -> raise "LogOp not found at {andx, auth}"
    end
  end

  def ndx_of(log, {_, _} = ts) do
    case at(log, ts) do
      %LogOp{ndx: ndx} -> ndx
      nil -> nil
    end
  end

  def ts(%LogOp{andx: andx, auth: auth}), do: {andx, auth}

  def ts(log, ndx) when is_integer(ndx) do
    case at(log, ndx) do
      %LogOp{andx: andx, auth: auth} -> {andx, auth}
      nil -> nil
    end
  end

  def is_root?(_, nil), do: false
  def is_root?(_, ndx) when is_integer(ndx), do: ndx == 1
  def is_root?(%Chronofold{root: root}, {_, _} = ts), do: root == ts

  def is_tail?(_, nil), do: false

  def is_tail?(%Chronofold{log: log} = cf, ndx) when is_integer(ndx),
    do: is_tail?(cf, ts(log, ndx))

  def is_tail?(%Chronofold{tail: tail}, {_, _} = ts), do: tail == ts

  def is_next_ndx?(%Chronofold{log: log}, ts_new, ts_pred) do
    ndx_of(log, ts_new) == ndx_of(log, ts_pred) + 1
  end

  def set_tail(%Chronofold{} = cf, ts), do: %Chronofold{cf | tail: ts}

  def get_next(_, nil), do: nil

  def get_next(%Chronofold{log: log} = cf, ndx) when is_integer(ndx),
    do: get_next(cf, ts(log, ndx))

  def get_next(%Chronofold{} = cf, {_, _} = ts) do
    get_note(cf, ts, :next)
  end

  def set_next(%Chronofold{log: log} = cf, ndx, val) when is_integer(ndx),
    do: set_next(cf, ts(log, ndx), val)

  def set_next(%Chronofold{} = cf, {_, _} = ts, nil) do
    remove_notes(cf, ts, :next)
  end

  def set_next(%Chronofold{} = cf, {_, _} = ts, val) do
    if is_next_ndx?(cf, val, ts) do
      remove_notes(cf, ts, :next)
    else
      add_note(cf, ts, :next, val)
    end
  end

  def get_ref(_, nil), do: nil
  def get_ref(%Chronofold{log: log} = cf, ndx) when is_integer(ndx), do: get_ref(cf, ts(log, ndx))

  def get_ref(%Chronofold{} = cf, {_, _} = ts) do
    get_note(cf, ts, :ref)
  end

  def set_ref(%Chronofold{log: log} = cf, ndx, val) when is_integer(ndx),
    do: set_ref(cf, ts(log, ndx), val)

  def set_ref(%Chronofold{} = cf, {_, _} = ts, nil) do
    remove_notes(cf, ts, :ref)
  end

  def set_ref(%Chronofold{} = cf, {_, _} = ts, val) do
    add_note(cf, ts, :ref, val)
  end

  def get_auth(_, nil), do: nil

  def get_auth(%Chronofold{log: log} = cf, ndx) when is_integer(ndx),
    do: get_auth(cf, ts(log, ndx))

  def get_auth(%Chronofold{} = cf, {_, _} = ts) do
    get_note(cf, ts, :auth)
  end

  def set_auth(%Chronofold{log: log} = cf, ndx, val) when is_integer(ndx),
    do: set_auth(cf, ts(log, ndx), val)

  def set_auth(%Chronofold{} = cf, {_, _} = ts, nil) do
    remove_notes(cf, ts, :auth)
  end

  def set_auth(%Chronofold{} = cf, {_, _} = ts, val) do
    add_note(cf, ts, :auth, val)
  end

  defp get_note(%Chronofold{notes: notes}, {_, _} = ts, key) do
    case Map.get(notes, ts) do
      nil -> nil
      note -> Keyword.get(note, key)
    end
  end

  defp add_note(%Chronofold{notes: notes} = cf, {_, _} = ts, key, val) do
    next_note = Map.get(notes, ts, []) |> Keyword.put(key, val)
    %Chronofold{cf | notes: Map.put(notes, ts, next_note)}
  end

  defp remove_notes(%Chronofold{notes: notes} = cf, {_, _} = ts, keys) do
    next_notes =
      case Map.get(notes, ts) do
        nil ->
          notes

        note ->
          keys = List.wrap(keys)

          case Keyword.drop(note, keys) do
            [] -> Map.delete(notes, ts)
            next_note -> Map.put(notes, ts, next_note)
          end
      end

    %Chronofold{cf | notes: next_notes}
  end

  def get_successor(_, nil), do: {:error, nil}

  def get_successor(%Chronofold{log: log, tail: tail} = cf, ndx) when is_integer(ndx) do
    ts_at_ndx = ts(log, ndx)

    if ts_at_ndx == tail do
      {:tail, nil}
    else
      case get_next(cf, ts_at_ndx) do
        {_, _} = ts_succ ->
          {:next, ts_succ}

        nil ->
          {:ndx, ts(log, ndx + 1)}
      end
    end
  end

  def get_successor!(%Chronofold{log: log, tail: tail} = cf, {_, _} = ts) do
    if ts == tail do
      {:tail, nil}
    else
      case get_next(cf, ts) do
        {_, _} = ts_succ ->
          {:next, ts_succ}

        nil ->
          op = at(log, ts)

          case op do
            %LogOp{ndx: ndx} -> {:ndx, ts(log, ndx + 1)}
            nil -> raise "No successor for #{LogOp.format(ts)}"
          end
      end
    end
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

        {cf, parse_result} =
          Enum.reduce_while([root | rest], {cf, :ok}, fn op, {cf, _} -> add_op(op, cf) end)

        case parse_result do
          :ok -> finalize_input(cf)
          error -> error
        end

      _empty ->
        {:error, "no root"}
    end
  end

  defp add_op(%Op{term: :header}, cf), do: {:cont, {cf, :ok}}
  defp add_op(%Op{term: :query}, cf), do: {:cont, {cf, :ok}}

  # Fix events that don't have authors?
  defp add_op(%Op{event: %UUID{lo: 0} = ev}, cf) do
    {:halt, {cf, {:error, "op with authless timestamp #{ev}"}}}
  end

  # Fix refs that don't have authors?
  defp add_op(%Op{event: %UUID{lo: evlo}, reference: %UUID{lo: 0} = ref} = op, cf) do
    add_op(%Op{op | reference: %UUID{ref | lo: evlo}}, cf)
  end

  defp add_op(%Op{event: ev, reference: ref, atoms: [0]}, cf) do
    register_op(cf, ev, ref, :root)
  end

  defp add_op(%Op{event: ev, reference: ref, atoms: [-1]}, cf) do
    register_op(cf, ev, ref, :del)
  end

  defp add_op(%Op{event: ev, reference: ref, atoms: [s]}, cf) when is_binary(s) do
    register_op(cf, ev, ref, String.slice(s, 0, 1))
  end

  defp add_op(_, cf), do: {:cont, {cf, :ok}}

  defp register_op(
         %Chronofold{input: input, andx_map: andx_map, ref_map: ref_map} = cf,
         %UUID{hi: andx, lo: auth} = ev,
         %UUID{hi: ref_andx, lo: ref_auth},
         val
       ) do
    auth = parse_auth(auth)
    andx = parse_andx(andx)

    if Enum.find(ref_map, fn
         {{^andx, ^auth}, _ts_ref} -> true
         _ -> false
       end) do
      {:halt, {cf, {:error, "op with duplicate timestamp #{ev}"}}}
    else
      ref_auth = parse_auth(ref_auth)
      ref_andx = parse_andx(ref_andx)
      op = %LogOp{andx: andx, auth: auth, val: val}

      {:cont,
       {%Chronofold{
          cf
          | input: [op | input],
            andx_map: MapSet.put(andx_map, andx),
            ref_map: [{{andx, auth}, {ref_andx, ref_auth}} | ref_map]
        }, :ok}}
    end
  end

  defp parse_auth(auth), do: UUID.u64_to_string(auth)

  defp parse_andx(andx) do
    UUID.u64_to_string(andx, trim: false) |> String.to_integer()
  end

  defp finalize_input(%Chronofold{input: input, andx_map: andx_map, ref_map: ref_map} = cf) do
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
      ref_map
      |> Enum.reverse()
      |> Enum.map(fn {{old_andx, auth}, {old_ref_andx, ref_auth}} ->
        {{Map.fetch!(andx_map, old_andx), auth}, {Map.fetch!(andx_map, old_ref_andx), ref_auth}}
      end)
      |> Enum.sort_by(fn {{t_child, _}, ts_ref} -> {ts_ref, 0 - t_child} end)

    root_op = at(input, 1)
    tail = ts(root_op)

    %Chronofold{
      cf
      | input: input,
        log: [root_op],
        root: tail,
        tail: tail,
        andx_map: andx_map,
        ref_map: ref_map
    }
  end

  @doc """
  Pulls the next op from the `:input` list, and weaves it into
  the chronofold (`:log`).
  """
  def process_op(%Chronofold{input: input} = cf) do
    new_ndx = Chronofold.lh(cf) + 1

    with %LogOp{} = op <- at(input, new_ndx) do
      # Set ndx before we weave
      op = %LogOp{op | ndx: new_ndx}

      cf
      |> do_weave(op)
      |> update_auth_note(op)
    else
      _ -> nil
    end
  end

  # Once process α receives an op `⟨i, β⟩`, it appends an entry to its
  # chronofold. Next, it has to find the op's position in the weave and
  # relink the linked list to include the new op at that position. It
  # locates the new op's CT parent `ref(⟨i, β⟩) = ⟨k, γ⟩ = α j` at
  # the index `j` in the local log. Here, `k < i` and `k ≤ j`; most of
  # the time we simply have `j = k`. It inserts the op after its parent,
  # unless it finds preemptive CT siblings at that location (those are
  # ops with greater timestamps also having `⟨k, γ⟩` as their parent).
  # If found, the new op is inserted after preemptive siblings and
  # their CT subtrees.
  defp do_weave(%Chronofold{log: log} = cf, %LogOp{andx: andx, auth: auth} = op) do
    # ⟨i, β⟩ = i_beta = ts(op)
    {i, _} = i_beta = {andx, auth}
    # ⟨k, γ⟩ = k_gamma = ref(⟨i, β⟩)
    {k, _} = k_gamma = mapped_ref!(cf, i_beta)
    # α j = ⟨k, γ⟩
    j = ndx_of(log, k_gamma)

    IO.puts("\ndo_weave #{LogOp.format(op, cf)} ref:#{LogOp.format(k_gamma)}")
    IO.puts("i #{i} j #{j} k #{k}")

    if j == k do
      IO.puts("j == k")
    end

    {preemptive, rest} = siblings_at(cf, k_gamma, i_beta)

    for op_sib <- preemptive do
      IO.puts("PREMPT: #{LogOp.format(op_sib, cf)}")
    end

    for op_sib <- rest do
      IO.puts("NORMAL: #{LogOp.format(op_sib, cf)}")
    end

    case List.first(preemptive) do
      nil ->
        insert_after(cf, op, k_gamma)

      %LogOp{andx: andx, auth: auth} ->
        ts_tree = {andx, auth}
        ts_pred = subtree_last(cf, ts_tree)
        IO.puts("subtree #{LogOp.format(ts_tree)} last #{LogOp.format(ts_pred)}")
        insert_after(cf, op, ts_pred)
    end
  end

  defp mapped_ref!(cf, %LogOp{} = op), do: mapped_ref!(cf, ts(op))

  defp mapped_ref!(%Chronofold{ref_map: ref_map}, ts_child) do
    case Enum.find(ref_map, fn
           {^ts_child, _ts_ref} -> true
           _ -> false
         end) do
      nil -> raise "ref not found for #{LogOp.format(ts_child)}"
      {_ts_child, ts_ref} -> ts_ref
    end
  end

  defp insert_after(
         %Chronofold{log: log} = cf,
         %LogOp{andx: andx, auth: auth, ndx: ndx} = op,
         ts_pred
       ) do
    IO.puts("insert_after op at #{ndx}")
    ts_new = {andx, auth}

    {%Chronofold{cf | log: log ++ [op]}
     |> update_next_links(ts_new, ts_pred), ts_pred}
  end

  # Return a list of ops in the current log that have "ref" or "pred"
  # pointers to the given timestamp, the list is sorted with the
  # highest timestamp first.
  #
  # Then the list is split into a "preemptive" list, where the timestamp
  # is "higher" than the given timestamp, and an "after" list
  defp siblings_at(
         %Chronofold{log: [_root | rest], ref_map: ref_map},
         ts_ref,
         {i, _beta}
       ) do
    Enum.filter(rest, fn %LogOp{andx: andx, auth: auth} ->
      Enum.member?(ref_map, {{andx, auth}, ts_ref})
    end)
    |> Enum.sort_by(fn %LogOp{andx: t_op} -> 0 - t_op end)
    |> Enum.split_while(fn %LogOp{andx: t_op, val: val} ->
      t_op > i || (t_op == i && val == :del)
    end)
  end

  # Recursive: go down from op.
  # `:acc` - Starting tree.
  # Returns the last op in the subtree (before we hit a "next" or tail).
  defp subtree_last(cf, ts_start, acc \\ nil)

  defp subtree_last(cf, ts_start, nil), do: subtree_last(cf, ts_start, ts_start)

  defp subtree_last(cf, ts_start, acc) do
    succ = succ_in_subtree(cf, ts_start)

    case succ do
      nil -> acc
      succ -> subtree_last(cf, succ, succ)
    end
  end

  defp succ_in_subtree(
         %Chronofold{log: log, tail: tail} = cf,
         ts_start
       ) do
    if ts_start == tail do
      # Stop if we are tail
      nil
    else
      case get_next(cf, ts_start) do
        # Stop if we have a "next"
        {_, _} ->
          nil

        # Step forward 1
        nil ->
          at(log, ndx_of(log, ts_start) + 1) |> ts()
      end
    end
  end

  defp update_next_links(%Chronofold{} = cf, ts_new, ts_pred) do
    case get_successor!(cf, ts_pred) do
      {:tail, nil} ->
        IO.puts("move :tail to #{LogOp.format(ts_new)}")

        cf
        |> set_next(ts_pred, ts_new)
        |> set_tail(ts_new)

      {_, ts_succ} ->
        IO.puts("set :next for pred #{LogOp.format(ts_pred)} to #{LogOp.format(ts_new)}")
        IO.puts("set :next for new #{LogOp.format(ts_new)} to #{LogOp.format(ts_succ)}")

        cf
        |> set_next(ts_pred, ts_new)
        |> set_next(ts_new, ts_succ)
    end
  end

  defp update_auth_note({cf, ts_pred}, %LogOp{andx: andx, auth: auth}) do
    if auth_equals?(cf, ts_pred, auth) do
      set_auth(cf, {andx, auth}, nil)
    else
      set_auth(cf, {andx, auth}, auth)
    end
  end

  defp auth_equals?(%Chronofold{root: root}, {_pred_andx, pred_auth} = ts_pred, auth) do
    if ts_pred == root do
      false
    else
      pred_auth == auth
    end
  end

  @doc """
  Walk the chronfold, assembling the text.
  """
  def map(%Chronofold{} = cf, ndx \\ 1, acc \\ "") do
    case do_op(cf, ndx, acc) do
      {nil, acc} -> acc
      {nndx, acc} -> map(cf, nndx, acc)
    end
  end

  defp do_op(%Chronofold{log: log} = cf, ndx, acc) do
    op = at(log, ndx)

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

  defp follow(%Chronofold{log: log} = cf, %LogOp{andx: andx, auth: auth, ndx: ndx}) do
    if is_tail?(cf, {andx, auth}) do
      nil
    else
      case get_next(cf, {andx, auth}) do
        nil ->
          ndx + 1

        ts_next ->
          ndx_of(log, ts_next)
      end
    end
  end

  def dump_input(%Chronofold{input: input} = cf) do
    IO.puts("\ninput:")

    for ndx <- 1..Enum.count(input) do
      IO.puts(LogOp.format(at(input, ndx), cf))
    end
  end

  def dump_log(%Chronofold{log: log} = cf) do
    IO.puts("\nlog:")

    for ndx <- 1..Enum.count(log) do
      IO.puts(LogOp.format(at(log, ndx), cf))
    end
  end
end
