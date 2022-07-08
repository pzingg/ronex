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

  def ref_ndx(%Chronofold{notes: notes}, ndx) do
    case Map.get(notes, ndx) do
      nil -> nil
      note -> Keyword.get(note, :ref)
    end
  end

  def next_ndx(%Chronofold{notes: notes}, ndx) do
    case Map.get(notes, ndx) do
      nil -> nil
      note -> Keyword.get(note, :next)
    end
  end

  def auth_note(%Chronofold{notes: notes}, ndx) do
    case Map.get(notes, ndx) do
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
              case ref_ndx(cf, last_op.ndx) do
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
              if ndx == rndx + 1 do
                # Normal flow
                cf
              else
                # Add a new jump from rndx to us and from us
                # to rndx + 1
                nndx = next_ndx(cf, rndx) || rndx + 1

                cf
                |> add_note(rndx, :next, ndx)
                |> add_note(ndx, :next, nndx)
              end
          end

        cf =
          if is_nil(from_ref) do
            case next_ndx(cf, last_op.ndx) do
              nil ->
                cf

              nndx ->
                # We have to move the ref jump from last_op to this one
                # :inf or an ndx
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
    {lref_andx, lref_auth} = get_ref!(cf, last_op)

    # IO.puts(
    #  "new_tree_ref last (#{last_op.auth} #{last_op.andx}) ref (#{ref_auth} #{ref_andx}) lref (#{lref_auth} #{lref_andx})"
    # )

    if last_op.auth == ref_auth && last_op.andx == ref_andx do
      nil
    else
      if lref_andx == ref_andx && lref_auth == ref_auth do
        nil
      else
        case at_ts(cf.log, {ref_andx, ref_auth}) do
          nil ->
            nil

          %LogOp{val: :root} ->
            nil

          %LogOp{ndx: rndx} ->
            # IO.puts("new_tree_ref rndx #{rndx}")
            rndx
        end
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
    case next_ndx(cf, op.ndx) do
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
end
