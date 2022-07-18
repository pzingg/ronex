# Ronex

RON for Elixir. An ASCII wire format for CRDT operations.

The information below is condensed from the original
at https://github.com/gritzko/ron#readme

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `ronex` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ronex, "~> 0.2.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/ronex](https://hexdocs.pm/ronex).

## UUIDs

Underlying a RON UUID is a "Base64x64" encoding scheme, using
the characters "0" to "9", "A" to "Z", "_", "a" to "z", and "~"
to represent the values 0 to 63, respectively. This means that
the encoded integer values will be sorted lexicographically.

A UUID has two halves. Each half has a 60 bit unsigned integer
of data in its least significant bits, encoded as 10 ASCII
Base64 characters (6 bits per char).The four most significant
bits of each half are used to specify the `:variety` (in the
`:hi` half) and the `:scheme` (in the `:lo` half).

UUIDs are represented by an Elixir struct `UUID` with these
keys:
* `:hi` - a 60 bit unsigned integer (encoded as 10 chars)
* `:lo` - a 60 bit unsigned integer (encoded as 10 chars)
* `:scheme` - either `:name`, `:hash`, `:event`, or `:derived`
* `:variety` - a 4 bit unsigned integer describing the type of data, such
   as an ISBN number, zip code, etc.

The full 128 bits (including the `:scheme` and `:variety` information)
can be obtained by using the `UUID.to_bitstring/1` function.

## Ops

RON Ops are represented by an Elixir struct `Op` carrying the four UUIDs
for the Op:
* `:type` - a `:name` UUID for the frame type, usually "lww", "rga", or "set"
* `:object` - the target object for the operation
* `:event` - an ordered `:event` or `:derived` UUID
* `:location` - a `:name` UUID defining the location within the target for the operation

the type of the Op, determined by the terminating punctuation (see below):
* `:term` - either `:header`, `:raw`, `:reduced` or `:query`

and a list of atoms (string, integer, or floating point numbers) for the operation:
* `:atoms`

## Frames

A `Frame` is a partially ordered set of `Op`s, further organized into chunks.

A chunk can be either:
1. A `:header` or `:query` Op followed by `:reduced` Ops belonging to the chunk, or
2. A `:raw` Op in its own one-Op chunk.

The `Frame.split/1` function returns the Ops grouped by chunks, where each chunk is
a list of Ops.

## Frame, Op and UUID text encoding

Besides the Base64 characters, other punctuation marks have special meanings:

| Char | Context | Class   | Meaning                                                        |
| ---- | ------- | ------- | -------------------------------------------------------------- |
| \/   | UUID    | Variety | Preceding hex char, "0" to "F", encodes the UUID's `:variety`  |
| \$   | UUID    | Scheme  | Hi-lo separator for a `:name` UUID                             |
| \%   | UUID    | Scheme  | Hi-lo separator for a `:hash` (number) UUID                    |
| \-   | UUID    | Scheme  | Hi-lo separator for an `:event` UUID                           |
| \+   | UUID    | Scheme  | Hi-lo separator for a "`:derived` UUID                         |
| \(   | UUID    | Prefix  | Use first 4 chars of context, then append following chars      |
| \[   | UUID    | Prefix  | Use first 5 chars of context, then append following chars      |
| \{   | UUID    | Prefix  | Use first 6 chars of context, then append following chars      |
| \}   | UUID    | Prefix  | Use first 7 chars of context, then append following chars      |
| \]   | UUID    | Prefix  | Use first 8 chars of context, then append following chars      |
| \)   | UUID    | Prefix  | Use first 9 chars of context, then append following chars      |
| \`   | UUID    | Redef   | Use the UUID from the previous "row" frame                     |
| \*   | Op      | Quant   | Start of Op's data type UUID ("lww", "rga", "set", etc.)       |
| \#   | Op      | Quant   | Start of Op's object UUID                                      |
| \@   | Op      | Quant   | Start of Op's event UUID                                       |
| \<   | Op      | Quant   | Start of Op's reference (predecessor) event UUID               |
| \:   | Op      | Quant   | Start of Op's location UUID                                    |
| \'   | Op      | Atom    | Start and end delimiter for string atom                        |
| \=   | Op      | Atom    | Start of integer atom                                          |
| \^   | Op      | Atom    | Start of floating point atom                                   |
| \>   | Op      | Atom    | Start of UUID UUID array or version vector atom                |
| \!   | Frame   | Term    | End of frame header                                            |
| \?   | Frame   | Term    | End of frame query                                             |
| \;   | Frame   | Term    | End of raw frame                                               |
| \,   | Frame   | Term    | End of reduced frame                                           |
| \.   | Frame   | Term    | Optional end of frame                                          |

## CRDTs

The library includes four example CRDT object implementations:

* `Crdt.Lww` - Last Write Wins
* `Crdt.Set` - Commutative Set with tombstones
* `Crdt.Rga` - Replicated Growable Array
* `Crdt.Chronofold` - A Causal Set-based structure for collaborative text

For examples of the first three, see the original Go implementations at
https://github.com/gritzko/ron

For the paper describing Chronofold, see
https://dl.acm.org/doi/10.1145/3380787.3393680

Note: The `Crdt.Rga` is not working at present (v0.2.0).  Work to do...

## Testing

See original https://github.com/gritzko/ron-test for test suites
