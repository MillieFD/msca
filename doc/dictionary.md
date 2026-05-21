### Dictionaries

The storage cost of large types with repetitive values can be amortised using a dictionary, which is implemented as an
ergonomic feature-gated abstraction over existing schema and data segments coordinated via the [manifest][1].

```text
manifest
├─ schemas: BTreeMap
├─ dictionaries: BTreeMap (optional)
├─ indexes: BTreeMap (optional)
└─ metadata: Sector (optional)
```

The manifest stores dictionary descriptors keyed by `name` in a `BTreeMap` sorted by lexicographic order; ensuring a
platform-agnostic deterministic layout with efficient lookup. Dictionary lookup by name returns the corresponding
dictionary descriptor which includes:

1. `Sector` containing the on-disk schema segment
2. `BTreeMap` storing column descriptors keyed by name and sorted by lexicographic order

The `Dataset::dictionary` function is used to create and retrieve named dictionaries. Callers must `await` due to file
IO on the creation branch; existing dictionaries return `Poll::Ready` immediately. Dictionaries are **not** duplicated
in the general purpose manifest `schemas` map.

##### Dictionary Schema

A dictionary inherently requires two opposing access patterns:

1. **Keys** optimised for search performance → columnar
2. **Values** optimised for extraction and reconstruction → row-oriented

Dictionaries are built using the standard schema and data segment architecture; with implementor-defined `key` and
`value` types. Dictionary entries are accumulated in-memory and written to disk as a data segment. Additional entries
can be added to an existing dictionary by appending additional data segments, with uniqueness enforced during in-memory
key accumulation.

Key instances are stored directly using a columnar buffer. `Sized` and `Ord` trait bounds are required on the `key` type
for this reason. Each value instance is considered unsized to enforce contiguous on-disk storage and improve extraction
efficiency.

##### Index Dictionaries

A specialised `Index` dictionary implementation is provided for entries keyed by insertion order. The key is
automatically incremented for each `push` call; creating a new index initialises the key at zero, whereas opening an
existing index eagerly reads the max existing key from the manifest. An index is recommended for dense ordered data
where position is the only required identifier.

```text
push(value 0) → key 0
push(value 1) → key 1
push(value 2) → key 2
```

The `Index` is implemented using a standard dictionary with one notable optimisation: the on-disk `keys` column is
omitted as values are searched by index. The manifest stores `count` for each data segment which enables direct access
via index arithmetic; if data segment 0 contains `100` values and data segment 1 contains a further `45` values, entry
number `110` is located at index `10` in data segment 0.

Dictionaries and indexes are not enabled by default. To use these abstractions, enable the `dictionary` or `index`
features in your `Cargo.toml`.

[1]: manifest.md