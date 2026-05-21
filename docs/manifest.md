A self-describing **CBOR** file `manifest` is included after the immutable segment region and lists all file segments by
type. The manifest acts like the index of a book to enhance segment discovery and enable O(1) random access.

```text
[header] [segment 1] ... [segment N] [manifest] ... [EOF]
```

The manifest stores segment descriptors keyed by `name` in a `BTreeMap` sorted by lexicographic order; ensuring a
platform-agnostic deterministic layout with efficient lookup. Segment-level statistics – such as min and max values for
data segments – are included to accelerate lookup operations via predicate pruning. An optional `metadata` sector allows
readers to access implementator-specific file level metadata when present. The manifest is moved and updated during each
[write-cycle][1].

```text
manifest
├─ schemas: BTreeMap
├─ dictionaries: BTreeMap (optional)
├─ indexes: BTreeMap (optional)
└─ metadata: Sector (optional)
```

Schema lookup by name returns the corresponding schema descriptor which includes:

1. `Sector` containing the on-disk schema segment
2. `BTreeMap` storing column descriptors keyed by name and sorted by lexicographic order

Column lookup by name returns the corresponding collection of contiguous buffers across all on-disk data segments. Each
buffer descriptor includes:

1. `Sector` containing the contiguous buffer (subset of the data segment)
2. Data statistics such as `min` and `max` for predicate pruning

An optional feature-gated [dictionaries][2] map allows implementors to leverage the manifest to amortise storage costs
for large types with repetitive values.

[1]: write-cycle.md
[2]: dictionary.md