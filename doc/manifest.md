A self-describing **CBOR** file `manifest` is written immediately after the immutable segment region. The manifest lists
all file segments by type, acting like the index of a book to enhance segment discovery and enable O(1) random access.

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
└─ metadata: Sector (optional)
```

Schema lookup by name returns the corresponding schema descriptor which includes:

1. `Sector` containing the on-disk schema segment
2. `BTreeMap` storing column descriptors keyed by name and sorted by lexicographic order

The manifest stores a lightweight **descriptor** for each on-disk [buffer](on-disk-format#columnar-data-buffers). These
descriptors **do not** hold data directly; they exist to drive discovery and predicate pruning before any file IO
occurs. Unordered items – such as IEEE-754 `NaN` – are excluded from the buffer statistics. Columns with no meaningful
order leave the bounds unset. Column lookup by name returns the corresponding collection of buffer descriptors across
all on-disk data segments.

```text
buffer descriptor
├─ full                  // standard buffer carrying `count` serialized rows
│  ├─ sector: Sector     // buffer location in the immutable region
│  ├─ count: NonZeroU64  // logical number of items in this buffer
│  ├─ min: LE bytes
│  └─ max: LE bytes
└─ lite                  // compact buffer; sector spans ONE serialized row
   ├─ sector: Sector
   └─ count: NonZeroU64
```

The buffer `count` fields drive [index-based random access](index-random-access.md). The sum of every segment `count`
yields the total committed item count for a given schema. Cumulative `count` arithmetic locates the buffer holding the
requested index.

[1]: write-cycle.md