### Read Cycle

The read cycle is built upon two complementary principles:

1. **Manifest-driven random access** with predicate pruning to minimise unnecessary IO.
2. **Lazy zero-copy streaming** directly from an immutable memory-mapped segment region.

Reading data from the file – across an arbitrary number of segments – requires up to three phases:

##### Phase 1: Manifest Resolution

The `Dataset` holds an in-memory `Manifest` deserialized from disk when the file is opened:

1. Read the file header to determine the manifest `offset` and `length`.
2. Deserialize the on-disk CBOR manifest into an in-memory `Manifest` instance.

All `write` operations update the in-memory manifest before committing it to disk, enabling subsequent `read` operations
to resolve the manifest immediately without any additional file IO.

##### Phase 2: Segment Pruning

The manifest exposes high-level statistics for each column involved in the predicate:

```text
manifest["schema_name"]["column_name"] → [Buffer { sector, count, min, max }]
```

The reader can use these statistics to eliminate segments where the query predicate is provably unsatisfiable.

```text
query.min > buffer.max  →  All values in segment are below the query range
query.max < buffer.min  →  All values in segment are above the query range
```

After pruning, the immutable manifest borrow is released and the retained segments are passed to phase three.

##### Phase 3: Lazy Zero-Copy Reads

Candidate segments are packaged into a lazy zero-copy reader that chains across sectors, presenting a flattened stream
of deserialized items to the caller. The reader pulls bytes directly from the read-only memory map and returns one item
each time `next` is called; no item is deserialized before being requested.

##### Immutable Segments

Segments are immutable once written. A reader extracts its list of candidate segments during phase two and then
deserializes directly from the read-only memory map; segment data regions are never mutated in place. New data is always
appended as additional segments, leaving existing segments – and any in-flight reads over them – untouched.

| Operation                                       | Access    | Duration                         |
|-------------------------------------------------|-----------|----------------------------------|
| Manifest deserialization during `Dataset::open` | Mutable   | File header read + CBOR decode.  |
| Resolving candidates during `Dataset::query`    | Immutable | Schema lookup + Columns copy.    |
| Writer updating in-memory manifest              | Mutable   | Phase 1 of the write cycle only. |
| Reading segment data from disk                  | **None**  | No manifest access required.     |

This design ensures:

- **Multiple readers** can resolve the manifest and build candidate segment lists simultaneously.
- **A writer** updating the manifest does not block phase three readers.
- **Segment IO** is fully parallel; readers and writers never contend on per-segment data regions.

The read cycle is implemented by the `Query` builder.