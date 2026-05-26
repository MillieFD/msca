### Read Cycle

The read cycle is built upon two complementary principles:

1. **Manifest-driven random access** with predicate pruning to minimise unnecessary IO.
2. **Granular cooperative locking** to operate **multiple** parallel readers concurrently alongside up to **one** active
   writer without contention.

Reading data from the file – across an arbitrary number of segments – requires up to three phases:

##### Phase 1: Manifest Resolution

The `Dataset` contains a `RwLock<Manifest>` field which is lazily initialised from disk on first access by:

1. Reading the file header to determine manifest `offset` and `length`.
2. Deserializing the on-disk CBOR manifest into an in-memory `Manifest` instance.
3. Downgrading access to read guard to minimise contention.

All `write` operations update the manifest in-memory before commiting to disk. All subsequent `read` operations acquire
a shared read guard and return the manifest immediately without any file IO.

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

After pruning, the shared manifest read guard is released and the retained segments are passed to phase three.

##### Phase 3: Lazy Async Zero-Copy Batched Reads

Candidate segments are packaged into a lazy async zero-copy reader that chains across sectors, presenting a flattened
stream of deserialized rows to the caller. Internally, the reader is batched to reduce syscall overhead; returning one
row each time `next` is called and only executing batched file IO when the internal buffer is exhausted.

##### Concurrency Model

Segments are immutable once written, meaning readers do not require coordination after extracting their list of
candidate segments in phase two. A concurrent writer appending a new segment must acquire an exclusive write-guard to
update the manifest and file header. This temporarily blocks new readers from accessing the manifest, but does not
affect in-flight reads.

| Operation                              | Lock mode   | Duration                                   |
|----------------------------------------|-------------|--------------------------------------------|
| First manifest load                    | Write       | File header read + CBOR decode.            |
| Subsequent manifest access (all reads) | Shared read | Extracting the list of candidate segments. |
| Writer updating header + manifest      | Write       | Phases 2 and 4 of the write cycle only.    |
| Reading segment data from disk         | **None**    | Segments are immutable; no lock required.  |

This design ensures:

- **Multiple readers** can resolve the manifest and build candidate segment lists simultaneously.
- **A writer** updating the manifest does not block phase three readers.
- **Segment IO** is fully parallel; readers and writers never contend on per-segment data regions.

The read cycle is implemented by the `Query` builder.