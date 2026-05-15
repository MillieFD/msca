### Write Cycle

Let `m` denote the combined byte length of the existing manifest and metadata (if present). Let `s` denote the byte
length of the incoming segment. The write-cycle exploits the relationship between `s` and `m` to guarantee that the
previous manifest is never overwritten before a new manifest pointer is committed to the file header.

Appending a new segment to the file – regardless of type – requires four phases:

**Phase 1:** Write the new manifest at EOF.

The `Dataset` contains a `RwLock<Manifest>` field which is lazily initialised from disk on first access by:

1. Reading the file header to determine manifest `offset` and `length`.
2. Deserializing the on-disk CBOR manifest into an in-memory `Manifest` instance.

The exiting in-memory manifest is updated to include the incoming segment. The new manifest and metadata (if present)
are written to a postition relative to `tail` depending on `s` and `m`:

- `s > m` → The new segment is larger than the combined existing manifest and metadata. The new manifest is written
  starting `s` bytes after `tail` to reserve the exact disk space required by the incoming segment. This introduces a
  transient empty region between the previous EOF and the new manifest offset.

- `s == m` → The new segment exactly fills the space occupied by the old manifest and metadata. The new manifest is
  written immediately following the prefious EOF with no empty region.

- `s < m` → The new segment is smaller than the combined existing manifest and metadata. The new manifest is written
  immediately following the prefious EOF with no empty region, leaving an unreferenced trailing region from `tail + s`
  to the new manifest offset. This trailing region lies beyond `tail` and is therefore invisible to readers; it is
  naturally overwritten in the next write cycle.

At the end of step 1, the file contains two manifests. The old manifest remains authoritative because the file header
has not yet been updated. A crash in phase 1 leaves the file contents intact. The new manifest is unreferenced and will
be overwritten in the next write-cycle as the `tail` remains unmoved.

```text
                                          Reserved for Incoming Segment
                                    ├───────────────────────────────────────┤
[Header] [Segment 0] ... [Segment N] ... [Prev Manifest] [Prev Metadata] ... [New Manifest] [New Metadata]
                               tail ↑   ↑ manifest.offset
```

**Phase 2:** Update the file header manifest sector.

The manifest `offset` and `length` fields in the file header are overwritten to point to the new manifest. The newly
authoritative manifest references a (currently unwritten) segment after the `tail` pointer. A crash in phase 2 can
therefore be recovered by ignoring any manifest segments with offsets past the `tail` pointer.

```text
                                          Reserved for Incoming Segment
                                    ├───────────────────────────────────────┤
[Header] [Segment 0] ... [Segment N] ... [Prev Manifest] [Prev Metadata] ... [New Manifest] [New Metadata]
                               tail ↑                                       ↑ manifest.offset
```

**Phase 3:** Write the incoming segment.

The incoming segment is written starting from `tail` and overwriting the old manifest and any empty regions if present.
Crash detection and recovery are identical to phase 2.

```text
[Header] [Segment 0] ... [Segment N] [New Segment] ... [New Manifest] [New Metadata]
                               tail ↑                 ↑ manifest.offset
```

**Phase 4:** Update the file header tail pointer.

The `tail` field is advanced to `tail + s`, pointing immediately after the end of the newly written segment. The
write-cycle is complete with `manifest.offset <= tail` and the manifest correctly indexing all committed segments.

```text
[Header] [Segment 0] ... [New Segment] ... [New Manifest] [New Metadata]
                                 tail ↑   ↑ manifest.offset
```