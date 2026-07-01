# Clem Format Specification

This document describes the on-disk layout of a clem file. It is intended as a reference for end-users and implementers.

The self-describing clem format has been carefully designed to maximise query performance and space efficiency while
remaining deterministic and portable across all platforms and architectures. This file does not describe the in-memory
layout which may vary between platforms and releases.

> [!NOTE] LE Byte Order
> Byte order is **little-endian** throughout. All sizes and offsets are encoded using `u64`. Platform-dependent types
> such as `usize` are deliberately omitted to ensure file portability.

This file describes how a single
self-describing file is partitioned into segments, how those segments encode columnar data, and how a reader navigates
the file with minimal IO.

### File Anatomy

Every clem file begins with a fixed-size [file header](#file-header), followed by a variable-length region of immutable
[segments](#sectors-and-segments). The file ends with a CBOR [manifest](#manifest) and optional [metadata](#metadata).

```text
[Header] [Segment 0] ... [Segment N] ... [Manifest] [Metadata]
                               tail ↑   ↑ manifest.offset
```

A transient empty region may exist between the final segment and the manifest after the [write-cycle](./write-cycle.md).
This region is unreferenced and invisible to readers; it is reclaimed during the next write-cycle.

### Sectors and Segments

Two abstractions are used to describe contiguous byte regions in the file: Sectors and Segments.

`Sector` provides a minimal building block to locate a byte range using a starting `offset` and non-zero `length`.
Sectors can point anything from a single columnar buffer to an entire segment.

```text
sector
├─ offset: u64
└─ length: NonZeroU64
```

Data is recorded using self-describing segments which are immutable once written. Each segment begins with a minimal
[segment header](#segment-header) consisting of a variant identifier and length. The variant-specific payload may be
followed by a zero-filled padding region to maintain [64-bit SIMD alignment](./simd-alignment.md).

```text
[Variant] [Length] [Payload] [Padding]
```

Two segment variants are currently defined.

| Variant |  Byte  | Payload                                                        |
|--------:|:------:|:---------------------------------------------------------------|
|  Schema | `0x01` | The [structure](#schema-segments) of an encoded type.          |
|    Data | `0x02` | The [columnar buffers](#data-segment) for one schema instance. |
|  Binary | `0x03` | Free-form immutable binary data                                |

Multimodality and schema evolution are realised by appending additional schema segments. Data storage and file
extensibility are realised by appending additional data segments. Format extensibility may be achieved via the
introduction of new segment variants in future releases.

### File Header

The header is the only file region with a fixed size and offset; essential for providing an entry point for
uninitialised readers. The header begins with a magic byte sequence and version number used to identify the clem format,
followed by the mutable pointers required to bootstrap navigation.

```text
file header
├─ magic: [u8; 4]      // b"clem"
├─ version: u8
├─ tail: NonZeroU64    // offset immediately after the final segment
├─ manifest: Sector    // offset + length of the encoded manifest
└─ alignment padding   // zero-filled to the next 64-bit boundary
```

##### Magic Bytes

Used to identify the file type. Implementers may prepend their own file header – e.g. to indicate a specific file type
built atop `clem` with a canonical schema – but must remove the prepended data before passing to the underlying reader.
Readers must reject any file that does not begin with the expected magic byte sequence.

##### Version Number

A major version number is embedded in the file header to indicate breaking changes in the format specification. Forwards
and backwards compatibility across version numbers is not guaranteed. Readers must reject any file with an unrecognised
version number.

##### Tail Pointer

Mutable pointer recording the byte offset immediately following the final committed segment. New segments are always
appended from `tail`, not from EOF. An empty region may exist between `tail` and the start of the manifest when
appending segments that are shorter than the combined manifest and metadata. This empty region is reclaimed during the
next write-cycle.

##### Manifest Sector

A [sector](#sectors-and-segments) to locate the CBOR manifest written after the immutable segment region.

##### Alignment Padding

The memory map is page-aligned. Padding the header to a 64-bit boundary is therefore essential to keep all subsequent
segments aligned both on-disk and in-memory.

### Segment Header

Every segment begins with a minimal header containing information shared by all variants. The header is used by
sequential readers to identify the segment type and skip if necessary without deserialisation.

```text
segment header
├─ variant: u8       // segment variant identifier
└─ size: NonZeroU64  // length of the segment in bytes
```

The `size` field encodes the number of bytes to the start of the next segment. Headers allow the entire segment region
to **self-describe**: a sequential reader can walk the segment region end-to-end using information contained solely in
the segment headers and dispatch relevant segment body deserialisation based on the `variant` identifier. This is the
basis for [manifest recovery](#durability-and-recovery).

##### Alignment Padding

The header is excluded from the sector recorded in the [manifest](#manifest) and is read **exclusively** during
[manifest recovery](#durability-and-recovery); the optimised random-access read path routes fearlessly to the relevant
segment body region without boundary checks or variant verification. [SIMD alignment](./simd-alignment.md) to the next
64-bit boundary is therefore applied selectively based on the segment variant:

- **Data segments** include a zero-filled padding region inserted after the [metadata](#data-segments) to ensure all
  subsequent [columnar data buffers](#columnar-data-buffers) are begin at a 64-bit boundary.
- **Schema segments** are unaligned to improve on-disk storage efficiency.

### Schema Segments

A schema segment is used to describe the **structure** of an encoded item type. Edge nodes from the hierarchical type
graph are flattened into a platform-invariant and deterministically-ordered sequence of column descriptors, each mapping
a field `name` to its corresponding `type`. The segment body encodes this structure using a CBOR map.

```text
schema segment
├─ segment header
│  ├─ variant: u8 = 0x01
│  └─ size: NonZeroU64
├─ segment body: CBOR
└─ alignment padding
```

Each schema segment encodes **one** schema and each clem file requires at least **one** schema segment. Multimodality
and schema evolution are achieved by appending additional segments.

### Data Segments

A data segment stores the **columnar buffers** for a single schema instance. Each data segment is associated with
**one** schema segment via the `schema` offset field for [data integrity](#durability-and-recovery); the optimised read
path resolves columns for a known schema via the [manifest](#manifest).

```text
data segment
├─ segment header
│  ├─ variant: u8 = 0x02
│  └─ size: NonZeroU64
├─ segment metadata
│  ├─ schema: NonZeroU64  // offset of the associated schema segment
│  ├─ count: NonZeroU64   // number of encoded items
│  └─ alignment padding
└─ segment body
   ├─ 1st buffer
   ⋮
   └─ nth buffer
```

A metadata region is included directly after the segment header containing:

1. A pointer to the associated schema segment which must be written to the file before this data segment.
2. An item `count` indicating the total number of encoded rows; used for index-based random-access reads.
3. A zero-filled padding region to maintain [64-bit SIMD alignment](./simd-alignment.md).

All data buffers are guaranteed to begin at a 64-bit boundary. The number and order of buffers is determined by the
associated schema segment.

### Buffer Header

Every buffer begins with a minimal header containing information shared by all variants.

```text
buffer header
└─ size: NonZeroU64  // length of the buffer in bytes
```

The `size` field encodes the number of bytes to the start of the next buffer. Headers allow the entire buffer region to
**self-describe**: a sequential reader can walk the data segment body end-to-end using information contained solely in
the buffer headers and associated schema segment. Buffer deserialisation is informed by `type` information from the
schema segment; the reader advances to the next deserialisation strategy at the end of each buffer (indicated by `size`
in the buffer header) and ceases deserialisation at the end of the segment (indicated by `size` in the segment header).
This is the basis for [manifest recovery](#durability-and-recovery).

The header is excluded from the sector recorded in the [manifest](#manifest); the optimised random-access read path
routes fearlessly to the relevant buffer without boundary checks or type verification.

### Columnar Data Buffers

Each schema column maps to one contiguous **buffer** within the data segment body. The buffer header is followed by a
buffer body containing end-to-end serialised data. The final item may be followed by a variable-length zero-filled
padding region to maintain [64-bit SIMD alignment](./simd-alignment.md).

```text
[Header] [Body] [Padding]
```

Item serialization is determined by the column `type` described in the associated schema segment.

| Size     | Optional  | Example              | Serialization Strategy                     |
|----------|-----------|----------------------|:-------------------------------------------|
| one bit  | no        | `bool`               | bit-packed in LSB0 order                   |
| fixed    | no        | `i32`                | direct LE representation                   |
| variable | no        | `Vec<f64>`           | offset region + concatenated data region   |
| fixed    | non-niche | `Option<u64>`        | validity bits + concatenated data region   |
| fixed    | niche     | `Option<NonZeroU64>` | concatenated data only; niche encodes none |
| variable | yes       | `Option<String>`     | offset region + concatenated data region   |

Each buffer body (the primary SIMD target) is aligned to a 64-bit boundary. The final serialized item may be followed by
a variable-length zero-filled padding region to maintain this alignment.

##### Unsized Buffers

It is not possible to predetermine the disk space required for each instance of an [unsized] type; there is no guarantee
that two `Vec<I>` instances will contain the same number of elements. Clem therefore unfolds unsized types into:

1. Initial `ends` region describing boundaries.
2. Contiguous `data` region encoding values.

This design enables **O(1) random access** and avoids per-element pointer chasing. Sequential scans across the contained
elements remain linear; leveraging columnar optimisations for SIMD and prefetch.

The `ends` region holds one `u64` **cumulative end offset** for each unsized item, where `0` corresponds to the start of
the contiguous `data` region and item `i` spans `ends[1 - 1]..ends[i]` with an implicit leading offset of zero. Multiple
consecutive equal offsets therefore indicate empty items. The number of end offsets is equal to the item `count`
recorded in the [data segment metadata](#data-segments).

```text
ends: [3, 6, 6, 8]
data:  [a, b, c, d, e, f, g, h]
```

The serialized on-disk example above (four items) is deserialized into the memory representation below.

> **Planned Feature:**
> Implementers will be able to specify the underlying offset type based on the number of expected elements.

```text
Row 0 → values[..3] → "abc" // implicit leading zero
Row 1 → values[3..6] → "def"
Row 2 → values[6..6] → "" (empty)
Row 3 → values[6..8] → "gh"
```

Nested unsized types use **multiple offset layers** alongside a **single data buffer**. This composable design preserves
the performance advantages associated with contiguous value storage; namely predictable vectorised traversal. Scanning
performance across the concatenated data region is unaffected by deep nesting. The inner offsets buffer is written in
order of traversal to improve cache locality during nested iteration and reduce TLB misses.

```text
outer ends
inner ends
data
```

##### Compact Buffers

Real-world applications often require the inclusion of columns with infrequently altered values; typically carrying
categorical data such as sensor type, device ID, or location. It is possible for a column to contain only **one**
repeated value across an entire data segment. Instead of repeatedly encoding identical values, clem defaults to a
**compact buffer** representation to improve storage density.

> **Prefer binary segments for constant values**
> Implementers are encouraged to use a `bin` segment for genuinely constant values that never change across the entire
> file lifetime. This improves storage efficiency by eliminating an unnecessary column from the schema.

Compact buffers contain exactly **one** value – regardless of the segment header `count` – and are therefore detected
automatically by the file reader when the buffer header `next` offset is reached after deserialising a single value.
The reader returns a looped iterator yielding this value `count` times.
