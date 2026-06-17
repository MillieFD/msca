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
Sector
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
|  Schema | `0x01` | The [structure](#schema-segment) of an encoded type.           |
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
Header
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
Segment Header
├─ variant: u8       // segment variant identifier
└─ next: NonZeroU64  // byte offset for the next segment header
```

The `next` field encodes the offset to the start of the next segment header which increases monotonically. Headers
allow the entire segment region to **self-describe**: a sequential reader can walk the segment region end-to-end using
information contained solely in the segment headers and dispatch relevant segment body deserialisation based on the
`variant` identifier. This is the basis for [manifest recovery](#durability-and-recovery).

The header is excluded from the sector recorded in the [manifest](#manifest); the optimised random-access read path
routes fearlessly to the relevant segment body region without boundary checks or variant verification.

> [!TODO] Refactor Length to Next
> The segment header currently encodes the segment body `length` instead of the `next` offset. A refactor is required to
> bring the codebase in line with this specification document.

> [!TODO] Header Padding
> The segment header is currently unpadded which causes misalignment of the segment body (does not start at a 64-bit
> boundary). Is it beneficial to add a varible-length zero-filled padding region after the header `next` field?
