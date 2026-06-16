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

