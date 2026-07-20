### File Header

The file header begins with a magic byte sequence used to identify the file type. The file IO mechanisms defined in this
module will reject incorrect magic byte sequences. Implementers may prepend their own file header – e.g. to indicate a
specific file type built atop `msca` with a canonical schema – but must remove the prepended data before passing to the
underlying reader.

```text
Header
├─ magic: [u8; 4]      // b"msca"
├─ version: u8
├─ manifest: Sector    // size & offset of the manifest segment
└─ alignment padding   // zero-filled to the next 64-bit boundary
```

A major version number is embedded in the file header to indicate breaking changes in the format specification. Forwards
and backwards compatibility across version numbers is not guaranteed. Implementers must reject any file with an
unrecognised version number.

```text
[Header] [Segment 0] ... [Segment N] [Manifest] [Metadata]
                                    ↑ manifest.offset
```

A mutable [sector](on-disk-format#sectors-and-segments) locates the [manifest](manifest). The file header carries no
checksum; readers should trust the indicated manifest sector **only if** the [checksum](on-disk-format#segment-checksum)
suffix passes and the CBOR body decodes.

The manifest sector is written immediately after the immutable segment region. The manifest sector offset therefore
doubles as the [write-cycle](write-cycle.md) entry point for new [segments](on-disk-format#sectors-and-segments).

The file header is zero-padded to the next [SIMD alignment boundary](simd-alignment.md).