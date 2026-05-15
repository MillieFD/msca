### File Header

The file header begins with a magic byte sequence used to identify the file type. The file IO
mechanisms defined in this module will reject incorrect magic byte sequences. Implementers may
prepend their own file header – e.g. to indicate a specific file type built atop `clem` with a
canonical schema – but must remove the prepended data before passing to the underlying reader.

```text
File
├─ Header
│  ├─ magic: [u8; 4] // b"clem"
│  ├─ version: u8
│  ├─ tail: NonZeroU64
│  └─ manifest: Sector
├─ Segment 0
⋮
├─ Segment N
├─ Empty (optional)
├─ Manifest
└─ Metadata (optional)
```

A major version number is embedded in the file header to indicate breaking changes in the format
specification. Forwards and backwards compatibility across version numbers is not guaranteed.
Implementers must reject any file with an unrecognised version number.

```text
[Header] [Segment 0] ... [Segment N] ... [Manifest] [Metadata]
                               tail ↑   ↑ manifest.offset
```
The [`tail`][1] field records the byte offset immediately following the final committed segment.
New segments are always appended from `tail`, not from EOF. An empty region may exist between
`tail` and the start of the manifest when appending segments that are shorter than the combined
manifest and metadata. This empty region is filled during the next write-cycle.

[1]: https://doc.rust-lang.org/std/num/type.NonZeroU64.html