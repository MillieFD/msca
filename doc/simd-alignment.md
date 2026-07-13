### Alignment

**Absolute 64-bit alignment** is applied to critical data for SIMD vectorisation and cache-line efficiency. Up to seven
(7) zero-filled bytes are inserted immediately **before** critical regions. Alignment is not enforced for small or
non-critical fields to improve on-disk storage efficiency.

Segments are **densely** packed: the next segment begins immediately after the preceding checksum with no inter-segment
padding. Each segment can therefore begin at any byte offset. Critical regions are aligned to an **absolute** 64-bit
boundary measured relative to the page-aligned memory map; not relative to the segment start.

##### Aligned Fields

| Location     | Type    | Field           | Reason                                                                |
|--------------|---------|-----------------|-----------------------------------------------------------------------|
| File Header  | Global  | Whole Header    | Memory map is page-aligned; in-memory and on-disk alignment are equal |
| Data Segment | All     | Serialized Data | Primary SIMD target; misalignment silently degrades vectorised reads  |
| Data Segment | Option  | Null Bitmap     | Iterated alongside serialized data; must be cache-line paired         |
| Data Segment | Unsized | Offsets         | Lookup hot-path; alignment benefits traversal efficiency              |

Exactly one padding region is inserted per critical region; only required following regions which do not terminate at
the 64-bit alignment boundary. Padding is zero-filled and carries no meaning. All padding is contained in the segment
body with no inter-segment alignment. The file stores the minimum number of zero bytes strictly required to reach the
next absolute 64-bit boundary.

##### Unaligned Fields

| Location       | Reason                                                                 |
|----------------|------------------------------------------------------------------------|
| File Header    | Read once when file opened; zero benefit from alignment                |
| Segment Header | Used for data recovery only; hot-path accesses via manifest            |
| Schema Segment | Deserialised once into owned descriptor; not accessed on the hot path  |
| Manifest       | Variable-length CBOR deserialised once into owned structure            |
| Metadata       | Free-form binary region; layout and alignment delegated to implementor |

Byte order is little-endian (LE) throughout.