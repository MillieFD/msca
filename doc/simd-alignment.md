### Alignment

**Targeted 64-bit alignment** is applied to critical data for SIMD vectorisation and cache-line efficiency. Padding is
inserted immediately **before** critical fields. Alignment is not enforced for small or non-critical fields to improve
on-disk storage efficiency.

##### Aligned Fields

| Location     | Type    | Field           | Reason                                                               |
|--------------|---------|-----------------|----------------------------------------------------------------------|
| Data Segment | All     | Serialized Data | Primary SIMD target; misalignment silently degrades vectorised reads |
| Data Segment | Option  | Null Bitmap     | Iterated alongside serialized data; must be cache-line paired        |
| Data Segment | Unsized | Offsets         | Lookup hot-path; alignment benefits traversal efficiency             |

Exactly one padding region is inserted per critical region; only required following regions which do not terminate at
the 64-bit alignment boundary.

##### Unaligned Fields

| Location       | Reason                                                                 |
|----------------|------------------------------------------------------------------------|
| File Header    | Read once when file opened; zero benefit from alignment                |
| Segment Header | Used for data recovery only; hot-path accesses via manifest            |
| Schema Segment | Deserialised once into owned descriptor; not accessed on the hot path  |
| Manifest       | Variable-length CBOR deserialised once into owned structure            |
| Metadata       | Free-form binary region; layout and alignment delegated to implementor |

Byte order is little-endian throughout.