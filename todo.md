### Foundational Functionality (Priority I)

- [ ] Implement `dictionary` and `index` abstractions without adding new segment types.
- [ ] Add a query builder with an async file reader.
    - [x] New query instance via `Dataset::query(&self, name: &str) -> Result<Query, Error>`:
        - [x] Add `query::Column` struct which holds buffers and filters.
        - [x] `Dataset::query` maps the schema `BTreeMap<String, manifest::Column>` → `BTreeMap<String, query::Column>`.
        - [x] New `Query` instances are initialised with every column and every buffer for the specified schema.
        - [x] Columns and buffers are removed by calling filter methods on the `Query` instance (subtractive).
    - [x] Some filters can be applied before file IO:
        - [x] Column-level filters to remove whole columns from the query map.
        - [x] Buffer-level filters to remove buffers from the query map e.g. using min / max statistics.
        - [ ] Columns are removed if their buffer count falls to zero.
    - [x] Other filters must be applied during file IO:
        - [x] `query::Filter` added to a collection owned by the `Query` instance.
        - [x] Use a `HashSet` to ensure filter uniqueness; duplicate filters reduce efficiency.
        - [x] Retain the most constrained conjunction if two filters conflict e.g. `> 20` should replace `> 10`.
    - [x] Some filters can be used before file IO to remove buffers, but must also be evaluated during IO e.g. `range`
    - [x] Add `Read` trait:
        - [x] Implemented **by each storable type**; mirrors `Data` on the write side and subsumes the `Reader` trait.
        - [x] Type-erased `Stream` trait object hides the concrete reader type; mirror the `BoxAcc` accumulator design.
            - `Ctx<'a>` GATs are not dyn-compatible (E0038), so the boxed object erases to `Iterator` directly.
        - [x] Add `Read::Ctx<'a>` GAT to carry per-type streaming context (mirrors `Write::Ctx`):
            - [x] Primitive types read from a `read::Column` (buffer cursor + mmap + filters).
            - [x] Composite types read from a generated context struct holding one `Stream` per external type field.
        - [x] Add `Read::Src<'a>: Default` GAT stateful stream cursor:
            - [x] Byte iterator for fixed-width primitives
            - [x] `&BitSlice` for `bool`
            - [x] Unit type `()` for composites.
        - [x] `Read::filter` evaluates a deserialized value against every column filter.
        - [x] `Read::next` returns an `Outcome` wrapping the next deserialized item.
        - [x] `Read::iter` builds an unboxed stream via `iter::from_fn`:
            - [x] `Outcome::Success(item)` yields `Some(item)`.
            - [x] `Outcome::Excluded` continues to the next item; loops until `Outcome::Success` or `Outcome::Finished`.
            - [x] `Outcome::Finished` yields `None`.
        - [x] `Read::boxed(ctx)` constructor returns a type-erased `Stream`; mirrors `Accumulate::boxed`.
    - [x] Query is converted into an `Iterator` via `Query::read::<I>()` where `I: Read` is the composite rebuilder:
        - [x] `Outcome::Success(item)` yields `Some(Ok(item))`; `Outcome::Error` yields `Some(Err(error))`.
        - [x] `Outcome::Excluded` continues to the next item; the stream ends with `None`.
    - [x] `Query::column` verifies the requested type exactly once and returns a `Stream` for the named column.
    - [x] Generalise `Deserialize` trait w/ a source
        - [x] Add an associated `&'a Src` type to the `Deserialize` trait (mirror Write::Ctx)
        - [x] Primitives deserialize from `&[u8]`
        - [ ] External types deserialize from a composite reader
    - [x] Users can add `#[derive(Read)]` to their external types:
        - [x] Generates an `impl Read for T` where `Read::iter` zips one column `Stream` per field.
        - [x] Rejects the entire item if any sub-stream returns `Outcome::Excluded`.
        - [x] Surfaces `Outcome::Error` eagerly.
        - [x] `Read::boxed` hides the closure type from users behind the type-erased `Stream` trait object.
        - [x] `Query::read::<I>` reads the composite stream for `I`; mirrors `Data::accumulator`.
    - [x] Fix inverted buffer pruning in `Query::range`; overlapping buffers are retained, disjoint buffers removed.
    - [ ] Implement optional and unsized readers: `OptBitVec` + `OptInSitu` + `Seq` + `OptSeq` + `Flatten`
    - [ ] Add remaining query filters: `eq` + `one_of` + `none_of` + `is_some` + `is_none` + `mask` + `limit` + `offset`
    - [ ] `Query::read` and other supporting functions are no longer async; update documentation.
        - [x] Remove async references from [read-cycle.msd](./doc/read-cycle.md)
- [ ] SIMD alignment on all critical data fields.
    - `align` function already exists (unused) in [segment.rs](./clem-core/src/segment.rs).
    - Critical fields are described in [simd-alignment.md](./doc/simd-alignment.md).
- [ ] Standardise buffer sector offset is relative to the immutable segment region excluding the file header:
    - [x] Update `Serialize::sector` and `Header::tail` documentation.
    - [ ] Refactor all buffer offset calculations to reflect this change.
        - [x] `Push for Accumulator` records buffer offsets relative to the mmap (excludes the file header).
        - [ ] `Header::tail`, segment sectors, and manifest sectors still use absolute file offsets.
- [ ] Manifest rebuild function
    - [ ] Triggered automatically during `File::open` if corruption is detected.
    - [ ] Ensure the on-disk layout is sufficiently self-describing to support rebuild.
    - [ ] Identify any redundant on-disk fields not required for the layout to self-describe.
    - [ ] Remove redundant fields to optimise on-disk size.
- [ ] Ensure schema / type verification is performed exactly once; not per-read.
- [ ] Add static assertion for usize into u64, then remove all `try_into` runtime checks with faster unchecked fn.
- [x] Refactor `Buffer` min / max to use `[u8; 16]` instead of `Vec<u8>`

### Path to Prototype (Priority II)

- [ ] Design public `Dataset` API with quality-of-life improvements and documentation.
- [ ] Finish `clem-core` root module (lib) to re-export public API. Check all visibility modifiers.
- [ ] Finalise `clem-derive` procedural macro design.
- [ ] Add README.md including:
    - [ ] What is `clem`; high-level overview with a link to [on-disk-format.md](./doc/on-disk-format.md) for details.
    - [ ] Cite `clem` in academic work; link to CITATION file and instructions for citing the crate.
    - [ ] Why use `clem`; motivation and design goals.
    - [ ] When to use `clem`; ideal use-cases and comparison to alternatives e.g. Apache Parquet or SQL databases.
    - [ ] How to use `clem`; installation instructions and basic usage examples for writing and reading data.
    - [ ] Contributing guidelines; how to report issues and contribute code.
- [ ] Add a CITATION file
- [ ] Add [on-disk-format.md](./doc/on-disk-format.md) describing the on-disk layout in detail; include ASCI diagrams.
- [ ] Resolve discrepancies between [doc](./doc) and actual implementations. Update documentation as needed.
- [ ] Add comprehensive unit tests for core functionalities in each module; cover edge cases.
- [ ] Add round-trip integration tests for `#[derive(Data)]` and `#[derive(Read)]` in "tests" directory.
- [ ] Remove all references to concurrency model `RwLock<Manifest>` in documentation; concurrency is deferred.

### Extend Functionality (Priority III)

- [ ] Fix `serde` feature; needs `serde` feature for `bitvec` dependency.
- [ ] Add support for free-form metadata written after the manifest. Feature-gated. Ignored if the feature is disabled.
- [ ] Add a feature-gated `bin` segment variant for immutable binary data in any format (e.g. TOML) like the manifest.
- [ ] Implement logging macros gated via the `log` feature.

### Crate Features (Priority IV)

- [ ] Add `derive` feature (ON by default) to enable `clem-derive` sub-crate.
- [ ] Add `no-std` feature (OFF by default).
- [ ] Add `async` feature (ON by default) to use `smol::fs` instead of `std::fs`.

### Ecosystem & Tooling (Priority V)

- [ ] Produce a CLI tool for inspecting `clem` files. Write to `clem-cli` subcrate.
