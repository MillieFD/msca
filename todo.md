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
        - [x] Readers are strongly typed and inherently know how to deserialize bytes into their target Rust type.
        - [x] Type-erased `BoxRead` trait object hides the concrete reader type; mirror the `BoxAcc` accumulator design.
        - [ ] `Read::boxed` trait method to construct a new `BoxRead`; mirror `Accumulate::boxed`.
        - [x] `Read::next` returns an `Outcome` wrapping the next deserialized item.
        - [ ] Any `Read` implementor can be converted into an `Iterator`:
            - [ ] `Outcome::Success(item)` yields `Some(item)`.
            - [ ] `Outcome::Excluded` continues to the next item; loops until `Outcome::Success` or `Outcome::Finished`.
            - [ ] `Outcome::Finished` yields `None`.
    - [ ] Query can be converted into an `Iterator`.
    - [x] `query::Column::read` returns a `BoxRead` for the calling column.
    - [x] Generalise `Deserialize` trait w/ a source
        - [x] Add an associated `&'a Src` type to the `Deserialize` trait (mirror Write::Ctx)
        - [x] Primitives deserialize from `&[u8]`
        - [ ] External types deserialize from a composite reader
    - [ ] Users can add `#[derive(Read)]` to their external types:
        - [ ] Generates a hidden composite reader struct that holds a `BoxRead` for each external type field.
        - [ ] Generated `Read::next` implementation calls `next` on each sub-reader to construct one instance of the
          external type; rejects the entire item if any sub-reader returns `Outcome::Excluded`.
        - [ ] `Read::boxed` hides the generated reader type from users behind a type-erased trait object.
        - [ ] `Query::read::<I>` returns the composite reader for `I`; mirrors `Data::accumulator`.
    - [ ] Implement optional and unsized readers: `OptBitVec` + `OptInSitu` + `Seq` + `OptSeq` + `Flatten`
    - [ ] Add remaining query filters: `eq` + `one_of` + `none_of` + `is_some` + `is_none` + `mask` + `limit` + `offset`
- [ ] SIMD alignment on all critical data fields.
    - `align` function already exists (unused) in [segment.rs](./clem-core/src/segment.rs).
    - Critical fields are described in [simd-alignment.md](./doc/simd-alignment.md).
- [ ] Standardise buffer sector offset is relative to the immutable segment region excluding the file header:
    - [x] Update `Serialize::sector` and `Header::tail` documentation.
    - [ ] Refactor all buffer offset calculations to reflect this change.
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
