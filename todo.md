### Foundational Functionality (Priority I)

- [ ] Implement `dictionary` and `index` abstractions without adding new segment types.
- [x] Add a query builder with an async file reader.
    - [x] New query instance via `Dataset::query(&self, name: &str) -> Result<Query, Error>`:
        - [x] Add `query::Column` which holds buffers and filters.
        - [x] `Dataset::query` maps the schema `BTreeMap<String, manifest::Column>` → `BTreeMap<String, query::Column>`.
        - [x] The `Query` therefore starts with every column and every buffer.
        - [x] Columns and buffers are removed by calling filter methods on the `Query` instance (subtractive).
    - [x] Some filters can be applied before file IO:
        - [x] Column-level filters to remove whole columns from the query map. (`select`)
        - [x] Buffer-level filters to remove buffers from the query map e.g. using min / max statistics. (`range`)
        - [x] Columns are removed if their buffer count falls to zero.
    - [x] Other filters must be applied during file IO:
        - [x] `query::Filter` added to a collection owned by the `Query` instance.
        - [x] Use a `BTreeSet` or `HashSet` to ensure filter uniqueness; duplicate filters reduce efficiency. (`HashSet`)
        - [x] Retain the most constrained filter if two filters conflict e.g. `> 20` should replace `> 10`. (conjunction)
    - [x] Some filters can be used before file IO to remove buffers, but must also be evaluated during IO e.g. `range`
    - [x] Add `Read` trait with associated `Item` type:
        - [x] Readers are strongly typed and inherently know how to deserialize bytes into their target Rust type.
        - [x] Type-erased `BoxRead` trait object hides the concrete reader type; mirror the `BoxAcc` accumulator design.
        - [x] Add `Type::reader(&self) -> BoxRead` to initialise a deserializer for the column via `column.ty.reader()`.
        - [x] `Read::next` returns the next deserialized item
    - [x] `query::Column::read` executes file IO and applies remaining filters; returns a `BoxRead` for the column.
    - [x] Generalise `Deserialize` trait w/ a source
        - [x] Add an associated source/context type to Deserialize (mirroring Write::Ctx<'a>)
        - [x] Leaves deserialize from `&[u8]`
        - [x] External types deserialize from a composite reader
        - [x] Users can add `#[derive(Deserialize)]` on their external types:
            - [x] Generates a hidden composite reader struct that holds a reader for each external type field.
            - [x] `deserialize` calls `next` on each sub-reader to construct one instance of the external type.
        - [x] This allows us to merge the Reconstruct and Deserialize traits
    - [x] `Query::read::<I>` returns the composite reader for `I`; mirrors `Data::accumulator`.
        - [x] Type-erased as `BoxRead<Item = I>` to hide the generated reader type from users.
    - [ ] Implement optional and unsized layout readers: `OptBitVec` + `OptInSitu` + `Seq` + `OptSeq` + `Flatten`
    - [ ] Add remaining query filters: `eq` + `one_of` + `none_of` + `is_some` + `is_none` + `mask` + `limit` + `offset`
- [ ] SIMD alignment on all critical data fields.
- [ ] Manifest rebuild function
    - [ ] Triggered automatically during `File::open` if corruption is detected.
    - [ ] Ensure the on-disk layout is sufficiently self-describing to support rebuild.
    - [ ] Identify any redundant on-disk fields not required for the layout to self-describe.
    - [ ] Remove redundant fields to optimise on-disk size.
- [ ] Ensure schema / type verification is performed exactly once; not per-read.
- [ ] Add static assertion for usize into u64, then remove all `try_into` runtime checks with faster unchecked fn.

### Path to Prototype (Priority II)

- [ ] Design public `Dataset` API with quality-of-life improvements and documentation.
- [ ] Finish `clem-core` root module (lib) to re-export public API. Check all visibility modifiers.
- [ ] Finalise `clem-derive` procedural macro design.
- [ ] Resolve discrepancies between [doc](./doc) and actual implementations. Update documentation as needed.
- [ ] Add comprehensive unit tests for core functionalities in each module; cover edge cases.
- [ ] Add round-trip integration tests for `#[derive(Data)]` and `#[derive(Deserialize)]` in "tests" directory.
- [ ] Remove references to concurrency model `RwLock<Manifest>` in read-cycle documentation

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
