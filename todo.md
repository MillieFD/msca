### Foundational Functionality (Priority I)

- [ ] Implement `dictionary` and `index` abstractions without adding new segment types.
- [ ] Add a query builder with an async file reader.
  - [ ] New query instance via `Dataset::query(&self, name: &str) -> Result<Query, Error>`:
    - [ ] Add `query::Column` which holds buffers and filters.
    - [ ] `Dataset::query` maps the schema `BTreeMap<String, manifest::Column>` into `BTreeMap<String, query::Column>`.
    - [ ] The `Query` therefore starts with every column and every buffer.
    - [ ] Columns and buffers are removed by calling filter methods on the `Query` instance (subtractive).
  - [ ] Certain filters can be applied before file IO:
    - [ ] Column-level filters to remove whole columns from the query map.
    - [ ] Buffer-level filters to remove buffers from the query map e.g. using min / max statistics.
    - [ ] Columns are removed if their buffer count falls to zero.
  - [ ] Certain filters must be applied during file IO:
    - [ ] `query::Filter` added to a collection owned by the `Query` instance.
    - [ ] Investigate using a `BTreeSet` or `HashSet` to ensure filter uniqueness; duplicate filters reduce efficiency.
    - [ ] Retain the most constrained filter if two filters conflict e.g. `> 20` should replace `> 10`.
  - [ ] Add `Read` trait with associated `Item` type:
    - [ ] Readers are strongly typed and inherently know how to deserialize bytes into their target Rust type.
    - [ ] Type-erased `BoxRead` trait object hides the concrete reader type; mirror the `BoxAcc` accumulator design.
    - [ ] Add `Type::reader(&self) -> BoxRead` to initialise an appropriate reader for the column e.g. `column.ty.reader()`.
    - [ ] `Read::next` returns the next deserialized item
  - [ ] `query::Column::read` executes file IO and applies remaining filters; returns a `BoxRead` for the column.
  - [ ] Generalise `Deserialize` trait w/ a source
    - [ ] Add an associated source/context type to Deserialize (mirroring Write::Ctx<'a>)
    - [ ] Leaves deserialize from `&[u8]`
    - [ ] External types deserialize from a composite reader
    - [ ] Users can add `#[derive(Deserialize)]` on their external types:
      - [ ] Generates a hidden composite reader struct that holds a reader for each external type field.
      - [ ] `deserialize` calls `next` on each sub-reader to construct one instance of the external type.
    - [ ] This allows us to merge the Reconstruct and Deserialize traits
  - `Query::read::<I>` returns `BoxRead<Item = I>` using the derived deserializer for `I`; mirrors `Data::accumulator`.
- [ ] SIMD alignment on all critical data fields.
- [ ] Manifest rebuild function
  - [ ] Triggered automatically during `File::open` if corruption is detected.
  - [ ] Ensure the on-disk layout is sufficiently self-describing to support rebuild.
  - [ ] Identify any redundant on-disk fields not required for the layout to self-describe.
  - [ ] Remove redundant fields to optimise on-disk size.
- [ ] Ensure schema / type verification is performed exactly once; not per-read.
- [ ] Add static assertion for usize into u64, then remove all `try_into` runtime checks with faster unchecked fn.

### Path to Prototype (Priority II)

- [ ] Design `Dataset` API with quality-of-life improvements and documentation.
- [ ] Finish `clem-core` root module (lib) to re-export public API. Check all visibility modifiers.
- [ ] Finalise `clem-derive` procedural macro design.

### Extend Functionality (Priority III)

- [ ] Add support for free-form metadata written after the manifest. Feature-gated. Ignored if the feature is disabled.
- [ ] Add a feature-gated `bin` segment variant for immutable binary data in any format (e.g. TOML) like the manifest.
- [ ] Implement logging macros gated via the `log` feature.

### Crate Features (Priority IV)

- [ ] Add `derive` feature (ON by default) to enable `clem-derive` sub-crate.
- [ ] Add `no-std` feature (OFF by default).
- [ ] Add `async` feature (ON by default) to use `smol::fs` instead of `std::fs`.

### Ecosystem & Tooling (Priority V)

- [ ] Produce a CLI tool for inspecting `clem` files. Write to `clem-cli` subcrate.
