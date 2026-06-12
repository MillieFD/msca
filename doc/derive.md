### Derive Macros

The feature-gated `clem-derive` sub-crate exports procedural macros that allow reading and writing external types from
and to datasets respectively.

```rust
#[derive(Data, Read)]
struct Record {
    uuid: u8,
    latitude: f64,
    longitude: f64,
}
```

The `Data` and `Read` traits are independent but often implemented together for write-read round trip support. Users
may choose to define a read-only struct with fields matching a subset of the schema columns to deserialize after
`Query::select`. Fields are processed in **name-sorted** order corresponding to the deterministic platform-invariant
`BTreeMap` column order used throughout the engine. Generated code lives inside an anonymous `const` block to avoid
collision with user items.

##### Cargo Feature

Procedural macros are re-exported from `clem` behind the `derive` feature, which is **enabled by default**:

```toml
[dependencies.clem]
version = "1.0"
features = ["derive"]
```

Disabling this feature removes the `clem-derive` sub-crate from the dependency tree, which may improve compile times.
`Data` and `Read` can still be implemented manually.
